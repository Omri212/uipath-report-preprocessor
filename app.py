import io
import json
from typing import List, Dict, Optional, Tuple

import pandas as pd
import streamlit as st


st.set_page_config(page_title="Excel Column Planner", page_icon="🧭", layout="wide")
st.title("🧭🧭 Excel Column Planner")
st.caption("Upload an Excel/CSV, choose per-column actions (remove / rename / reorder), and export the plan and transformed file.")

# --- Session State Initialization ---
# plan_data_df will now be indexed by 'original' column name for stable tracking
if 'plan_data_df' not in st.session_state:
    st.session_state.plan_data_df = pd.DataFrame()
if 'file_kind' not in st.session_state:
    st.session_state.file_kind = None
if 'sheet_name' not in st.session_state:
    st.session_state.sheet_name = None
if 'last_file_name' not in st.session_state:
    st.session_state.last_file_name = None
if 'editor_key_version' not in st.session_state:
    st.session_state.editor_key_version = 0
# ------------------------------------

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def read_excel_headers(file_bytes: bytes, sheet_name: Optional[str]) -> List[str]:
    """Return headers from an Excel sheet (first row)."""
    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, nrows=0)
    return list(df.columns)


def read_excel_df(file_bytes: bytes, sheet_name: Optional[str]) -> pd.DataFrame:
    return pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name)


def read_csv_headers(file_bytes: bytes) -> List[str]:
    df = pd.read_csv(io.BytesIO(file_bytes), nrows=0)
    return list(df.columns)


def read_csv_df(file_bytes: bytes) -> pd.DataFrame:
    return pd.read_csv(io.BytesIO(file_bytes))


def compute_order(final_names: List[str], positions: List[Optional[int]]) -> List[str]:
    """
    final_names: visible names after rename, only for KEPT columns (in their current order)
    positions: list of integers or None, one per kept column. Integers are 1-based slots.
    """
    n = len(final_names)
    result: List[Optional[str]] = [None] * n

    # place explicitly positioned items
    for idx, slot in enumerate(positions):
        if slot is not None:
            result[slot - 1] = final_names[idx]

    # fill remaining in original order
    next_free = (i for i, v in enumerate(result) if v is None)
    for idx, slot in enumerate(positions):
        if slot is None:
            j = next(next_free)
            result[j] = final_names[idx]

    return [x for x in result if x is not None]  # type: ignore


def validate_and_build(headers: List[str], rows_df: pd.DataFrame) -> Dict[str, object]:
    """
    rows_df columns: ['original', 'remove', 'new_name', 'order']
    Build: remove[], rename{old:new}, order[new/kept...]

    IMPORTANT: rows_df here is the CANONICAL, index-reset DF.
    """
    if rows_df.empty:
        raise ValueError("No rows provided for plan.")

    # Ensure 'remove' is boolean and handle missing 'order' values
    rows_df["remove"] = rows_df["remove"].fillna(False).astype(bool)
    rows_df["new_name"] = rows_df["new_name"].fillna("").astype(str).str.strip()

    # No need to reset_index(drop=True) here if rows_df is already the canonical one

    kept_df = rows_df.loc[~rows_df["remove"].astype(bool)].copy()

    # Visible names after rename (or original if blank)
    kept_df["visible"] = kept_df.apply(
        lambda r: r["new_name"] if r["new_name"] else r["original"], axis=1
    )
    final_names = kept_df["visible"].tolist()
    n = len(final_names)

    # Validate 'order' for kept columns
    pos_raw = kept_df["order"].tolist()

    positions: List[Optional[int]] = []
    used = set()
    for pos in pos_raw:
        # Normalize: Treat None, empty string, or 0 as "no explicit order"
        if pd.isna(pos) or pos in ("", " ", 0, "0"):
            positions.append(None)
            continue

        if isinstance(pos, str):
            if not pos.isdigit():
                raise ValueError("Order must be a number in the range 1..N or left blank.")
            pos = int(pos)

        # Ensure it's treated as an integer
        pos = int(pos)

        if not (1 <= pos <= max(1, n) if n > 0 else 1):
            raise ValueError(f"Order '{pos}' out of range. Must be between 1 and {max(1, n)}.")
        if pos in used:
            raise ValueError(f"Duplicate order slot '{pos}'. Each position may be used once.")

        used.add(pos)
        positions.append(pos)

    # Validate visible names uniqueness
    if len(set(final_names)) != len(final_names):
        dupes = sorted([name for name in set(final_names) if final_names.count(name) > 1])
        raise ValueError(f"Visible column names must be unique after rename. Duplicates: {dupes}")

    remove_list: List[str] = rows_df.loc[rows_df["remove"], "original"].tolist()

    rename_map: Dict[str, str] = {}
    for _, r in rows_df.iterrows():
        old = r["original"]
        new = (r["new_name"] or "").strip()
        # Only include non-removed columns where a change happened
        if (not r["remove"]) and new and new != old:
            rename_map[old] = new

    # Calculate final order based on explicit positions and relative fill-in
    order_list = compute_order(final_names, positions)

    return {
        "remove": remove_list,
        "change_name": rename_map,
        "change_order": order_list,
    }


def apply_plan_to_df(df: pd.DataFrame, plan: Dict[str, object]) -> pd.DataFrame:
    """Apply plan (remove/rename/order) to a DataFrame and return the transformed DF."""
    remove: List[str] = list(plan.get("remove", []))
    rename: Dict[str, str] = dict(plan.get("change_name", {}))
    order: List[str] = list(plan.get("change_order", []))

    # Remove
    kept_df = df.drop(columns=[c for c in remove if c in df.columns], errors="ignore")

    # Rename (only for columns that exist)
    safe_rename = {k: v for k, v in rename.items() if k in kept_df.columns}
    kept_df = kept_df.rename(columns=safe_rename)

    # Reorder by order list (subset present)
    present = [c for c in order if c in kept_df.columns]
    # Include any remaining columns not mentioned (append at end, preserve their order)
    remaining = [c for c in kept_df.columns if c not in present]
    final_cols = present + remaining
    return kept_df[final_cols]


def plan_from_json(json_bytes: bytes) -> Dict[str, object]:
    plan = json.loads(json_bytes.decode("utf-8"))
    # normalize keys if user provided 'rename' instead of 'change_name'
    if "rename" in plan and "change_name" not in plan:
        plan["change_name"] = plan.pop("rename")
    # basic shape validation
    for k in ("remove", "change_name", "change_order"):
        if k not in plan:
            raise ValueError(f"Missing '{k}' in uploaded plan JSON.")
    return plan


def make_prefilled_table(headers: List[str], plan: Dict[str, object]) -> pd.DataFrame:
    """Create a table matching the editor schema, prefilled from a plan."""
    remove = set(plan.get("remove", []))
    rename: Dict[str, str] = dict(plan.get("change_name", {}))
    order: List[str] = list(plan.get("change_order", []))

    # Build base rows
    rows = []
    for h in headers:
        rows.append({
            "original": h,
            "remove": h in remove,
            "new_name": rename.get(h, ""),
            "order": None,
        })

    # Assign order indices (1-based) for kept columns based on change_order (which uses visible names)
    # First compute visible names after rename for kept columns
    kept_indices = [i for i, r in enumerate(rows) if not r["remove"]]
    kept_visible = [rows[i]["new_name"] if rows[i]["new_name"] else rows[i]["original"] for i in kept_indices]

    name_to_pos = {name: idx + 1 for idx, name in enumerate(order)}
    for k_idx, vis_name in zip(kept_indices, kept_visible):
        if vis_name in name_to_pos:
            rows[k_idx]["order"] = name_to_pos[vis_name]
        else:
            rows[k_idx]["order"] = None

    # Return DF with 'original' set as index for stable storage
    return pd.DataFrame(rows, columns=["original", "remove", "new_name", "order"]).set_index("original")


# -----------------------------------------------------------------------------
# Sidebar: File selection & sheet picker
# -----------------------------------------------------------------------------
with st.sidebar:
    st.header("1) Upload File")
    # File is read outside of session state to ensure fresh upload is captured
    file = st.file_uploader("Choose .xlsx or .csv", type=["xlsx", "csv"], accept_multiple_files=False)

    # Use session state to store derived file info
    st.session_state.file_kind = None
    st.session_state.sheet_name = None
    headers: List[str] = []

    # Store the uploaded file bytes persistently for later use in export_data
    file_bytes = None
    if file is not None:
        file_bytes = file.getvalue()

        # Check if a new file was uploaded or sheet selection changed (simple check)
        if st.session_state.last_file_name != file.name:
            st.session_state.plan_data_df = pd.DataFrame() # Clear plan on new file
            st.session_state.last_file_name = file.name

        try:
            if file.type in ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",) or file.name.lower().endswith(".xlsx"):
                st.session_state.file_kind = "xlsx"
                xls = pd.ExcelFile(io.BytesIO(file_bytes))

                selected_sheet = None
                if len(xls.sheet_names) > 1:
                    # Reset index=0 only if the selector is being displayed for the first time or sheet names changed
                    # Use a unique key for the sheet selector to force re-render
                    selected_sheet = st.selectbox("Sheet", xls.sheet_names, index=0, key="sheet_selector")
                else:
                    selected_sheet = xls.sheet_names[0]

                st.session_state.sheet_name = selected_sheet
                headers = read_excel_headers(file_bytes, st.session_state.sheet_name)
            else:
                st.session_state.file_kind = "csv"
                headers = read_csv_headers(file_bytes)

            st.success(f"Found {len(headers)} column(s)." + (f" Sheet: '{st.session_state.sheet_name}'" if st.session_state.sheet_name else ""))
        except Exception as e:
            st.error(f"Failed to read file: {e}")
            file = None # Invalidate file if read failed

if file is None or not headers:
    st.info("⬅️ Upload a file to begin.")
    st.stop()

# -----------------------------------------------------------------------------
# Plan editor
# -----------------------------------------------------------------------------
st.header("2) Configure columns")
st.write(
    "For each column: **Remove** (checkbox), optional **New name**, and optional **Order** (1..N). "
    "Check **'Remove'** to move a column to the bottom. Use **'Order'** to move a row to the top."
)

# Optional: load plan JSON to prefill
prefilled = None
with st.expander("Load an existing plan (JSON)"):
    plan_file = st.file_uploader("Upload plan JSON", type=["json"], key="plan_json")
    if plan_file is not None:
        try:
            loaded_plan = plan_from_json(plan_file.getvalue())
            prefilled = make_prefilled_table(headers, loaded_plan)
            st.success("Plan loaded. Table prefilled.")
        except Exception as e:
            st.error(f"Failed to load plan JSON: {e}")

# --- Data Initialization/Sorting Logic ---

# 1. Initialize data into session state if first run or new file
if st.session_state.plan_data_df.empty or st.session_state.plan_data_df.index.tolist() != headers:
    default_df = pd.DataFrame({
        "original": headers,
        "remove": [False] * len(headers),
        "new_name": [""] * len(headers),
        "order": [None] * len(headers),
    }).set_index('original') # Set the stable index

    start_df = prefilled if prefilled is not None else default_df
    st.session_state.plan_data_df = start_df.copy() # Store the canonical (unsorted) base data

# 2. Prepare the DataFrame for display/editing by applying the desired visual sort
# We must reset the index before sorting so 'original' becomes a column again for sorting/display
current_df_for_sort = st.session_state.plan_data_df.reset_index().copy()

# Ensure 'remove' is boolean for proper sorting
current_df_for_sort['remove'] = current_df_for_sort['remove'].astype(bool)

# Convert 'order' to numeric, coercing NaNs for sorting purposes (NaNs go last)
current_df_for_sort['sort_order_col'] = pd.to_numeric(current_df_for_sort['order'], errors='coerce')
# Fill explicit NaNs (blanks/None) with a large number
max_order = current_df_for_sort['sort_order_col'].max()
fill_value = (max_order + 1) if pd.notna(max_order) else 999999
current_df_for_sort['sort_order_col'] = current_df_for_sort['sort_order_col'].fillna(fill_value)

# Perform the dynamic sort:
sorted_df = current_df_for_sort.sort_values(
    by=['remove', 'sort_order_col', 'original'],
    ascending=[True, True, True],
    ignore_index=True
).drop(columns=['sort_order_col'])

# 3. Render the data editor with the sorted DataFrame
edited = st.data_editor(
    sorted_df, # Pass the sorted dataframe to the editor
    num_rows="fixed",
    use_container_width=True,
    column_config={
        "original": st.column_config.Column("Original Name", disabled=True, width="medium"),
        "remove": st.column_config.CheckboxColumn("Remove"),
        "new_name": st.column_config.TextColumn("New name (optional)"),
        "order": st.column_config.NumberColumn(
            "Order (kept cols 1..N)",
            min_value=1,
            step=1,
            help="Only for kept columns. Leave blank to keep relative order.",
        ),
    },
    hide_index=True,
    key=f"plan_table_{st.session_state.editor_key_version}", # Dynamic key to force re-render
)

# 4. Update session state with the edited data for the next run/plan generation
# CRITICAL FIX: We must compare the returned 'edited' DF (sorted) against the 'sorted_df'
# that was displayed, and then merge the changes back into the *canonical* (unsorted, indexed) state.
if not edited.equals(sorted_df):

    # Identify which rows changed in the editor
    # Since 'edited' is the sorted output, we merge it back to the canonical DF using 'original'
    edited_with_index = edited.set_index('original')

    # Update the canonical state with the user's latest edits. This ensures persistence (Fix 2).
    # .update() performs a stable, index-based update of shared columns.
    st.session_state.plan_data_df.update(edited_with_index)

    # Increment key version to force the editor to re-render the rows in the new sorted order (Fix 1).
    st.session_state.editor_key_version += 1

    # --- FIX FOR INSTANT JUMP ---
    # Force an immediate rerun to apply the new key and display the newly sorted data frame instantly.
    st.rerun()

# -----------------------------------------------------------------------------
# Build & show outputs
# -----------------------------------------------------------------------------
st.header("3) Build plan & export")
col_a, col_b, col_c = st.columns([1, 1, 2])

with col_a:
    run = st.button("Generate Outputs", type="primary")

with col_b:
    export_data = st.checkbox("Also build transformed file preview & download", value=False)

output_container = st.empty()
download_plan_container = st.empty()
preview_container = st.empty()
download_file_container = st.empty()

if run:
    try:
        # Use the latest CANONICAL data for plan generation
        # Reset index to make 'original' a column again, matching the expected input of validate_and_build
        plan = validate_and_build(headers, st.session_state.plan_data_df.reset_index())

        with output_container.container():
            st.subheader("remove:")
            st.code(json.dumps(plan["remove"], ensure_ascii=False, indent=2), language="json")

            st.subheader("change name:")
            st.code(json.dumps(plan["change_name"], ensure_ascii=False, indent=2), language="json")

            st.subheader("change order:")
            st.code(json.dumps(plan["change_order"], ensure_ascii=False, indent=2), language="json")

        # Download plan JSON
        combined = json.dumps(plan, ensure_ascii=False, indent=2)
        download_plan_container.download_button(
            label="⬇️ Download plan JSON",
            data=combined.encode("utf-8"),
            file_name="column_plan.json",
            mime="application/json",
        )

        if export_data:
            # Read full DF and apply plan
            if st.session_state.file_kind == "xlsx":
                # Ensure we read the bytes from the persistent file object if available
                df = read_excel_df(file_bytes, st.session_state.sheet_name)
            else:
                df = read_csv_df(file_bytes)

            transformed = apply_plan_to_df(df, plan)

            with preview_container.container():
                st.subheader("Preview transformed data (first 200 rows)")
                st.dataframe(transformed.head(200), use_container_width=True)

            # Offer download as Excel
            out_buf = io.BytesIO()
            with pd.ExcelWriter(out_buf, engine="openpyxl") as writer:
                transformed.to_excel(writer, index=False, sheet_name="Transformed")
            out_buf.seek(0)

            download_file_container.download_button(
                label="⬇️ Download transformed Excel",
                data=out_buf.getvalue(),
                file_name="transformed.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            st.success("Plan and transformed file generated successfully.")
        else:
            st.success("Plan generated successfully.")

    except Exception as e:
        st.error(str(e))
