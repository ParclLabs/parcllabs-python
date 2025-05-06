import numpy as np
import pandas as pd

from parcllabs.common import DATE_COLUMNS, ID_COLUMNS


def _process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Process a single DataFrame to handle empty values and columns."""
    # Select only common columns
    processed_df = df.copy()

    # Remove columns that are entirely NA
    processed_df = processed_df.dropna(axis=1, how="all")

    # For columns that are all empty strings, replace with NaN
    for col in processed_df.columns:
        if (
            processed_df[col].dtype == object
            and processed_df[col].astype(str).str.strip().eq("").all()
        ):
            processed_df[col] = np.nan

    # Remove columns that became all NaN after replacing empty strings
    processed_df = processed_df.dropna(axis=1, how="all")

    return processed_df


def _reorder_columns(output: pd.DataFrame, original_columns: list) -> pd.DataFrame:
    """Reorder columns based on specified rules and original order."""
    final_columns = []

    # Rule 1: parcl_id or parcl_property_id should be the first column
    for col in ID_COLUMNS:
        if col in output.columns:
            final_columns.append(col)
            break

    # Rule 2: date or event_date should be the second column
    date_columns = ["date", "event_date"]
    for col in date_columns:
        if col in output.columns:
            final_columns.append(col)
            break

    # Add remaining columns in their original order
    for col in original_columns:
        if col in output.columns and col not in final_columns:
            final_columns.append(col)

    return output[final_columns]


def safe_concat_and_format_dtypes(data_container: list) -> pd.DataFrame:
    # Filter out empty DataFrames
    non_empty_dfs = [df for df in data_container if not df.empty]

    # Check if there are any DataFrames left to concatenate
    if not non_empty_dfs:
        return pd.DataFrame()  # Return an empty DataFrame if no non-empty DataFrames are found

    # Get the column order from the first non-empty DataFrame
    original_columns = non_empty_dfs[0].columns.tolist()

    # Find common columns across all non-empty DataFrames
    common_columns = list(set.intersection(*[set(df.columns) for df in non_empty_dfs]))

    # Process each DataFrame
    processed_dfs = []
    for df in non_empty_dfs:
        common_cols_df = df[common_columns].copy()
        processed_df = _process_dataframe(common_cols_df)
        if not processed_df.empty:
            processed_dfs.append(processed_df)

    # Concatenate the processed DataFrames
    if not processed_dfs:
        return pd.DataFrame()  # Return an empty DataFrame if all processed DataFrames are empty

    output = pd.concat(processed_dfs, axis=0, ignore_index=True)

    # Cast date columns to datetime
    for col in DATE_COLUMNS:
        if col in output.columns:
            output[col] = pd.to_datetime(output[col], errors="coerce")

    # Reorder columns
    return _reorder_columns(output, original_columns)
