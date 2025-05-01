import numpy as np
import pandas as pd

from parcllabs.common import DATE_COLUMNS, ID_COLUMNS


def safe_concat_and_format_dtypes(data_container: list) -> pd.DataFrame:  # noqa: C901, PLR0912
    # Filter out empty DataFrames
    non_empty_dfs = [df for df in data_container if not df.empty]

    # Check if there are any DataFrames left to concatenate
    if not non_empty_dfs:
        return pd.DataFrame()  # Return an empty DataFrame if no non-empty DataFrames are found

    # Get the column order from the first non-empty DataFrame
    original_columns = non_empty_dfs[0].columns.tolist()

    # Find common columns across all non-empty DataFrames
    common_columns = list(set.intersection(*[set(df.columns) for df in non_empty_dfs]))

    # Initialize an empty list to store processed DataFrames
    processed_dfs = []

    for df in non_empty_dfs:
        # Select only common columns
        common_cols_df = df[common_columns].copy()

        # Remove columns that are entirely NA
        common_cols_df = common_cols_df.dropna(axis=1, how="all")

        # For columns that are all empty strings, replace with NaN
        for col in common_cols_df.columns:
            if (
                common_cols_df[col].dtype == object
                and common_cols_df[col].astype(str).str.strip().eq("").all()
            ):
                common_cols_df[col] = np.nan

        # Remove columns that became all NaN after replacing empty strings
        common_cols_df = common_cols_df.dropna(axis=1, how="all")

        if not common_cols_df.empty:
            processed_dfs.append(common_cols_df)

    # Concatenate the processed DataFrames
    if processed_dfs:
        output = pd.concat(processed_dfs, axis=0, ignore_index=True)
    else:
        return pd.DataFrame()  # Return an empty DataFrame if all processed DataFrames are empty

    # Cast date columns to datetime
    for col in DATE_COLUMNS:
        if col in output.columns:
            output[col] = pd.to_datetime(output[col], errors="coerce")

    # Reorder columns based on the specified rules and original order
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

    # Reorder the DataFrame
    return output[final_columns]
