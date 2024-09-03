import pandas as pd
import numpy as np

from parcllabs.common import ID_COLUMNS, DATE_COLUMNS


def safe_concat_and_format_dtypes(data_container):
    # Filter out empty DataFrames
    non_empty_dfs = [df for df in data_container if not df.empty]

    # Check if there are any DataFrames left to concatenate
    if not non_empty_dfs:
        return (
            pd.DataFrame()
        )  # Return an empty DataFrame if no non-empty DataFrames are found

    # Get the column order from the first non-empty DataFrame
    original_columns = non_empty_dfs[0].columns.tolist()

    # Find common columns across all non-empty DataFrames
    common_columns = list(set.intersection(*[set(df.columns) for df in non_empty_dfs]))

    # Initialize an empty list to store processed DataFrames
    processed_dfs = []

    for df in non_empty_dfs:
        # Select only common columns
        df = df[common_columns].copy()

        # Remove columns that are entirely NA
        df = df.dropna(axis=1, how="all")

        # For columns that are all empty strings, replace with NaN
        for col in df.columns:
            if df[col].dtype == object and df[col].astype(str).str.strip().eq("").all():
                df[col] = np.nan

        # Remove columns that became all NaN after replacing empty strings
        df = df.dropna(axis=1, how="all")

        if not df.empty:
            processed_dfs.append(df)

    # Concatenate the processed DataFrames
    if processed_dfs:
        output = pd.concat(processed_dfs, axis=0, ignore_index=True)
    else:
        return (
            pd.DataFrame()
        )  # Return an empty DataFrame if all processed DataFrames are empty

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
    output = output[final_columns]

    return output
