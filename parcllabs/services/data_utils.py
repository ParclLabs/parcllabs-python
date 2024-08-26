from typing import List

import pandas as pd
import numpy as np


def safe_concat_and_format_dtypes(data_container):
    # Filter out empty DataFrames
    non_empty_dfs = [df for df in data_container if not df.empty]

    # Check if there are any DataFrames left to concatenate
    if not non_empty_dfs:
        return (
            pd.DataFrame()
        )  # Return an empty DataFrame if no non-empty DataFrames are found

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
    date_columns = ["date", "event_date"]
    for col in date_columns:
        if col in output.columns:
            output[col] = pd.to_datetime(output[col], errors="coerce")

    return output


def validate_input_str_param(
    param: str, param_name: str, valid_values: List[str], params_dict: dict = None
):
    if param:

        param = param.upper()

        params_dict[param_name] = param

        if param not in valid_values:
            raise ValueError(
                f"{param_name} value error. Valid values are: {valid_values}. Received: {param}"
            )

    return params_dict


def validate_input_bool_param(param, param_name: str, params_dict: dict = None):
    if param is None:
        return params_dict  # or return None if that’s preferred

    if not isinstance(param, bool):
        raise ValueError(
            f"{param_name} value error. Expected boolean. Received: {param}"
        )

    params_dict[param_name] = "true" if param else "false"
    return params_dict
