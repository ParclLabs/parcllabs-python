import pandas as pd


def safe_concat_and_format_dtypes(data_container):
    # Filter out empty DataFrames
    non_empty_dfs = [df for df in data_container if not df.empty]

    # Check if there are any DataFrames left to concatenate
    if not non_empty_dfs:
        return (
            pd.DataFrame()
        )  # Return an empty DataFrame if no non-empty DataFrames are found

    # Concatenate the non-empty DataFrames
    output = pd.concat(non_empty_dfs).reset_index(drop=True)

    # cast date columns to datetime
    if "date" in output.columns:
        output["date"] = pd.to_datetime(output["date"])
    if "event_date" in output.columns:
        output["event_date"] = pd.to_datetime(output["event_date"])
    return output
