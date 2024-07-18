import json
import os

import numpy as np
import pandas as pd

from workload import Workload


def workloads_preprocess(workloads_file_path: str) -> pd.DataFrame:
    """
    Returns a pd.DataFrame with a sorted index representing some execution time
    and a list of workloads corresponding to each time.

    ?TODO(phtof): For now, we ignore `stdev`.
    """
    with open(workloads_file_path) as fin:
        workloads_json = json.load(fin)
    workloads = pd.DataFrame(workloads_json).sort_values(by=["mean"])
    workloads.rename(columns={"mean": "dur_ms"}, inplace=True)

    workloads["wl"] = workloads.apply(lambda row: Workload(row), axis=1)
    workloads.drop(columns=["bench", "payload"], inplace=True)

    return workloads.groupby(by=["dur_ms"])["wl"].apply(list).to_frame("workloads")


def function_durations_percentiles(
    day: int,
    dirpath: str = "",
) -> pd.DataFrame:
    return (
        pd.read_csv(
            os.path.join(dirpath, f"function_durations_percentiles.anon.d{day:02}.csv")
        )
        # use shorter names that I'm used to:
        .rename(
            {
                "Average": "dur_ms",
                "Count": "cnt",
                "Minimum": "min",
                "Maximum": "max",
                "percentile_Average_0": "p0",
                "percentile_Average_1": "p1",
                "percentile_Average_25": "p25",
                "percentile_Average_50": "p50",
                "percentile_Average_75": "p75",
                "percentile_Average_99": "p99",
                "percentile_Average_100": "p100",
            },
            axis=1,
        )
        # drop all na (though iirc there shouldn't be any?):
        .dropna()
        # rearrange columns for better visualization in ipynb:
        .loc[
            :,
            [
                "dur_ms",
                "cnt",
                "min",
                "max",
                "p0",
                "p1",
                "p25",
                "p50",
                "p75",
                "p99",
                "p100",
                "HashFunction",
                "HashApp",
                "HashOwner",
            ],
        ]
    )


def invocations_per_function_md(day: int, dirpath: str = "") -> pd.DataFrame:
    idf = pd.read_csv(
        os.path.join(dirpath, f"invocations_per_function_md.anon.d{day:02}.csv")
    ).dropna()  # drop all na (though iirc there shouldn't be any?)

    minute_cols = idf.columns[idf.columns.str.isdigit()]
    # Assert that every column with all-digits name refers to invocations per
    # minute, and that none of them is missing:
    assert (minute_cols.map(int) == np.arange(1, 1441)).all()

    # Drop all rows that contain negative # of invocations in any of its
    # minute column:
    _midf: pd.DataFrame = idf.loc[:, minute_cols]
    idf.drop(_midf.loc[(_midf < 0).any(axis=1)].index, inplace=True)

    # Calculate total # of invocations per Function based on per-minute data:
    idf["cnt_finv"] = idf[minute_cols].sum(axis=1)

    return idf


def joined_func_invoc_df(day: int, dirpath: str = "") -> pd.DataFrame:
    jdf = (
        function_durations_percentiles(day, dirpath)
        .rename(columns={"cnt": "cnt_fdur"})
        .merge(
            invocations_per_function_md(day, dirpath),
            how="inner",
            on=["HashFunction", "HashApp", "HashOwner"],
        )
        .rename(columns={"cnt_finv": "inv_count"})
    )
    # Use median in cases where mean is invalid:
    # Replace mean with median in cases where the former is invalid. Drop the
    # whole row if the median is invalid too.
    jdf["dur_ms"] = jdf["dur_ms"].where(jdf["dur_ms"] >= 0, jdf["p50"])
    jdf.drop(jdf.loc[jdf["dur_ms"] < 0].index, inplace=True)
    # FIXME: Apart from the mean, other data might be bad too; maybe just drop
    # such rows? (minute cols are already checked btw, and dropped if invalid)
    return jdf


def azure_trace_preprocess(trace_dir_path: str) -> pd.DataFrame:
    # # Use the Hash triplet as our index
    # index = ["HashOwner", "HashApp", "HashFunction"]
    #
    # christos = (
    #     joined_func_invoc_df(1, trace_dir_path)
    #     .groupby(by=["dur_ms"])
    #     .sum()
    #     .drop(columns=index)
    # )
    #
    # if trace_dir_path[-1] != "/":
    #    trace_dir_path += "/"
    #
    # dur_filename = "function_durations_percentiles.anon.d01.csv"
    # trace_durations = pd.read_csv(trace_dir_path + dur_filename).dropna()
    # # I have checked that there are no duplicates
    # trace_durations = trace_durations.set_index(index)
    # renaming = {"Average": "dur_ms"}
    # trace_durations = trace_durations.rename(columns=renaming)
    # # Sanitization
    # # Drop the percentile columns
    # #### trace_durations = trace_durations[["dur_ms"]]
    #
    # invoc_filename = "invocations_per_function_md.anon.d01.csv"
    # trace_invocations = pd.read_csv(trace_dir_path + invoc_filename).dropna()
    # # ?TODO(phtof): What about Trigger = Timer?
    # #### trace_invocations = trace_invocations.drop(columns=["Trigger"])
    #
    # minute_cols = trace_invocations.columns[trace_invocations.columns.str.isdigit()]
    # minutes = trace_invocations.loc[:, minute_cols]
    # trace_invocations.drop(minutes.loc[(minutes < 0).any(axis=1)].index, inplace=True)
    #
    # trace_invocations["inv_count"] = trace_invocations.loc[:, "1":"1440"].sum(axis=1)
    #
    # # We have ~10 entries having duplicate indices: just
    # # sum the duplicates
    # #### trace_invocations = trace_invocations.groupby(index).sum()
    #
    # # Inner join of the two parts
    # trace = pd.merge(trace_durations, trace_invocations, how="inner", on=index)
    # # We don't need the index triplet anymore
    # trace = trace.reset_index(drop=True)
    #
    # trace["dur_ms"] = trace["dur_ms"].where(
    #    trace["dur_ms"] >= 0, trace["percentile_Average_50"]
    # )
    # trace.drop(trace.loc[trace["dur_ms"] < 0].index, inplace=True)
    #
    # trace.sort_values(by=["dur_ms"], inplace=True)
    #
    # trace = trace.groupby(by=["dur_ms"]).sum()
    #
    # trace = trace.drop(columns=index)
    #
    # print(christos)
    # print(trace)
    #
    # print("FUCKITY FUCK::")
    # print("Please don't be true for the love of god")
    #
    # df_merged = trace.merge(christos,
    #    on=trace.columns.tolist(), how='outer', indicator=True)
    #
    # df_only_in_trace = df_merged[df_merged['_merge'] == 'left_only'].drop(
    #    columns=['_merge'])
    #
    # print(df_only_in_trace)
    #
    # return christos

    return (
        joined_func_invoc_df(1, trace_dir_path)
        .groupby(by=["dur_ms"])
        .sum()
        .drop(columns=["HashFunction", "HashApp", "HashFunction"])
    )
