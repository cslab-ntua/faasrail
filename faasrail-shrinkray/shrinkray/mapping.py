import numpy as np
import pandas as pd

from utils import flatten2d
from workload import Workload


class FunctionMapping(object):
    """
    Create a mapping between functions found in Azure Functions' trace
    and our generated Workloads (i.e., Function-Input combinations, for
    functions adopted from FunctionBench.)

    Terms used:
    - Real function / trace function: A function in the Azure trace.
    - Workload: A unique (benchmark, input) pair that attempts to represent
                a real function when it comes to its execution time. This
                class ultimately defines a mapping
                [Real functions] -> [workloads] (1:N relationship).
    - Benchmark: Simply refers generally to some (FunctionBench) benchmark.
                It should not be confused with the term 'workload' (i.e.,
                the latter is more specific, as defined right above).

    General Idea:
    For each real function, pick all workloads that have execution times within
    a certain radius (here, we allow execution time to diverge in any direction
    by 1%).
    If there are no such functions, we are forced to choose the workload(s)
    with the closest execution time.
    Having many candidates per function allows us to later avoid picking (for
    the most part) densely distrbuted benchmarks (i.e., those that admit a
    larger input set and thus are probable to have execution times closer to
    real functions' execution times more often).
    """

    def __init__(self, trace_functions: pd.DataFrame, workloads: pd.DataFrame) -> None:
        """
        :param trace_functions: DataFrame produced by `preprocess.azure_trace_preprocess`
        :param workloads: DataFrame produced by `preprocess.workloads_preprocess`

        # Note

        Both input DataFrames are expected to be already sorted by mean execution time
        (as performed by functions in the `preprocess` module).
        """
        # Paper: 3.1.2 Reducing Trace Functions
        # NOTE: Both dataframes are already sorted by mean execution time
        self._trace = trace_functions.groupby("dur_ms").sum().reset_index()
        self._workloads = workloads

        # Paper: 3.1.3 Mapping Functions to Workloads [Par. 1 & 2]
        candidates = self._pick_candidates()
        # Paper: 3.1.3 Mapping Functions to Workloads [Par. 3]
        unique = self._unique_benchmarks_per_function(candidates)
        chosen_bench = self._greedy_glb(unique)
        self.mapping = self._get_mapping(candidates, chosen_bench)

        # Construct an inverse index for exec time (for performance, instead
        # of binary searching on each `FunctionMapping.__call__()`):
        values = self._trace["dur_ms"].tolist()
        self.exec_time_to_idx: dict[float, int] = {v: i for i, v in enumerate(values)}

    @property
    def trace(self) -> pd.DataFrame:
        return self._trace

    # Implementation detail: Having the dataframes already sorted makes our
    # idea realizable via a simple linear scan with a bit of backtracking.
    def _pick_candidates(self) -> list[list[int]]:
        RADIUS_FRAC = 0.0100  # +/- 1.00%
        t_len = len(self._trace)

        s_idx = 1  # Skip the guard

        # Sorted, by construction
        t_times = self._trace["dur_ms"].to_list()
        s_times = self._workloads.index.to_list()
        # Guards to make our code cleaner
        s_times = [float("-inf")] + s_times + [float("inf")]

        candidates = [[] for _ in range(t_len)]

        for t_idx in range(0, t_len):
            t_time = t_times[t_idx]
            l_border = (1 - RADIUS_FRAC) * t_time
            r_border = (1 + RADIUS_FRAC) * t_time

            # Case 1:
            # There is overlap with the previous function's radius, go back
            while s_times[s_idx] > l_border:
                s_idx -= 1
            # Case 2: (Most common) We are in no radius
            while s_times[s_idx] < l_border:
                s_idx += 1
            # Collect everything within this function's radius
            while s_times[s_idx] <= r_border:
                # Don't forget about the guard
                candidates[t_idx].append(s_idx - 1)
                s_idx += 1

            # We collected nothing: just add the closest point(s)
            if not candidates[t_idx]:
                l_dist = t_times[t_idx] - s_times[s_idx - 1]
                r_dist = s_times[s_idx] - t_times[t_idx]
                if l_dist <= r_dist:
                    candidates[t_idx].append(s_idx - 1 - 1)
                if l_dist >= r_dist:
                    candidates[t_idx].append(s_idx - 1)

        return candidates

    def _unique_benchmarks_per_function(
        self, candidates: list[list[int]]
    ) -> list[list[int]]:
        unique_benchmarks = [None] * len(candidates)
        for i, c in enumerate(candidates):
            curr_function: set[int] = set()
            for workload_id in c:
                for wl in self._workloads.iloc[workload_id, 0]:
                    curr_function.add(wl.benchmark)
            unique_benchmarks[i] = list(curr_function)
        return unique_benchmarks

    def _greedy_glb(self, permitted_machines: list[list[int]]) -> list[int]:
        """
        ** Greedy Generalized Load Balancing **

        For details about the problem, see:
        <https://www.cs.princeton.edu/~wayne/kleinberg-tardos/pdf/11ApproximationAlgorithms-2x2.pdf>

        This is not the approximation algorithm covered there (we adapted the
        algorithm for the plain load balancing problem instead), but when
        implemented, the approximation is overly complicated and sometimes
        produces worse results.

        Our heuristic is "Choose a machine that is somewhat not loaded or/and
        will not have a lot of chances to get picked in the future", thus why
        the sum load + remaining_quota is used. It is a makeshift solution,
        but it seems to work OK.

        """
        # Machines <-> FunctionBench benchmarks
        # Jobs <-> Real functions
        num_jobs = len(permitted_machines)
        unique_machines = set(flatten2d(permitted_machines))
        supply = self._trace["inv_count"].tolist()

        load = {m: 0 for m in unique_machines}
        scheduling = [-1] * num_jobs
        remaining_quota = {m: 0 for m in unique_machines}
        for j, pm in enumerate(permitted_machines):
            for machine in pm:
                remaining_quota[machine] += supply[j]

        sorted_ind = np.argsort(supply)
        for i in reversed(sorted_ind):
            # Find the permitted machine with the least load
            min_mach = min(
                permitted_machines[i], key=lambda i: load[i] + remaining_quota[i]
            )
            scheduling[i] = min_mach
            load[min_mach] += supply[i]
            for machine in permitted_machines[i]:
                remaining_quota[machine] -= supply[i]

        return scheduling

    def _get_mapping(
        self, candidates: list[list[int]], chosen_benchmark: list[int]
    ) -> list[Workload]:
        """
        :param candidates: the indices of possible workloads that may be used
        by function i
        :param chosen_benchmark: what specific benchmark we want to use for
        each function i to preserve balancing
        """
        wls = self._workloads["workloads"].tolist()

        def pick_workload(i: int) -> Workload:
            for wl_id in candidates[i]:
                for wl in wls[wl_id]:
                    if wl.benchmark == chosen_benchmark[i]:
                        return wl
            raise RuntimeError("Unreachable: auxiliary function always returns")

        return list(map(pick_workload, range(len(candidates))))

    def __call__(self, exec_time: float) -> Workload:
        assert (
            exec_time in self.exec_time_to_idx.keys()
        ), "Execution time must be present in the input trace"
        return self.mapping[self.exec_time_to_idx[exec_time]]
