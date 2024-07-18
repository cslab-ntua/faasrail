from dataclasses import dataclass
import random

import numpy as np

from distribution import Distribution
from mapping import FunctionMapping
from specification import Specification, SpecificationRow
from workload import Workload


DEFAULT_SEED = 0xF0F0F0F0F0F0F0F0


@dataclass(frozen=True, kw_only=True)
class Config:
    gen_mode: str
    time_scaling: str
    max_rps: int
    first_minute: int
    target_minutes: int


class RequestGenerator:
    """Request Generator (v4)"""

    TOTAL_MINUTES = 1440

    def __init__(self, fm: FunctionMapping, config: Config):
        self.config = config
        self._fm = fm

        if self.config.gen_mode == "smirnov":
            # Our workflow for Smirnov transform:
            # - Create a time distribution ->
            # - Sample some time value for it ->
            # - Find the (super-)Function having the chosen time.
            # We need a structure that gives us a list of the trace's
            # indices with a specific, given execution time.
            values = fm.trace["dur_ms"].tolist()
            weights = fm.trace["inv_count"].tolist()
            self.exec_time_dist = Distribution(values, weights)

    def spec_generate(self) -> Specification:
        """
        Generate an experiment specification employing FaaSRail's "Spec" mode.
        """

        # TODO(phtof): This must be documented
        def custom_round(value: float) -> np.integer:
            floor = np.floor(value)
            ceiling = np.ceil(value)
            if value - floor < 0.35:
                return np.intc(floor)
            else:
                return np.intc(ceiling)

        if self.config.gen_mode != "spec":
            raise RuntimeError(
                f'RequestGenerator instance configured for "{self.config.gen_mode}"'
                ' rather than "spec"'
            )

        # Paper: 3.2.1.2 Scaling in Time
        # Convert the minutes part of the dataframe to a numpy array to avoid
        # the performance penalty.
        exec_times: list[float] = self._fm.trace["dur_ms"].to_list()
        invocs: list[int] = self._fm.trace["inv_count"].to_list()
        reorder = np.argsort(invocs)[::-1]  # indices sorted by invoc count
        minute_columns: list[str] = list(map(str, range(1, self.TOTAL_MINUTES + 1)))
        minutes = self._fm.trace[minute_columns].to_numpy()

        if self.config.time_scaling == "thumbnails":
            assert self.TOTAL_MINUTES % self.config.target_minutes == 0
            pg = self.TOTAL_MINUTES // self.config.target_minutes  # pg: per_group
            columns = [
                minutes[:, (pg * i) : (pg * (i + 1))].sum(axis=1, keepdims=True)
                for i in range(self.config.target_minutes)
            ]
            minutes = np.concatenate(columns, axis=1)
            minutes_header = [
                f"{pg * i + 1}-{pg * (i + 1)}"
                for i in range(self.config.target_minutes)
            ]
        elif self.config.time_scaling == "minute_range":
            first_minute = self.config.first_minute
            next_to_last_minute = first_minute + self.config.target_minutes
            # Doesn't matter for numpy, but header will be logically incorrect
            assert next_to_last_minute <= self.TOTAL_MINUTES
            minutes = minutes[:, first_minute:next_to_last_minute]
            minutes_header = [f"{i}" for i in range(first_minute, next_to_last_minute)]
        else:
            raise RuntimeError(
                f"Unreachable: unknown time_scaling == {self.config.time_scaling}"
            )

        # Paper: 3.2.1.1 Scaling the Request Rate
        total_rpm = minutes.sum(axis=0)
        max_rpm = max(total_rpm)
        target_max_rpm = self.config.max_rps * 60  # target max RPS --> target max RPM

        # Normalize and round down
        vec_custom_round = np.vectorize(custom_round)
        minutes = vec_custom_round(minutes * target_max_rpm / max_rpm)
        non_zero_rows_indices = np.where(minutes.any(axis=1))[0]

        sorted_rows = []
        for i in reorder:
            if i not in non_zero_rows_indices:
                continue
            sorted_rows.append(
                SpecificationRow(
                    exec_times[i], self._fm(exec_times[i]), minutes[i].tolist()
                )
            )

        headers = ["avg", "mapped_wreq"] + minutes_header

        return Specification(headers, sorted_rows)

    # Paper: 3.2.2 Smirnov Transform Mode
    def smirnov_generate_single(self) -> tuple[float, Workload]:
        """
        Generate a single invocation request employing FaaSRail's "Smirnov
        Transform" (or "Inverse Transform Sampling") mode.
        """
        rand_cdf_y = random.uniform(0, 1)
        chosen_exec_time = self.exec_time_dist.inverse_cdf(rand_cdf_y)
        assert chosen_exec_time >= 0, "Negative execution time in inverse CDF"
        return chosen_exec_time, self._fm(chosen_exec_time)

    def smirnov_generate(self, seed: int = DEFAULT_SEED) -> Specification:
        """
        Generate an experiment specification employing FaaSRail's "Smirnov
        Transform" (or "Inverse Transform Sampling") mode.
        """
        if self.config.gen_mode != "smirnov":
            raise RuntimeError(
                f'RequestGenerator instance configured for "{self.config.gen_mode}"'
                ' rather than "smirnov"'
            )
        random.seed(seed)

        generated_wls: dict[str, SpecificationRow] = {}
        for m in range(self.config.target_minutes):
            for _ in range(self.config.max_rps * 60):  # RPS --> RPM
                exec_time, wl = self.smirnov_generate_single()
                bench_name = wl.get_name()
                if bench_name not in generated_wls:
                    generated_wls[bench_name] = SpecificationRow(
                        exec_time, wl, [0] * self.config.target_minutes
                    )
                generated_wls[bench_name].minutes[m] += 1  # permitted despite frozen

        # Sort rows by invocation count (in descending order):
        sorted_rows = sorted(generated_wls.values(), key=lambda row: -sum(row.minutes))

        headers = ["avg", "mapped_wreq"] + list(
            map(str, range(1, self.config.target_minutes + 1))
        )

        return Specification(headers, sorted_rows)
