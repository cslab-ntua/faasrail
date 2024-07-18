from bisect import bisect
from dataclasses import dataclass
from typing import Sequence

import numpy as np


@dataclass(init=False, frozen=True)
class Distribution:
    cdf_x: np.ndarray[np.floating, np.dtype[np.intp]]
    cdf_y: np.ndarray[np.integer, np.dtype[np.intp]]

    def __init__(self, values: Sequence, weights: Sequence) -> None:
        assert len(values) == len(weights), "#values != #weights"
        so = np.argsort(values)
        sorted_values = [float(values[i]) for i in so]
        sorted_weights = [int(weights[i]) for i in so]

        # Merge duplicates
        values_dedup = [float("-inf")]
        weights_dedup = [0]

        curr = None
        for i, value in enumerate(sorted_values):
            if value == curr:
                weights_dedup[-1] += sorted_weights[i]
            else:
                values_dedup.append(value)
                weights_dedup.append(sorted_weights[i])
                curr = value

        weights_norm_cumsum = np.array(weights_dedup).cumsum().astype(np.float64)
        weights_norm_cumsum /= weights_norm_cumsum[-1]

        # We have constructed the cumulative distribution function (CDF). We
        # store the x points at which there is a "step" (we meet a new value
        # in the given sequence) and what is the value of y after the step.
        object.__setattr__(self, "cdf_x", np.array(values_dedup))
        object.__setattr__(self, "cdf_y", weights_norm_cumsum)

    def cdf(self, x: int) -> float:
        # FIXME(ckatsak): `x` (i.e., `values`) should be float since it's time?
        pos = bisect(self.cdf_x, x)
        return float(self.cdf_y[pos - 1])

    def inverse_cdf(self, u: float) -> int:
        # FIXME(ckatsak): Should return float (since it's time)? Fix trace!
        assert 0 <= u <= 1
        pos = bisect(self.cdf_y, u)
        # print(
        #     f"u = {u}, pos = {pos}\n"
        #     f"\tcdf_y[{pos-1}] = {self.cdf_y[pos-1]} --> cdf_x[{pos-1}] = {self.cdf_x[pos-1]}\n"
        #     f"\tcdf_y[{pos}] = {self.cdf_y[pos]} --> cdf_x[{pos}] = {self.cdf_x[pos]}\n"
        # )
        return int(self.cdf_x[pos])
