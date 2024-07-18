from dataclasses import dataclass
import hashlib
import json
from typing import Any, Optional

import pandas as pd


@dataclass(init=False, frozen=True)
class Workload:
    """Immutable class representing a single FaaSRail Workload."""

    benchmark: str

    payload: dict[str, Any]

    exec_time_ms: int
    """Expected execution time"""

    memory_mb: Optional[int]

    # Initialize based on a pandas row
    def __init__(self, row: pd.Series) -> None:
        # We have two different kinds of measurements (icy{1,2}*.json)
        # and the first one has no "mem_mib" field

        # https://docs.python.org/3/library/dataclasses.html#frozen-instances
        object.__setattr__(self, "benchmark", row.loc["bench"])
        object.__setattr__(self, "payload", json.loads(row.loc["payload"]))
        object.__setattr__(self, "exec_time_ms", row.loc["dur_ms"])
        object.__setattr__(
            self, "memory_mb", row.loc["mem_mib"] if "mem_mib" in row.keys() else None
        )

    def get_name(self) -> str:
        """
        Returns a fixed size str that identifies this Workload, formatted as:
        "{bench_name}-{payload hash}"
        """
        FUNCTION_ID_LEN = 24
        payload_encoded = json.dumps(self.payload).encode("utf-8")
        payload_hash = hashlib.sha256(payload_encoded).hexdigest()[
            : FUNCTION_ID_LEN - len(self.benchmark) - 1
        ]
        return f"{self.benchmark}-{payload_hash}"

    def __str__(self):
        # Keep generated `self.__repr__` for debugging, and override only this
        return json.dumps(
            {
                "mean": self.exec_time_ms,
                "bench": self.get_name(),
                "payload": json.dumps(self.payload),
            }
        )
