import csv
from dataclasses import dataclass
from typing import Any

from workload import Workload


@dataclass(frozen=True)
class SpecificationRow:
    trace_exec_time: float
    workload: Workload
    minutes: list[int]

    def to_list(self) -> list[Any]:
        return [self.trace_exec_time, str(self.workload)] + self.minutes


@dataclass(frozen=True)
class Specification:
    headers: list[str]
    sorted_rows: list[SpecificationRow]

    def to_csv(self, fout) -> None:  # fout: _typeshed.SupportsWrite
        """
        Export this experiment specification instance as a CSV file.

        :param fout: any object with a `write()` method
        """
        writer = csv.writer(fout)
        writer.writerow(self.headers)
        for row in self.sorted_rows:
            writer.writerow(row.to_list())
