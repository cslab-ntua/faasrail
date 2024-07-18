from bisect import bisect
from collections import OrderedDict

import pandas as pd

from utils import flatten2d


# Memory values with at most 2 set bits, which are relatively
# close to each other (at most 3 positions away).
MEM_MIB_VALUES = [
    # Sorted by construction
    (1 << e) + ((1 << e - i) if i else 0)
    for e in [7, 8, 9]
    for i in [0, 3, 2, 1]
]
MICROVM_EXTRA_MEM_MIB = 50


def _quantize_mem_size(mem_val: int) -> int:
    idx = bisect(MEM_MIB_VALUES, mem_val + MICROVM_EXTRA_MEM_MIB)
    return MEM_MIB_VALUES[idx]


FAASCELL_FUNCTIONS = OrderedDict(
    # These "memory_mb" fields exist only for compatibility with 0.0.2-dev
    {
        "chameleon": {
            "image": "docker.io/ckatsak/snaplace-fbpml-chameleon:0.0.3",
            "memory_mb": 640,
        },
        "rnn_serving": {
            "image": "docker.io/ckatsak/snaplace-fbpml-rnn_serving:0.0.3",
            "memory_mb": 384,
            "process_args": "/usr/local/bin/python3 /bench/server.py",  # XXX
        },
        "cnn_serving_geol": {
            "image": "docker.io/ckatsak/snaplace-fbpml-cnn_serving_geol:0.0.3",
            "memory_mb": 384,
        },
        "cnn_serving": {
            "image": "docker.io/ckatsak/snaplace-fbpml-cnn_serving:0.0.2-dev",  # XXX
            "memory_mb": 256,
        },
        "helloworld": {
            "image": "docker.io/ckatsak/snaplace-fbpml-helloworld:0.0.3",
            "memory_mb": 128,
        },
        "image_rotate": {
            "image": "docker.io/ckatsak/snaplace-fbpml-image_rotate:0.0.3",
            "memory_mb": 128,
        },
        "json_serdes": {
            "image": "docker.io/ckatsak/snaplace-fbpml-json_serdes:0.0.3",
            "memory_mb": 256,
        },
        "new_lr_serving": {
            "image": "docker.io/ckatsak/snaplace-fbpml-new_lr_serving:0.0.3",
            "memory_mb": 256,
        },
        "lr_serving": {
            "image": "docker.io/ckatsak/snaplace-fbpml-lr_serving:0.0.3",
            "memory_mb": 256,
        },
        "lr_training": {
            "image": "docker.io/ckatsak/snaplace-fbpml-new_lr_training:0.0.3",  # XXX
            "memory_mb": 448,
        },
        "matmul_fb": {
            "image": "docker.io/ckatsak/snaplace-fbpml-matmul_fb:0.0.3",
            "memory_mb": 192,
        },
        "pyaes": {
            "image": "docker.io/ckatsak/snaplace-fbpml-pyaes:0.0.3",
            "memory_mb": 128,
        },
        "video_processing": {
            "image": "docker.io/ckatsak/snaplace-fbpml-video_processing:0.0.3",
            "memory_mb": 128,
        },
    }
)


def workload_json_entries(workloads: pd.DataFrame) -> list[str]:
    """
    :param workloads: A dataframe produced by `preprocess.workloads_preprocess`
    """
    ret = []
    wl_names = set()  # wl: workload
    # Record all unique Workloads and their memory footprint
    for workload in flatten2d(workloads["workloads"].tolist()):
        benchmark_info = FAASCELL_FUNCTIONS[workload.benchmark]
        # The "or" holds for either every iteration (icy2) or none of them (icy1)
        memory_mb = workload.memory_mb or benchmark_info["memory_mb"]
        wl_name = workload.get_name()
        # Verify the uniqueness of wl_name
        assert wl_name not in wl_names, f'"{wl_name}" inserted twice'
        wl_names.add(wl_name)
        workload_info = {
            "id": wl_name,
            "image": benchmark_info["image"],
            "memory": _quantize_mem_size(memory_mb),
        }
        if "process_args" in benchmark_info:
            # Only applies to rnn_serving for now
            workload_info["process_args"] = benchmark_info["process_args"]
        ret.append(workload_info)
    ret.sort(key=lambda wl: wl["id"])
    return ret
