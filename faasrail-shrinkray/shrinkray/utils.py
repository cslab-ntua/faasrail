from typing import Any


def flatten2d(ll: list[list[Any]]) -> list[Any]:
    """Flatten two dimensional list"""
    return [e for l in ll for e in l]
