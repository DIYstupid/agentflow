from dataclasses import dataclass


@dataclass(frozen=True)
class Edge:
    """Graph 有向边：source -> target。"""

    source: str
    target: str
