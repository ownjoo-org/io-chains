from dataclasses import dataclass, field

@dataclass
class LinkMetrics:
    name: str
    items_in: int
    items_out: int
    items_skipped: int
    items_errored: int
    elapsed_seconds: float
    memory_peak_bytes: int = field(default=0)
    time_per_item_seconds: float = field(default=0.0)
    subscribed_count: int = field(default=0)
    subscriber_count: int = field(default=0)
    queue_depth_max: int = field(default=0)
    throughput_items_per_sec: float = field(default=0.0)
