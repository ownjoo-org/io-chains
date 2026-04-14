# io-chains

A lightweight Python library for building async data pipelines using a publisher/subscriber pattern. Chain together data sources, transformations, and consumers with minimal boilerplate.

## Installation

```bash
pip install oj-io-chains
```

## Core Concepts

A pipeline is built from **Processors** coordinated by a **Chain**:

```
[source] → Processor → Processor → Collector
                     ↘ Processor  (fan-out)
```

Each `Processor`:
- Pulls items from a source (list, iterable, async generator, or callable)
- Optionally transforms each item (sync or async)
- Publishes to one or more subscribers

A `Chain` wires multiple Processors together and runs them concurrently — the caller just `await chain()`.

For fan-in enrichment (joining data from multiple concurrent sources), use **`Enricher`** with **`Relation`** declarations and channel-tagged subscriptions.

To persist items to disk as they flow through the pipeline, use **`PersistenceLink`**.

---

## Public API

```python
from io_chains import Chain, Collector, Enricher, PersistenceLink, Processor, Relation, Skip, LinkMetrics
```

---

## Classes

### `Processor`

The core processing unit: source → transform → publish.

```python
Processor(
    source=...,        # list, iterable, async gen, or callable (optional)
    processor=...,     # transform function — sync or async (optional)
    subscribers=[...], # downstream subscribers (optional)
    workers=1,         # number of concurrent workers
    batch_size=1,      # items per worker call
    on_error=None,     # callable(exc, item) — handle errors without stopping the stream
    on_metrics=None,   # callable(LinkMetrics) — called once on completion
    name="",           # label for metrics and logs
    queue_size=0,      # internal queue depth (0 = unbounded)
)
```

**`processor=` return values**

| Return value | Effect |
|---|---|
| Any value | Published downstream |
| `Skip` | Item dropped silently |
| Generator | Expanded — each yielded value published |
| Async generator | Expanded asynchronously |
| `None` (or omitted `processor=`) | Original item passed through unchanged |

**Source types**

```python
Processor(source=[1, 2, 3])                      # list
Processor(source=(x * 2 for x in range(10)))     # generator expression
Processor(source=my_async_gen_func)              # async generator function (called automatically)
Processor(source=lambda: [1, 2, 3])              # callable returning iterable
```

---

### `Chain`

Wires Processors (and sub-Chains) together and runs them concurrently. The caller just `await chain()`.

```python
chain = Chain(
    source=...,        # attached to the first link (optional)
    links=[...],       # ordered list of Processors or Chains
    subscribers=[...], # attached to the last link's output (optional)
)
await chain()
```

A `Chain` is itself a `Linkable` — it can be nested inside another `Chain` or used as a subscriber of an external `Processor`.

---

### `Collector`

Buffers pipeline output for iteration after the pipeline completes.

```python
results = Collector()

# async iteration (preferred — works inside a running event loop)
async for item in results:
    print(item)

# sync iteration (use after pipeline has completed)
for item in results:
    print(item)
```

---

### `PersistenceLink`

A mid-chain tap: writes each item to a named store via `AsyncPersistenceManager` (from `oj-persistence`), then passes the item through unchanged.

`store_context()` is entered at run start and exited on completion, guaranteeing any buffered writes (e.g. NDJSON) are flushed.

```python
from oj_persistence.async_manager import AsyncPersistenceManager
from oj_persistence.store.async_ndjson_file import AsyncNdjsonFileStore
from io_chains import PersistenceLink

manager = AsyncPersistenceManager()
manager.register("records", AsyncNdjsonFileStore("data/output.ndjson"))

PersistenceLink(
    manager=manager,
    store_id="records",               # name registered with the manager
    key_fn=lambda item: str(item["id"]),  # extracts the store key from each item
    operation="upsert",               # "upsert" (default) | "create" | "update"
    allow_inefficient=False,          # passed through to manager.upsert()
    on_error=None,                    # callable(exc, item) — handle store errors without stopping the stream
)
```

---

### `Enricher` and `Relation`

Fan-in join: collects items from multiple named channels, then streams primary items enriched via `Relation` declarations.

```python
from io_chains import Enricher, Relation, Processor, Collector
from asyncio import create_task, gather

results = Collector()

enricher = Enricher(
    relations=[
        Relation(
            from_field="location_id",   # FK on the primary item
            to_channel="locations",     # channel holding related items
            to_field="id",              # field to match against
            attach_as="location",       # key added to enriched item
        ),
        Relation(
            from_field="episode_ids",
            to_channel="episodes",
            to_field="id",
            attach_as="episodes",
            many=True,                  # one-to-many: attach a list
        ),
    ],
    primary_channel="chars",
    subscribers=[results],
)

chars_link = Processor(source=chars_source)
locs_link = Processor(source=locations_source)
eps_link = Processor(source=episodes_source)

chars_link.subscribe(enricher, channel="chars")
locs_link.subscribe(enricher, channel="locations")
eps_link.subscribe(enricher, channel="episodes")

await gather(
    create_task(chars_link()),
    create_task(locs_link()),
    create_task(eps_link()),
    create_task(enricher()),
)
```

**`Relation` parameters**

| Parameter | Type | Description |
|---|---|---|
| `from_field` | `str` | Field on the primary item whose value is the join key. For `many=True`, the value should be a list of keys. |
| `to_channel` | `str` | Channel name holding the related items. |
| `to_field` | `str` | Field on related items to match against `from_field`. |
| `attach_as` | `str` | Key added to the enriched primary item. |
| `many` | `bool` | `True` → one-to-many (attach list); `False` → one-to-one (attach single or `None`). |
| `key_transform` | callable or `None` | Optional transform applied to each key before lookup. |

---

### `Skip`

Sentinel returned by a `processor=` function to drop an item silently.

```python
from io_chains import Skip

Processor(processor=lambda x: x if x > 0 else Skip)
```

---

### `LinkMetrics`

Emitted once per `Processor` / `PersistenceLink` on completion via the `on_metrics=` callback.

```python
@dataclass
class LinkMetrics:
    name: str
    items_in: int
    items_out: int
    items_skipped: int
    items_errored: int
    elapsed_seconds: float
```

---

## Usage Examples

### Simple transformation

```python
import asyncio
from io_chains import Chain, Collector, Processor

async def main():
    results = Collector()
    await Chain(
        source=[1, 2, 3],
        links=[Processor(processor=lambda x: x * 2)],
        subscribers=[results],
    )()
    print([item async for item in results])  # [2, 4, 6]

asyncio.run(main())
```

### Multi-stage pipeline

```python
results = Collector()
await Chain(
    source=["a", "b", "c"],
    links=[
        Processor(processor=str.upper),
        Processor(processor=lambda x: f"item: {x}"),
    ],
    subscribers=[results],
)()
# ["item: A", "item: B", "item: C"]
```

### Persist items to disk mid-pipeline

```python
from oj_persistence.async_manager import AsyncPersistenceManager
from oj_persistence.store.async_ndjson_file import AsyncNdjsonFileStore
from io_chains import Chain, Collector, PersistenceLink, Processor

manager = AsyncPersistenceManager()
manager.register("records", AsyncNdjsonFileStore("data/records.ndjson"))

results = Collector()
await Chain(
    source=fetch_records,
    links=[
        Processor(processor=normalize),
        PersistenceLink(
            manager=manager,
            store_id="records",
            key_fn=lambda r: str(r["id"]),
        ),
    ],
    subscribers=[results],
)()
```

### Fan-out to multiple subscribers

```python
sink1, sink2 = Collector(), Collector()
await Chain(
    source=[1, 2, 3],
    links=[Processor(processor=lambda x: x * 2)],
    subscribers=[sink1, sink2],
)()
# both sinks contain [2, 4, 6]
```

### Nested Chains

```python
normalise = Chain(links=[
    Processor(processor=lambda x: abs(x)),
    Processor(processor=lambda x: round(x, 2)),
])
stringify = Chain(links=[
    Processor(processor=lambda x: x * 100),
    Processor(processor=lambda x: f"{x:.0f}%"),
])
await Chain(source=[-0.156, 0.999, -0.301], links=[normalise, stringify], subscribers=[results])()
# ["16%", "100%", "30%"]
```

### Observability

```python
def log_metrics(m):
    print(f"{m.name}: {m.items_in} in, {m.items_out} out, {m.elapsed_seconds:.3f}s")

Processor(
    source=records,
    processor=transform,
    name="transform-stage",
    on_metrics=log_metrics,
)
```

---

## Architecture

```
Subscriber (ABC)
└── Collector

Publisher (ABC)
└── Linkable(Publisher, Subscriber)  (ABC)
    ├── Link                          (internal base: queue, EOS, metrics)
    │   ├── Processor                 (source → transform → publish)
    │   └── PersistenceLink           (tap: write to store → pass through)
    ├── Chain                         (orchestrator: wires and runs Links)
    └── Enricher                      (fan-in: join multiple channels)
```

---

## Development

```bash
# Install in editable mode
pip install -e .

# Run unit tests
python -m pytest test/unit -v

# Run user acceptance tests (requires network)
python -m pytest test/ua -v

# Lint
python -m ruff check io_chains/ test/

# Format
python -m ruff format io_chains/ test/
```
