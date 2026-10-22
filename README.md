## How to run

### 1. Run the simulation

```bash
java lcr.Main [seed] [maxDelayMs] [nNodes] [order] [outDir]
```

| Argument     | Default         | Description                                                        |
|--------------|-----------------|--------------------------------------------------------------------|
| `seed`       | `System.nanoTime()` | Seed for the UID shuffle (`random` order) and per-node delays  |
| `maxDelayMs` | `50`            | Maximum random delay applied to each message sent                  |
| `nNodes`     | `9`             | Number of nodes in the ring (UIDs are `1..nNodes`)                 |
| `order`      | `random`        | UID placement around the ring: `asc`, `desc` or `random`           |
| `outDir`     | `runs`          | Output directory (created if it does not exist)                    |

Example:

```bash
java lcr.Main 42 100 9 random runs
```

Add `-Dverbose=true` to also print the observed trace and the ground-truth trace to stdout:

```bash
java -Dverbose=true lcr.Main 42 100 9 random runs
```

### 2. Output

Each run writes two CSV files to `outDir`, named after the run parameters
(`<tag>` = `n<nNodes>-d<maxDelayMs>-<order>-s<seed>`):

- `observed-<tag>.csv`: the trace as recorded by the observer.
- `truth-<tag>.csv`: the same events sorted by their true sequence number (`trueSeq`), i.e. the real order in which they happened.

Using the same `seed` and parameters reproduces the same ring layout and delay pattern (hopefully).

### 3. Exit codes

- `0`: the run finished and both CSVs were written.
- `3`: the scheduler did not terminate within 30 s, so the observer trace may be incomplete (a warning is printed to stderr and no CSV is written).
