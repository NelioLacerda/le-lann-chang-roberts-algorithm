## How to run:

### 1. Generate the traces

After running the program, each node will write its own local trace to `logs/node-<uid>.log`.

### 2. Build the global trace
Run the following command to generate the trace:

```bash
python trace_tool.py logs --out trace_baseline
```


This command will print the count of `ELECTION` and `ELECTED`, discards, the safety and agreement of the trace and print if the trace was some `send` withou `recv` (or vice-versa). 

Will also generate into `trace_baseline/` the merge of all node logs into one global trace, ordered by Lamport clock (`merged.txt`) and a space-time diagram of the execution (`trace.html`).