## 1. Paxos Algorithm (paxos.py)

### Overview

- Simulates a cluster of nodes running the Paxos algorithm.
- Each node can act as Proposer, Acceptor, and Learner.
- Demonstrates consensus, node failures, recovery, and logs all events.

### Features

- **Consensus Phases:** Prepare and Accept phases are implemented.
- **Fault Tolerance:** Nodes can fail and recover during consensus.
- **Execution Logs:** Detailed logs for each node, saved to a file after the demo.
- **Statistics:** Tracks requests, responses, successes, and failures.

### How to Run

```sh
python paxos.py
```

### What to Expect

- The script initializes a cluster of 5 nodes.
- Multiple values are proposed for consensus.
- Simulates node failure and recovery.
- Prints cluster status and execution logs.
- Saves logs to a file (e.g., `paxos_execution_log_<timestamp>.txt`).

---

## 2. Raft Algorithm (raft.py)

### Overview

- Simulates a cluster of nodes running the Raft algorithm.
- Nodes elect a leader, replicate log entries, and apply commands.
- Demonstrates leader election, log replication, fault tolerance, and recovery.

### Features

- **Leader Election:** Automatic leader selection and re-election on failure.
- **Log Replication:** Commands are replicated and committed across nodes.
- **Fault Tolerance:** Simulates leader failure and node recovery.
- **Cluster Status:** Shows node states, terms, logs, and applied commands.

### How to Run

```sh
python raft.py
```

### What to Expect

- The script initializes a cluster of 5 nodes.
- Waits for leader election, then proposes several commands.
- Simulates leader failure and recovery.
- Prints cluster status and applied commands.
- Logs are saved to `raft_log.txt`.

---

## Organization & Usage

- **No external dependencies** are required beyond Python 3.7+ and the standard library.
- You can use the provided virtual environment (env) if you wish, but it's not required for basic usage.
- Both scripts are self-contained and can be run independently.

---

## Output & Logs

- **Paxos:** Execution logs are printed and saved per node.
- **Raft:** Logs are printed to console and saved to `raft_log.txt`.

---

## Example Commands

Run Paxos demo:
```sh
python paxos.py
```

Run Raft demo:
```sh
python raft.py
```

---

---

## Questions?

Open paxos.py or raft.py and review the code and comments for further details.
