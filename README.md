<div align="center">

# Key-Value Store

A concurrent key-value store with a client-server protocol and subscriptions. Sistemas Operativos 2023/2024.

<img src="https://img.shields.io/badge/C-A8B9CC?style=flat&logo=c&logoColor=black" alt="C">
<img src="https://img.shields.io/badge/Makefile-427819?style=flat&logo=gnu&logoColor=white" alt="Makefile">
<img src="https://img.shields.io/badge/POSIX-threads-555555?style=flat" alt="POSIX">
<img src="https://img.shields.io/badge/Built_at-IST-009DE0?style=flat" alt="Built at">

</div>

<br>

The project for Sistemas Operativos at Instituto Superior Técnico. A server keeps a key-value store in
memory, runs batches of commands from job files in parallel threads, writes backups in child
processes, and serves clients over named pipes. Clients can subscribe to keys and are told when they
change.

## Commands

| Command | What it does |
|---|---|
| `WRITE [(key,value)...]` | Write pairs |
| `READ [key...]` | Read keys |
| `DELETE [key...]` | Delete keys |
| `SHOW` | Print the whole store |
| `WAIT <ms>` | Pause |
| `BACKUP` | Write a backup without blocking the other jobs |
| `SUBSCRIBE <key>` / `UNSUBSCRIBE <key>` | Client only: follow a key |
| `DELAY <ms>` | Client only: pause |
| `DISCONNECT` | Client only: leave |

## Build and run

```bash
make
./src/server/kvs <jobs_dir> <max_threads> <max_backups> <server_fifo>
./src/client/client <client_id> <server_fifo>
```

Job files (`*.job`) are read from the jobs folder; each produces a `.out` file and its backups
(`-1.bck`, `-2.bck`, ...).

## Layout

```
src/server/      store, job runner, backups, client sessions
src/client/      client API and command parser
src/common/      protocol and shared I/O
src/tests/       client test scripts
codigo_base/     the course's starting code
```
