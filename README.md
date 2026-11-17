# Lab 2: Raft

This project asks you to implement leader election, heartbeats, and replicated
log entries in `raft/raft.go`.

## Requirements

- Go 1.22 or newer
- No external packages

## Run the tests

Run all tests from the project root:

```text
go test ./...
```

Run the leader-election checkpoint:

```text
go test -run 3A ./raft
```

Run the replicated-log tests:

```text
go test -run 3B ./raft
```

Run the tests with Go's race detector:

```text
go test -race ./...
```

### Race tests in Docker (Windows)

The race detector needs CGO and a C compiler. If Windows reports
`go: -race requires cgo`, you can run the tests in a Linux container.
Start Docker Desktop with Linux containers, then run these commands from
the project root in PowerShell:

```powershell
docker build -t raft-lab2 .
docker run --rm raft-lab2
```

The Docker image includes Go and GCC and enables `CGO_ENABLED=1`. Its default
command runs all tests with `-race`, disables cached test results, and sets a
three-minute timeout per package. A successful run exits with code 0; a test
failure or detected race exits with a nonzero code.

To run just one checkpoint:

```powershell
docker run --rm raft-lab2 go test -race ./raft -run 3A -count=1 -timeout=180s
docker run --rm raft-lab2 go test -race ./raft -run 3B -count=1 -timeout=180s
```

The image contains a copy of your code at build time. Rebuild after editing,
or mount your current project to test changes without rebuilding:

```powershell
docker run --rm --mount "type=bind,source=$($PWD.Path),target=/workspace,readonly" raft-lab2
```

Go's compilation cache stays inside the container; the bind mount can be
read-only. The first image build needs internet access to download the base
image. This setup runs the tests on Linux; it does not change your Windows
Go environment.

## Scope

Implement the Raft protocol in `raft/raft.go`. Use the supplied `labrpc`
package for all communication between Raft peers.

Do not modify `labrpc`, the supplied tests, or the public Raft method
signatures. You may add private types, fields, methods, and helper goroutines
inside `raft.go`.

Persistence, crash recovery from stable storage, cluster membership changes,
snapshots, and log compaction are outside the scope of Lab 2. The
`UseSnapshot` and `Snapshot` fields in `ApplyMsg` are kept for interface
compatibility and must be ignored.

## Test phases

The 3A tests cover leader election and heartbeats. The 3B tests cover log
replication, commitment, recovery from network partitions, conflict repair,
and RPC volume.
