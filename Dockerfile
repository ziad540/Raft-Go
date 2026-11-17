FROM golang:1.27-bookworm

WORKDIR /workspace
ENV CGO_ENABLED=1

COPY go.mod ./
COPY labrpc/ ./labrpc/
COPY raft/ ./raft/

CMD ["go", "test", "-race", "./...", "-count=1", "-timeout=180s"]
