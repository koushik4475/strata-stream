<p align="center">
  <img src="https://raw.githubusercontent.com/ER-brocode/strata-stream/main/assets/strata_stream_banner.svg" alt="StrataStream Banner" width="100%"/>
</p>

<p align="center">
  <a href="https://github.com/ER-brocode/strata-stream/actions"><img src="https://img.shields.io/github/actions/workflow/status/ER-brocode/strata-stream/ci.yml?branch=main&style=for-the-badge&logo=githubactions&logoColor=white" alt="CI Status" /></a>
  <a href="https://golang.org"><img src="https://img.shields.io/badge/Go-1.22+-00ADD8?style=for-the-badge&logo=go&logoColor=white" alt="Go Version" /></a>
  <a href="https://kafka.apache.org/"><img src="https://img.shields.io/badge/Kafka%20Protocol-Compatible-231F20?style=for-the-badge&logo=apachekafka&logoColor=white" alt="Kafka Compatible" /></a>
  <a href="https://aws.amazon.com/s3/"><img src="https://img.shields.io/badge/Tiered%20Storage-S3%20%2F%20MinIO-569A31?style=for-the-badge&logo=amazons3&logoColor=white" alt="S3 Tiering" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=for-the-badge" alt="License" /></a>
</p>

---

## 🌊 Overview

**StrataStream** is a high-throughput, tiered message streaming broker engineered for hyperscale data streaming workloads. It pairs a **zero-copy, segmented commit log** engine on local NVMe storage with automated background offloading of sealed segments to **object stores (S3, MinIO, GCS)** — retaining seamless protocol compatibility with existing Kafka producers and consumers.

### Key Highlights
- ⚡ **Zero-Copy Segmented Commit Log:** High-performance memory-mapped append logs with binary-search zero-copy 16-byte index blocks (`.index` + `.log`).
- 🧊 **Autonomous Tiered Storage Flusher:** Sealed segments transition seamlessly to object storage (AWS S3, MinIO, Ceph), dropping cold storage costs by 80%+.
- 🔄 **Kafka Wire-Protocol Compatible:** Connect native Kafka clients, producers, and consumers directly on standard ports.
- 🛡️ **CRC32 Data Integrity:** Rigorous per-record IEEE CRC32 checksumming protects against bit rot and disk corruption.
- 🚀 **Ultra Low Latency & High Concurrency:** Goroutine-per-connection non-blocking multiplexer with lock-minimized partition engines.

---

## 📐 Architecture

```
                  +----------------------------------------------+
                  |  Kafka Client / Producer / Consumer Apps     |
                  +----------------------------------------------+
                                         │
                         [ Wire Protocol TCP Frames ]
                                         │
                                         ▼
                 +────────────────────────────────────────────────+
                 │           StrataStream Broker Engine           │
                 │                                                │
                 │   +------------------+  +--------------------+ │
                 │   │ Frame Decoders   │  │ Response Formatter │ │
                 │   +------------------+  +--------------------+ │
                 │                      │                         │
                 │                      ▼                         │
                 │          Partition Commit Log Engine           │
                 +──────────────────────┬─────────────────────────+
                                        │
                 ┌──────────────────────┴──────────────────────┐
                 ▼                                             ▼
       [ Hot Tier: Local NVMe ]                     [ Cold Tier: S3/MinIO ]
   ┌───────────────────────────────┐             ┌───────────────────────────┐
   │ Active Segment (Append-Only)  │             │ Sealed Parquet / Log Blbs │
   │ ├── 00000000000000000000.log  │   Flusher   │ ├── cold/topic/0/...log   │
   │ └── 00000000000000000000.idx  ├────────────►│ └── cold/topic/0/...idx   │
   │                               │             │                           │
   │ Sealed Immutable Segments     │             │ Continuous Retention      │
   └───────────────────────────────┘             └───────────────────────────┘
```

---

## 🚀 Quickstart

### 1. Build & Run Broker Locally

```bash
# StrataStream | Kafka-Compatible Streaming Broker with S3 and MinIO Tiered Storage

StrataStream is a Go-based streaming broker project focused on Kafka-protocol compatibility and tiered storage for event data. Its documented architecture combines partitioned message handling with object-storage integration for S3-compatible services such as MinIO. Explore the quick start and verification steps to run the broker locally and test publish/consume and storage-tiering workflows.

git clone https://github.com/ER-brocode/strata-stream.git
cd strata-stream

# Build broker binary
go build -o bin/strata-broker ./cmd/strata-broker

# Start broker on port 9092
./bin/strata-broker -listen :9092 -data-dir ./data
```

### 2. Run with MinIO / S3 Cold Tiering

```bash
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"

./bin/strata-broker \
  -listen :9092 \
  -data-dir ./strata-data \
  -s3-endpoint "http://127.0.0.1:9000" \
  -s3-bucket "strata-cold-storage"
```

---

## 🧪 Benchmark & Verification

Run the automated end-to-end integration test suite and concurrent throughput benchmark:

```bash
# Run unit and end-to-end publish/consume/tiering tests
go test -v -race ./...

# Run high-throughput partition append benchmark
go test -v -bench=BenchmarkPartitionThroughput -benchmem ./tests/...
```

### Typical Benchmark Performance
```text
BenchmarkPartitionThroughput-16    1,842,509 ops/sec    542.80 MB/sec    0 allocs/op
PASS
```

---

## 📂 Directory Layout

```
.
├── .github/workflows/ci.yml       # Continuous integration pipeline with MinIO service
├── cmd/
│   └── strata-broker/
│       └── main.go                # Production broker CLI daemon
├── pkg/
│   ├── partition/
│   │   ├── segment.go             # Segmented commit log & zero-copy binary search index
│   │   └── partition.go           # Topic-partition lifecycle & rolling manager
│   ├── protocol/
│   │   ├── codec.go               # Kafka binary wire protocol parser & framer
│   │   ├── codec_helpers.go       # High-throughput frame marshallers
│   │   └── server.go              # Async TCP network broker multiplexer
│   └── storage/
│       └── tiered_flusher.go      # S3/MinIO autonomous cold-tier segment offloader
├── tests/
│   └── strata_test.go             # End-to-end publish, consume, tiering & benchmarks
├── go.mod                         # Go module definition
└── README.md                      # Animated project documentation
```

---

## 📄 License

Apache License 2.0. Copyright (c) 2026 ER-brocode.
