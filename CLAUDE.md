# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Go-based experimental project evaluating NATS JetStream as a primary data store alternative to SQLite. The project contains three distinct experiments demonstrating different aspects of NATS JetStream usage: point store, time-series database, and bi-directional synchronization.

## Development Commands

### Build and Run
```bash
# Build all experiments
go build ./...

# Run individual experiments
go run ./point-store/          # Point store experiment
go run ./tsd/                  # Time-series database experiment  
go run ./bi-directional-sync/  # Bi-directional sync experiment

# Pre-compiled TSD benchmark (already available as ./t)
./t                            # Runs time-series performance tests
```

### Development Workflow
```bash
# Standard Go development
go mod tidy                    # Clean up dependencies
go mod download                # Download dependencies
go fmt ./...                   # Format code
go list -m all                 # Check module status
```

## Architecture

The project is organized around three self-contained experiments:

1. **Point Store** (`point-store/`) - Uses NATS streams as a key-value point store with `GetLastMsgForSubject` for current state retrieval
2. **Time-Series Database** (`tsd/`) - Performance benchmarks for time-series data with sync/async publishing comparisons
3. **Bi-directional Sync** (`bi-directional-sync/`) - Hub-leaf synchronization using JetStream domains and stream sources

### Common Patterns
- All experiments use embedded NATS servers (no external dependencies)
- JetStream streams with hierarchical subject patterns (`n.{nodeId}.{property}`)
- Context-based timeout handling for all operations
- Automatic resource cleanup and temporary directory management
- Stream configuration through declarative setup

### Key Dependencies
- `github.com/nats-io/nats-server/v2 v2.11.8` - NATS server
- `github.com/nats-io/nats.go v1.45.0` - NATS Go client
- Go version 1.23.0+ (using toolchain go1.24.6)

## Development Notes

- Each experiment is independent and self-contained
- No external NATS server required - uses embedded instances
- The `t` executable is a pre-compiled version of the TSD performance test
- All experiments log detailed performance metrics to stdout
- Temporary JetStream state directories are created and cleaned automatically