# Thunderduck Documentation

This directory contains comprehensive documentation for the thunderduck project.

---

## Core Documentation

### Architecture & Design
- **[PLATFORM_SUPPORT.md](PLATFORM_SUPPORT.md)** - Multi-architecture support (x86_64, ARM64)
- **[OPTIMIZATION_STRATEGY.md](OPTIMIZATION_STRATEGY.md)** - Why we use DuckDB's optimizer
- **[SPARK_CONNECT_PROTOCOL_SPEC.md](SPARK_CONNECT_PROTOCOL_SPEC.md)** - Spark Connect protocol details
- **[Testing_Strategy.md](Testing_Strategy.md)** - BDD and differential testing approach

### Spark Connect Architecture (architect/)
- **[SPARK_CONNECT_ARCHITECTURE.md](architect/SPARK_CONNECT_ARCHITECTURE.md)** - Server architecture
- **[SINGLE_SESSION_ARCHITECTURE.md](architect/SINGLE_SESSION_ARCHITECTURE.md)** - Session management design
- **[SPARK_CONNECT_QUICK_REFERENCE.md](architect/SPARK_CONNECT_QUICK_REFERENCE.md)** - Quick reference guide

### Build & Infrastructure (coder/)
- **[01_Build_Infrastructure_Design.md](coder/01_Build_Infrastructure_Design.md)** - Maven build system
- **[02_Testing_Infrastructure_Design.md](coder/02_Testing_Infrastructure_Design.md)** - Test framework
- **[03_Module_Organization_Design.md](coder/03_Module_Organization_Design.md)** - Module structure
- **[04_CI_CD_Integration_Design.md](coder/04_CI_CD_Integration_Design.md)** - CI/CD pipeline
- **[05_Data_Generation_Pipeline_Design.md](coder/05_Data_Generation_Pipeline_Design.md)** - Test data generation

### DataFrame API Documentation
- **[DataFrame_API_Reference.md](DataFrame_API_Reference.md)** - Comprehensive DataFrame API reference, implementation status, and migration guide
- **[TPC-DS_DataFrame_Implementation.md](TPC-DS_DataFrame_Implementation.md)** - TPC-DS DataFrame test suite with 34 queries (100% validation)

### Research & Analysis (research/)
Deep technical investigations and root cause analyses conducted during development:

- **[DUCKDB_TPCDS_DISCOVERY.md](research/DUCKDB_TPCDS_DISCOVERY.md)** - Discovery of DuckDB's TPC-DS implementation patterns
- **[TPCDS_ROOT_CAUSE_ANALYSIS.md](research/TPCDS_ROOT_CAUSE_ANALYSIS.md)** - Root cause analysis of Q36 & Q86 failures
- **[Q36_DUCKDB_LIMITATION.md](research/Q36_DUCKDB_LIMITATION.md)** - Documentation of Q36's GROUPING() in PARTITION BY limitation
- **[GROUPING_FUNCTION_ANALYSIS.md](research/GROUPING_FUNCTION_ANALYSIS.md)** - Analysis of GROUPING() function behavior
- **[GROUPING_STANDARD_RESEARCH.md](research/GROUPING_STANDARD_RESEARCH.md)** - Research on SQL standard GROUPING() semantics
- **[GROUPING_ANSWER.md](research/GROUPING_ANSWER.md)** - Definitive answer on GROUPING() standardization

### Testing
- **[Differential_Testing_Guide.md](Differential_Testing_Guide.md)** - Comprehensive guide for differential and end-to-end testing

### Test Scripts (../tests/scripts/)
Shell scripts for running tests and servers:

| Script | Description |
|--------|-------------|
| [start-server.sh](../tests/scripts/start-server.sh) | Start Thunderduck Spark Connect server |
| [start-spark-connect.sh](../tests/scripts/start-spark-connect.sh) | Start Apache Spark Connect server for differential testing |
| [stop-spark-connect.sh](../tests/scripts/stop-spark-connect.sh) | Stop Apache Spark Connect server |
| [run-differential-tests.sh](../tests/scripts/run-differential-tests.sh) | Run differential tests against Spark |
| [run-tpc-spark-connect-tests.sh](../tests/scripts/run-tpc-spark-connect-tests.sh) | Run TPC-H/TPC-DS benchmarks via Spark Connect |

### Troubleshooting & Fixes
- **[PROTOBUF_FIX_REPORT.md](PROTOBUF_FIX_REPORT.md)** - Resolution of protobuf version conflict causing VerifyError

### Development Journal (dev_journal/)
Weekly completion reports documenting the project's development progress:

| Week | Report | Key Achievements |
|------|--------|------------------|
| 1 | [WEEK1_COMPLETION_REPORT.md](dev_journal/WEEK1_COMPLETION_REPORT.md) | Project setup, initial architecture |
| 2 | [WEEK2_COMPLETION_REPORT.md](dev_journal/WEEK2_COMPLETION_REPORT.md) | Core infrastructure |
| 3 | [WEEK3_COMPLETION_REPORT.md](dev_journal/WEEK3_COMPLETION_REPORT.md) | DataFrame API foundation |
| 4 | [WEEK4_COMPLETION_REPORT.md](dev_journal/WEEK4_COMPLETION_REPORT.md) | TPC-H implementation |
| 5 | [WEEK5_COMPLETION_REPORT.md](dev_journal/WEEK5_COMPLETION_REPORT.md) | Query optimization |
| 7 | [WEEK7_FINAL_REPORT.md](dev_journal/WEEK7_FINAL_REPORT.md) | Final report |
| 8 | [WEEK8_COMPLETION_REPORT.md](dev_journal/WEEK8_COMPLETION_REPORT.md) | Advanced features |
| 10 | [WEEK10_COMPLETION_REPORT.md](dev_journal/WEEK10_COMPLETION_REPORT.md) | Spark Connect research |
| 11 | [WEEK11_COMPLETION_REPORT.md](dev_journal/WEEK11_COMPLETION_REPORT.md) | Spark Connect server |
| 11+ | [WEEK11_POST_CLEANUP_REPORT.md](dev_journal/WEEK11_POST_CLEANUP_REPORT.md) | Post-cleanup documentation |
| 12 | [WEEK12_COMPLETION_REPORT.md](dev_journal/WEEK12_COMPLETION_REPORT.md) | TPC-H via Spark Connect |
| 13 | [WEEK13_COMPLETION_REPORT.md](dev_journal/WEEK13_COMPLETION_REPORT.md) | TPC-DS implementation |
| 14 | [WEEK14_COMPLETION_REPORT.md](dev_journal/WEEK14_COMPLETION_REPORT.md) | DataFrame API expansion |
| 15 | [WEEK15_COMPLETION_REPORT.md](dev_journal/WEEK15_COMPLETION_REPORT.md) | Window functions |
| 16 | [WEEK16_COMPLETION_REPORT.md](dev_journal/WEEK16_COMPLETION_REPORT.md) | Production readiness |
| 17 | [WEEK17_COMPLETION_REPORT.md](dev_journal/WEEK17_COMPLETION_REPORT.md) | Protobuf fix, stabilization |

---

## Quick Links

**Getting Started**:
- Start with [../README.md](../README.md) - Project overview
- Then read [PLATFORM_SUPPORT.md](PLATFORM_SUPPORT.md) - Platform selection guide
- For testing: [Testing_Strategy.md](Testing_Strategy.md)

**For Developers**:
- Architecture: [architect/SPARK_CONNECT_ARCHITECTURE.md](architect/SPARK_CONNECT_ARCHITECTURE.md)
- Build system: [coder/01_Build_Infrastructure_Design.md](coder/01_Build_Infrastructure_Design.md)
- DataFrame API: [DataFrame_API_Reference.md](DataFrame_API_Reference.md)
- Optimization: [OPTIMIZATION_STRATEGY.md](OPTIMIZATION_STRATEGY.md)

**For Operations**:
- Platform support: [PLATFORM_SUPPORT.md](PLATFORM_SUPPORT.md)
- Spark Connect: [architect/SINGLE_SESSION_ARCHITECTURE.md](architect/SINGLE_SESSION_ARCHITECTURE.md)

---

**Last Updated**: 2025-12-09
