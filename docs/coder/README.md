# CODER Agent Design Documentation

This directory contains comprehensive infrastructure and architectural designs for the catalyst2sql project, created by the CODER agent in the Hive Mind collective intelligence system.

## Document Index

### 0. Summary
**[00_CODER_AGENT_SUMMARY.md](00_CODER_AGENT_SUMMARY.md)**
- Executive summary of all design work
- Key decisions and rationale
- Technology stack overview
- Success criteria and next steps

### 1. Build Infrastructure
**[01_Build_Infrastructure_Design.md](01_Build_Infrastructure_Design.md)**
- Maven multi-module project structure
- Dependency management strategy
- Build plugin configuration
- Build profiles and optimization
- Development workflow

**Key Highlights:**
- Build Tool: Maven 3.9+
- Java Version: 17 LTS (Phase 1-3), 21 (Phase 4+)
- Modules: core, formats, api, tests, benchmarks
- Build Time Target: < 15 minutes

### 2. Testing Infrastructure
**[02_Testing_Infrastructure_Design.md](02_Testing_Infrastructure_Design.md)**
- Unit testing with JUnit 5
- Integration testing strategy
- Differential testing (vs Apache Spark)
- Performance benchmarking with JMH
- Code coverage requirements

**Key Highlights:**
- Test Count: 500+ unit, 100+ integration, 50+ differential
- Coverage Target: 85%+ overall
- Frameworks: JUnit 5, AssertJ, Testcontainers, JMH
- Execution Time: < 3 min unit, < 10 min integration

### 3. Module Organization
**[03_Module_Organization_Design.md](03_Module_Organization_Design.md)**
- Package structure and class design
- Module dependencies and boundaries
- Code organization principles
- Cross-cutting concerns (logging, errors, config)
- API versioning strategy

**Key Highlights:**
- 5 Modules: core, formats, api, tests, benchmarks
- ~230 classes, ~29K LOC total
- Clear separation of concerns
- Loose coupling, high cohesion

### 4. CI/CD Integration
**[04_CI_CD_Integration_Design.md](04_CI_CD_Integration_Design.md)**
- GitHub Actions workflows
- Build matrix strategy (Java 17/21, x86_64/aarch64)
- Quality gates and checks
- Performance regression detection
- Artifact publishing to Maven Central

**Key Highlights:**
- Platform: GitHub Actions
- Matrix: Ubuntu/macOS, Java 17/21, Intel/ARM
- Quality Gates: Coverage ≥ 85%, zero warnings
- Automated Release: Maven Central + GitHub Releases

### 5. Data Generation Pipeline
**[05_Data_Generation_Pipeline_Design.md](05_Data_Generation_Pipeline_Design.md)**
- Synthetic data generation framework
- TPC-H benchmark data pipeline
- TPC-DS benchmark data pipeline
- Format-specific generators (Parquet, Delta, Iceberg)
- Data validation and quality checks

**Key Highlights:**
- Synthetic: Schema-based with realistic distributions
- TPC-H: 8 tables, 22 queries, SF 1-1000
- TPC-DS: 24 tables, 99 queries, SF 1-1000
- Formats: Parquet, Delta Lake, Iceberg

## Quick Start

### For Developers

1. **Read the summary first**
   - Start with [00_CODER_AGENT_SUMMARY.md](00_CODER_AGENT_SUMMARY.md)
   - Get overview of entire architecture

2. **Set up build environment**
   - Follow [01_Build_Infrastructure_Design.md](01_Build_Infrastructure_Design.md)
   - Install Maven 3.9+, Java 17
   - Clone repository and run `mvn clean install`

3. **Understand testing strategy**
   - Read [02_Testing_Infrastructure_Design.md](02_Testing_Infrastructure_Design.md)
   - Set up JUnit 5 tests
   - Generate test data

4. **Follow module structure**
   - Review [03_Module_Organization_Design.md](03_Module_Organization_Design.md)
   - Understand package organization
   - Follow naming conventions

### For DevOps/CI Engineers

1. **Set up CI/CD pipeline**
   - Follow [04_CI_CD_Integration_Design.md](04_CI_CD_Integration_Design.md)
   - Configure GitHub Actions workflows
   - Set up Maven Central credentials

2. **Configure quality gates**
   - Set up branch protection rules
   - Configure code coverage checks
   - Enable security scanning

3. **Set up benchmark tracking**
   - Configure nightly benchmark runs
   - Set up performance dashboards
   - Enable regression alerts

### For QA/Test Engineers

1. **Understand test strategy**
   - Read [02_Testing_Infrastructure_Design.md](02_Testing_Infrastructure_Design.md)
   - Understand test pyramid
   - Learn differential testing approach

2. **Generate test data**
   - Follow [05_Data_Generation_Pipeline_Design.md](05_Data_Generation_Pipeline_Design.md)
   - Generate TPC-H/TPC-DS data
   - Create synthetic test datasets

3. **Run test suites**
   ```bash
   # Unit tests
   mvn test

   # Integration tests
   mvn verify

   # Differential tests
   mvn test -Dgroups=differential

   # Benchmarks
   mvn clean install -Pbenchmarks
   cd benchmarks
   java -jar target/benchmarks.jar
   ```

## Technology Stack Reference

| Component | Technology | Version | Purpose |
|-----------|------------|---------|---------|
| Build Tool | Maven | 3.9+ | Project build and dependency management |
| Language | Java | 17 / 21 | Implementation language |
| Database | DuckDB | 1.1.3 | Embedded SQL engine |
| Arrow | Apache Arrow | 17.0.0 | Zero-copy data interchange |
| Spark API | Apache Spark | 3.5.3 | API compatibility (provided scope) |
| Test Framework | JUnit | 5.10.0 | Unit and integration testing |
| Assertions | AssertJ | 3.24.2 | Fluent test assertions |
| Containers | Testcontainers | 1.19.0 | Integration test infrastructure |
| Benchmarking | JMH | 1.37 | Performance benchmarking |
| Coverage | JaCoCo | 0.8.10 | Code coverage analysis |
| CI/CD | GitHub Actions | - | Continuous integration |
| Logging | SLF4J + Logback | 2.0.9 / 1.4.11 | Application logging |

## Build Commands Reference

### Common Maven Commands

```bash
# Clean build with tests
mvn clean install

# Fast build (skip tests)
mvn clean install -Pfast

# Run tests with coverage
mvn clean verify -Pcoverage

# Run specific test
mvn test -Dtest=TypeMapperTest

# Run integration tests only
mvn verify -DskipUnitTests

# Run benchmarks
mvn clean install -Pbenchmarks
cd benchmarks
java -jar target/benchmarks.jar

# Build with parallel execution
mvn clean install -T 4

# Deploy to Maven Central
mvn clean deploy -Prelease
```

### Data Generation Commands

```bash
# Generate TPC-H data (scale factor 10)
java -jar testdata.jar tpch --scale 10 --output data/tpch_sf10

# Generate TPC-DS data (scale factor 1)
java -jar testdata.jar tpcds --scale 1 --output data/tpcds_sf1

# Generate synthetic data
java -jar testdata.jar synthetic --rows 1000000 --output data/synthetic

# Generate Delta Lake table with versions
java -jar testdata.jar synthetic --format delta --versions 5
```

### CI/CD Commands

```bash
# Run CI pipeline locally (with act)
act -j build-matrix

# Run quality gates
act -j quality-gate

# Run benchmarks locally
act -j benchmark-tpch
```

## Design Principles

1. **Modularity**: Clear module boundaries, minimal coupling
2. **Testability**: Every component independently testable
3. **Maintainability**: Clean code, good documentation
4. **Performance**: Fast builds, efficient tests
5. **Extensibility**: Easy to add features

## Performance Targets

| Metric | Target | Phase |
|--------|--------|-------|
| Build time (full) | < 15 min | Phase 1 |
| Unit test time | < 3 min | Phase 1 |
| Integration test time | < 10 min | Phase 2 |
| Code coverage | ≥ 85% | Phase 3 |
| Query performance | 5-10x faster than Spark | Phase 3 |
| Memory efficiency | 6-8x less than Spark | Phase 3 |

## Implementation Phases

- **Phase 1** (Weeks 1-3): Foundation - Basic operations, Parquet support
- **Phase 2** (Weeks 4-6): Advanced operations - Delta/Iceberg, joins, aggregations
- **Phase 3** (Weeks 7-9): Optimization - Performance tuning, production hardening
- **Phase 4** (Weeks 10-12): Spark Connect Server - gRPC, multi-client support

## Contributing Guidelines

1. Follow module structure in [03_Module_Organization_Design.md](03_Module_Organization_Design.md)
2. Write tests for all new code (target: 85%+ coverage)
3. Run quality checks before committing: `mvn verify -Pcoverage`
4. Follow code style (Checkstyle, SpotBugs, PMD)
5. Update documentation for API changes

## Support and Questions

For questions about:
- **Build issues**: See [01_Build_Infrastructure_Design.md](01_Build_Infrastructure_Design.md)
- **Testing**: See [02_Testing_Infrastructure_Design.md](02_Testing_Infrastructure_Design.md)
- **Module structure**: See [03_Module_Organization_Design.md](03_Module_Organization_Design.md)
- **CI/CD**: See [04_CI_CD_Integration_Design.md](04_CI_CD_Integration_Design.md)
- **Test data**: See [05_Data_Generation_Pipeline_Design.md](05_Data_Generation_Pipeline_Design.md)

## Document Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-10-13 | CODER Agent | Initial design documentation |

## Next Steps

1. **Implementation Team**: Begin Phase 1 development
   - Set up repository structure
   - Create parent POM and module POMs
   - Implement core logical plan classes
   - Set up test framework

2. **DevOps Team**: Set up CI/CD infrastructure
   - Configure GitHub Actions workflows
   - Set up Maven Central publishing
   - Configure code coverage tracking

3. **QA Team**: Generate test data
   - Set up TPC-H data generation
   - Create synthetic test datasets
   - Prepare differential test framework

---

**Status**: Design phase complete, ready for implementation
**Created by**: CODER Agent (Hive Mind Collective Intelligence)
**Date**: October 13, 2025
