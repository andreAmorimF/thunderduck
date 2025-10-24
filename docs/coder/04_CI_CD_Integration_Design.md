# CI/CD Integration Strategy for thunderduck

## Executive Summary

This document defines the continuous integration and deployment strategy, including GitHub Actions workflows, build automation, test execution, artifact publishing, and performance tracking.

**Key Components:**
- **GitHub Actions**: Primary CI/CD platform
- **Build Matrix**: Test across Java 17/21, Intel/ARM architectures
- **Automated Testing**: Unit, integration, differential, benchmarks
- **Artifact Publishing**: Maven Central deployment
- **Performance Tracking**: Benchmark result tracking over time

## 1. CI/CD Pipeline Overview

```
┌─────────────┐
│   PR Open   │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────┐
│  Build & Unit Tests (Matrix)       │
│  - Java 17, 21                      │
│  - x86_64, aarch64                  │
│  - Ubuntu, macOS                    │
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│  Integration Tests                  │
│  - End-to-end workflows             │
│  - Format compatibility             │
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│  Differential Tests                 │
│  - Compare with real Spark          │
│  - Verify correctness               │
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│  Code Quality Checks                │
│  - Coverage (85%+)                  │
│  - Static analysis                  │
│  - Dependency check                 │
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│  PR Approved & Merged to main       │
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│  Nightly Benchmarks (main)          │
│  - TPC-H suite                      │
│  - TPC-DS suite                     │
│  - Performance regression detection │
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│  Release (Tagged Commits)           │
│  - Build release artifacts          │
│  - Publish to Maven Central         │
│  - Generate changelog               │
└─────────────────────────────────────┘
```

## 2. GitHub Actions Workflows

### Primary Workflow: Build and Test

```yaml
# .github/workflows/build-test.yml
name: Build and Test

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main, develop ]

jobs:
  build-matrix:
    name: Build (${{ matrix.os }}, Java ${{ matrix.java }})
    runs-on: ${{ matrix.os }}
    strategy:
      fail-fast: false
      matrix:
        os: [ubuntu-22.04, macos-13, macos-14]  # Intel + ARM
        java: [17, 21]
        include:
          # Ubuntu: x86_64
          - os: ubuntu-22.04
            arch: x86_64
          # macOS 13: Intel x86_64
          - os: macos-13
            arch: x86_64
          # macOS 14: Apple Silicon (ARM)
          - os: macos-14
            arch: aarch64

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Setup Java ${{ matrix.java }}
        uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: ${{ matrix.java }}
          cache: 'maven'

      - name: Cache Maven dependencies
        uses: actions/cache@v3
        with:
          path: ~/.m2/repository
          key: ${{ runner.os }}-maven-${{ hashFiles('**/pom.xml') }}
          restore-keys: |
            ${{ runner.os }}-maven-

      - name: Build project
        run: mvn clean compile -B

      - name: Run unit tests
        run: mvn test -B
        env:
          MAVEN_OPTS: -Xmx2g

      - name: Upload test results
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: test-results-${{ matrix.os }}-java${{ matrix.java }}
          path: |
            **/target/surefire-reports/*.xml
            **/target/failsafe-reports/*.xml

  integration-tests:
    name: Integration Tests
    runs-on: ubuntu-22.04
    needs: build-matrix
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Setup Java 17
        uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: 17
          cache: 'maven'

      - name: Run integration tests
        run: mvn verify -Pintegration -B
        env:
          MAVEN_OPTS: -Xmx4g

      - name: Upload integration test results
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: integration-test-results
          path: tests/target/failsafe-reports/**

  differential-tests:
    name: Differential Tests (vs Spark)
    runs-on: ubuntu-22.04
    needs: build-matrix
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Setup Java 17
        uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: 17
          cache: 'maven'

      - name: Generate test data
        run: |
          mvn test-compile -B
          mvn exec:java -Dexec.mainClass="com.thunderduck.testdata.DataGenerator" -pl tests

      - name: Run differential tests
        run: mvn test -Dgroups=differential -B
        env:
          MAVEN_OPTS: -Xmx4g

      - name: Upload differential test results
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: differential-test-results
          path: tests/target/surefire-reports/**

  code-quality:
    name: Code Quality Checks
    runs-on: ubuntu-22.04
    needs: build-matrix
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Setup Java 17
        uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: 17
          cache: 'maven'

      - name: Run tests with coverage
        run: mvn verify -Pcoverage -B

      - name: Upload coverage to Codecov
        uses: codecov/codecov-action@v3
        with:
          files: ./target/site/jacoco/jacoco.xml
          flags: unittests
          name: codecov-umbrella
          fail_ci_if_error: true

      - name: Check coverage threshold
        run: |
          mvn jacoco:check -Pcoverage
          # Fails if coverage < 85%

      - name: Run static analysis
        run: mvn spotbugs:check checkstyle:check pmd:check

      - name: Check dependencies for vulnerabilities
        run: mvn dependency-check:check
```

### Nightly Benchmarks

```yaml
# .github/workflows/nightly-benchmarks.yml
name: Nightly Benchmarks

on:
  schedule:
    - cron: '0 2 * * *'  # Run at 2 AM UTC daily
  workflow_dispatch:      # Allow manual trigger

jobs:
  benchmark-tpch:
    name: TPC-H Benchmarks
    runs-on: ubuntu-22.04
    timeout-minutes: 120
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Setup Java 17
        uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: 17
          cache: 'maven'

      - name: Build benchmarks
        run: mvn clean install -Pbenchmarks -DskipTests -B

      - name: Download TPC-H data generator
        run: |
          cd benchmarks
          git clone https://github.com/databricks/tpch-dbgen.git
          cd tpch-dbgen
          make

      - name: Generate TPC-H data (SF=10)
        run: |
          cd benchmarks/tpch-dbgen
          ./dbgen -s 10 -f
          mkdir -p ../data/tpch_sf10
          mv *.tbl ../data/tpch_sf10/

      - name: Run TPC-H benchmarks
        run: |
          cd benchmarks
          java -jar target/benchmarks.jar \
            -rf json \
            -rff tpch-results.json \
            TPCHBenchmark

      - name: Store benchmark results
        uses: benchmark-action/github-action-benchmark@v1
        with:
          name: TPC-H Benchmarks
          tool: 'jmh'
          output-file-path: benchmarks/tpch-results.json
          github-token: ${{ secrets.GITHUB_TOKEN }}
          auto-push: true
          alert-threshold: '150%'  # Alert if 50% slower
          comment-on-alert: true
          fail-on-alert: false

  benchmark-micro:
    name: Micro Benchmarks
    runs-on: ubuntu-22.04
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Setup Java 17
        uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: 17
          cache: 'maven'

      - name: Build benchmarks
        run: mvn clean install -Pbenchmarks -DskipTests -B

      - name: Run micro benchmarks
        run: |
          cd benchmarks
          java -jar target/benchmarks.jar \
            -rf json \
            -rff micro-results.json \
            '.*Benchmark$'

      - name: Store benchmark results
        uses: benchmark-action/github-action-benchmark@v1
        with:
          name: Micro Benchmarks
          tool: 'jmh'
          output-file-path: benchmarks/micro-results.json
          github-token: ${{ secrets.GITHUB_TOKEN }}
          auto-push: true
```

### Release Workflow

```yaml
# .github/workflows/release.yml
name: Release

on:
  push:
    tags:
      - 'v*.*.*'

jobs:
  release:
    name: Build and Release
    runs-on: ubuntu-22.04
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Setup Java 17
        uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: 17
          cache: 'maven'
          server-id: ossrh
          server-username: MAVEN_USERNAME
          server-password: MAVEN_PASSWORD
          gpg-private-key: ${{ secrets.GPG_PRIVATE_KEY }}
          gpg-passphrase: MAVEN_GPG_PASSPHRASE

      - name: Extract version from tag
        id: version
        run: echo "VERSION=${GITHUB_REF#refs/tags/v}" >> $GITHUB_OUTPUT

      - name: Set version in POM
        run: mvn versions:set -DnewVersion=${{ steps.version.outputs.VERSION }} -B

      - name: Build and deploy to Maven Central
        run: mvn clean deploy -Prelease -B
        env:
          MAVEN_USERNAME: ${{ secrets.OSSRH_USERNAME }}
          MAVEN_PASSWORD: ${{ secrets.OSSRH_TOKEN }}
          MAVEN_GPG_PASSPHRASE: ${{ secrets.GPG_PASSPHRASE }}

      - name: Create GitHub Release
        uses: softprops/action-gh-release@v1
        with:
          files: |
            api/target/thunderduck-api-*.jar
            core/target/thunderduck-core-*.jar
            formats/target/thunderduck-formats-*.jar
          generate_release_notes: true
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}

      - name: Build documentation
        run: mvn javadoc:aggregate -B

      - name: Deploy documentation to GitHub Pages
        uses: peaceiris/actions-gh-pages@v3
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          publish_dir: ./target/site/apidocs
```

## 3. Build Optimization

### Maven Daemon Setup

```yaml
# For faster builds, use Maven Daemon
- name: Setup Maven Daemon
  run: |
    curl -s "https://get.sdkman.io" | bash
    source "$HOME/.sdkman/bin/sdkman-init.sh"
    sdk install mvnd
    echo "$HOME/.sdkman/candidates/mvnd/current/bin" >> $GITHUB_PATH
```

### Caching Strategy

```yaml
# Aggressive caching for faster builds
- name: Cache Maven dependencies
  uses: actions/cache@v3
  with:
    path: |
      ~/.m2/repository
      ~/.mvnd
    key: ${{ runner.os }}-maven-${{ hashFiles('**/pom.xml') }}
    restore-keys: |
      ${{ runner.os }}-maven-

- name: Cache DuckDB extensions
  uses: actions/cache@v3
  with:
    path: ~/.duckdb
    key: duckdb-extensions-${{ runner.os }}
```

## 4. Self-Hosted Runners (Future)

### For Hardware-Specific Testing

```yaml
# .github/workflows/hardware-tests.yml
name: Hardware-Specific Tests

on:
  schedule:
    - cron: '0 4 * * 0'  # Weekly on Sunday

jobs:
  test-graviton:
    name: Test on AWS Graviton (ARM)
    runs-on: [self-hosted, linux, arm64, graviton3]
    steps:
      # Test ARM NEON optimizations
      - name: Run on Graviton3
        run: mvn verify -Phardware-tests

  test-intel-avx512:
    name: Test on Intel with AVX-512
    runs-on: [self-hosted, linux, x86_64, avx512]
    steps:
      # Test Intel AVX-512 optimizations
      - name: Run on Intel Ice Lake
        run: mvn verify -Phardware-tests
```

## 5. Quality Gates

### Required Checks for PR Merge

```yaml
# .github/workflows/quality-gates.yml
name: Quality Gates

on:
  pull_request:
    types: [opened, synchronize, reopened]

jobs:
  quality-gate:
    name: Quality Gate
    runs-on: ubuntu-22.04
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Setup Java 17
        uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: 17
          cache: 'maven'

      - name: Check code coverage
        run: |
          mvn verify -Pcoverage
          mvn jacoco:check -Pcoverage
          # Fails if coverage < 85%

      - name: Check for test failures
        run: mvn test -B

      - name: Check for compiler warnings
        run: mvn clean compile -Werror

      - name: Check dependency vulnerabilities
        run: mvn dependency-check:check

      - name: Check for outdated dependencies
        run: mvn versions:display-dependency-updates

      - name: Static analysis
        run: |
          mvn spotbugs:check
          mvn checkstyle:check
          mvn pmd:check

      - name: Comment on PR
        if: always()
        uses: actions/github-script@v6
        with:
          script: |
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: 'Quality gate checks completed. See details in Actions.'
            })
```

## 6. Performance Regression Detection

### Automated Performance Alerts

```yaml
# In nightly-benchmarks.yml
- name: Store benchmark results
  uses: benchmark-action/github-action-benchmark@v1
  with:
    name: Performance Benchmarks
    tool: 'jmh'
    output-file-path: benchmarks/results.json
    github-token: ${{ secrets.GITHUB_TOKEN }}
    auto-push: true
    # Alert if performance degrades by 20%
    alert-threshold: '120%'
    comment-on-alert: true
    fail-on-alert: true
    # Compare against baseline
    alert-comment-cc-users: '@coder-agent'
```

## 7. Artifact Management

### Maven Central Publishing

```xml
<!-- pom.xml -->
<distributionManagement>
    <repository>
        <id>ossrh</id>
        <name>Central Repository OSSRH</name>
        <url>https://oss.sonatype.org/service/local/staging/deploy/maven2/</url>
    </repository>
    <snapshotRepository>
        <id>ossrh</id>
        <name>OSS Sonatype Snapshots</name>
        <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    </snapshotRepository>
</distributionManagement>
```

### Docker Image Publishing (Future)

```yaml
# .github/workflows/docker-publish.yml
name: Publish Docker Image

on:
  release:
    types: [published]

jobs:
  docker:
    runs-on: ubuntu-22.04
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Login to Docker Hub
        uses: docker/login-action@v3
        with:
          username: ${{ secrets.DOCKERHUB_USERNAME }}
          password: ${{ secrets.DOCKERHUB_TOKEN }}

      - name: Build and push
        uses: docker/build-push-action@v5
        with:
          context: .
          platforms: linux/amd64,linux/arm64
          push: true
          tags: |
            thunderduck/thunderduck:latest
            thunderduck/thunderduck:${{ github.ref_name }}
```

## 8. Monitoring and Alerts

### Status Badges

```markdown
# README.md badges
[![Build Status](https://github.com/thunderduck/thunderduck/workflows/Build%20and%20Test/badge.svg)](https://github.com/thunderduck/thunderduck/actions)
[![Code Coverage](https://codecov.io/gh/thunderduck/thunderduck/branch/main/graph/badge.svg)](https://codecov.io/gh/thunderduck/thunderduck)
[![Maven Central](https://img.shields.io/maven-central/v/com.thunderduck/thunderduck-api)](https://central.sonatype.com/artifact/com.thunderduck/thunderduck-api)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
```

### Slack Notifications

```yaml
# Add to workflows
- name: Notify Slack on failure
  if: failure()
  uses: 8398a7/action-slack@v3
  with:
    status: ${{ job.status }}
    text: 'Build failed on ${{ github.ref }}'
    webhook_url: ${{ secrets.SLACK_WEBHOOK }}
```

## 9. Branch Protection Rules

### Required Status Checks

```
Branch: main
Require status checks:
  ✓ Build (ubuntu-22.04, Java 17)
  ✓ Build (ubuntu-22.04, Java 21)
  ✓ Build (macos-14, Java 17)
  ✓ Integration Tests
  ✓ Differential Tests
  ✓ Code Quality

Require review from code owners: Yes
Require linear history: Yes
Include administrators: No
```

## 10. CI/CD Metrics

### Track and Monitor

| Metric | Target | Current |
|--------|--------|---------|
| Build time (full) | < 15 min | TBD |
| Unit test time | < 3 min | TBD |
| Integration test time | < 10 min | TBD |
| Build success rate | > 95% | TBD |
| Test flakiness | < 1% | TBD |
| Code coverage | > 85% | TBD |

## 11. Local CI/CD Testing

### Act (Run GitHub Actions Locally)

```bash
# Install act
brew install act  # macOS
curl https://raw.githubusercontent.com/nektos/act/master/install.sh | sudo bash  # Linux

# Run workflows locally
act -j build-matrix
act -j integration-tests

# Run specific workflow
act -W .github/workflows/build-test.yml
```

### Pre-commit Hooks

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: mvn-test
        name: Maven Test
        entry: mvn test -B
        language: system
        pass_filenames: false

      - id: checkstyle
        name: Checkstyle
        entry: mvn checkstyle:check
        language: system
        pass_filenames: false
```

## Summary

This CI/CD strategy provides:

- **Comprehensive Testing**: Unit, integration, differential, benchmarks
- **Multi-Platform**: Test on Intel and ARM architectures
- **Automated Quality Gates**: Coverage, static analysis, security
- **Performance Tracking**: Detect regressions automatically
- **Efficient Builds**: Caching, parallel execution
- **Automated Releases**: Maven Central deployment
- **Monitoring**: Status badges, Slack notifications

**Build Time Targets:**
- PR validation: 10-15 minutes
- Full test suite: 30-40 minutes
- Nightly benchmarks: 60-90 minutes

**Next Steps:**
1. Set up GitHub Actions workflows
2. Configure Maven Central publishing
3. Implement benchmark tracking
4. Set up branch protection rules
5. Configure notifications
