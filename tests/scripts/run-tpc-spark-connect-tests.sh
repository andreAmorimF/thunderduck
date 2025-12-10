#!/bin/bash

# Script to run TPC-H and TPC-DS benchmarks through Spark Connect interface
# This tests the full Spark Connect protocol implementation
# Run from project root: ./tests/scripts/run-tpc-spark-connect-tests.sh

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Change to project root
cd "$PROJECT_ROOT"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
THUNDERDUCK_PORT=50051
SPARK_PORT=15002
TEST_TYPE=${1:-"both"}  # tpch, tpcds, or both
SCALE_FACTOR=${2:-"0.01"}

echo -e "${BLUE}====================================================================${NC}"
echo -e "${BLUE}TPC Benchmark Testing via Spark Connect${NC}"
echo -e "${BLUE}====================================================================${NC}"

# Function to check if thunderduck server is running
check_thunderduck() {
    echo -e "${BLUE}Checking thunderduck Spark Connect server...${NC}"

    if lsof -i :$THUNDERDUCK_PORT > /dev/null 2>&1; then
        echo -e "${GREEN}✓ thunderduck server is running on port $THUNDERDUCK_PORT${NC}"
        return 0
    else
        echo -e "${YELLOW}thunderduck server is not running${NC}"
        echo -e "${YELLOW}Starting thunderduck server...${NC}"

        # Start thunderduck server in background
        "$SCRIPT_DIR/start-server.sh" > /tmp/thunderduck.log 2>&1 &
        THUNDERDUCK_PID=$!

        # Wait for server to start
        sleep 5

        if lsof -i :$THUNDERDUCK_PORT > /dev/null 2>&1; then
            echo -e "${GREEN}✓ thunderduck server started (PID: $THUNDERDUCK_PID)${NC}"
            return 0
        else
            echo -e "${RED}Failed to start thunderduck server${NC}"
            echo "Check logs at: /tmp/thunderduck.log"
            return 1
        fi
    fi
}

# Function to run TPC-H tests
run_tpch_tests() {
    echo ""
    echo -e "${BLUE}====================================================================${NC}"
    echo -e "${BLUE}Running TPC-H Tests (Scale Factor: $SCALE_FACTOR)${NC}"
    echo -e "${BLUE}====================================================================${NC}"

    # Check if TPC-H data exists
    TPCH_DATA="./data/tpch_sf${SCALE_FACTOR/./}"
    if [ ! -d "$TPCH_DATA" ]; then
        echo -e "${YELLOW}TPC-H data not found at $TPCH_DATA${NC}"
        echo "Generate TPC-H data first with:"
        echo "  ./generate-tpch-data.sh $SCALE_FACTOR"
        return 1
    fi

    # Run TPC-H tests through Spark Connect
    echo -e "${BLUE}Testing TPC-H queries through thunderduck Spark Connect...${NC}"

    mvn test -pl tests \
        -Dtest=TPCHSparkConnectTest \
        -Dtpch.spark.connect.enabled=true \
        -Dthunderduck.url=sc://localhost:$THUNDERDUCK_PORT \
        -Dtpch.data.path=$TPCH_DATA

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ TPC-H tests passed${NC}"
    else
        echo -e "${RED}✗ TPC-H tests failed${NC}"
    fi
}

# Function to run TPC-DS tests
run_tpcds_tests() {
    echo ""
    echo -e "${BLUE}====================================================================${NC}"
    echo -e "${BLUE}Running TPC-DS Tests (Scale Factor: $SCALE_FACTOR)${NC}"
    echo -e "${BLUE}====================================================================${NC}"

    # Check if TPC-DS data exists
    TPCDS_DATA="./data/tpcds_sf${SCALE_FACTOR/./}"
    if [ ! -d "$TPCDS_DATA" ]; then
        echo -e "${YELLOW}TPC-DS data not found at $TPCDS_DATA${NC}"
        echo "Generate TPC-DS data first with:"
        echo "  ./generate-tpcds-data.sh $SCALE_FACTOR"
        return 1
    fi

    # Run TPC-DS tests through Spark Connect
    echo -e "${BLUE}Testing TPC-DS queries through thunderduck Spark Connect...${NC}"

    mvn test -pl tests \
        -Dtest=TPCDSSparkConnectTest \
        -Dtpcds.spark.connect.enabled=true \
        -Dthunderduck.url=sc://localhost:$THUNDERDUCK_PORT \
        -Dtpcds.data.path=$TPCDS_DATA

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ TPC-DS tests passed${NC}"
    else
        echo -e "${RED}✗ TPC-DS tests failed${NC}"
    fi
}

# Function to run comparison tests against real Spark
run_comparison_tests() {
    echo ""
    echo -e "${BLUE}====================================================================${NC}"
    echo -e "${BLUE}Running Comparison Tests: thunderduck vs Apache Spark${NC}"
    echo -e "${BLUE}====================================================================${NC}"

    # Check if Spark Connect is running
    if ! lsof -i :$SPARK_PORT > /dev/null 2>&1; then
        echo -e "${YELLOW}Apache Spark Connect not running${NC}"
        echo "Start it with: ./tests/scripts/start-spark-connect.sh"
        return 1
    fi

    echo -e "${BLUE}Comparing results between:${NC}"
    echo "  - Apache Spark on port $SPARK_PORT"
    echo "  - thunderduck on port $THUNDERDUCK_PORT"

    mvn test -pl tests \
        -Dtest=TPCComparativeTest \
        -Dspark.url=sc://localhost:$SPARK_PORT \
        -Dthunderduck.url=sc://localhost:$THUNDERDUCK_PORT \
        -Dtpc.scale=$SCALE_FACTOR

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Comparison tests passed - thunderduck matches Spark!${NC}"
    else
        echo -e "${RED}✗ Comparison tests failed - divergences found${NC}"
        echo "Check divergence reports in: target/differential-reports/"
    fi
}

# Function to run performance comparison
run_performance_comparison() {
    echo ""
    echo -e "${BLUE}====================================================================${NC}"
    echo -e "${BLUE}Performance Comparison: thunderduck vs Apache Spark${NC}"
    echo -e "${BLUE}====================================================================${NC}"

    echo -e "${BLUE}Running performance benchmarks...${NC}"

    # Run TPC-H performance test
    echo "TPC-H Performance:"
    java -cp benchmarks/target/benchmarks.jar \
        com.thunderduck.benchmark.SparkConnectTPCHBenchmark \
        --thunderduck-url sc://localhost:$THUNDERDUCK_PORT \
        --spark-url sc://localhost:$SPARK_PORT \
        --scale $SCALE_FACTOR

    # Run TPC-DS performance test
    echo ""
    echo "TPC-DS Performance:"
    java -cp benchmarks/target/benchmarks.jar \
        com.thunderduck.benchmark.SparkConnectTPCDSBenchmark \
        --thunderduck-url sc://localhost:$THUNDERDUCK_PORT \
        --spark-url sc://localhost:$SPARK_PORT \
        --scale $SCALE_FACTOR
}

# Main execution
main() {
    # Check prerequisites
    if [ ! -f "pom.xml" ]; then
        echo -e "${RED}Must run from thunderduck root directory${NC}"
        exit 1
    fi

    # Build project if needed
    if [ ! -f "connect-server/target/thunderduck-connect-server-1.0-SNAPSHOT.jar" ]; then
        echo -e "${YELLOW}Building thunderduck...${NC}"
        mvn clean package -DskipTests
    fi

    # Check thunderduck server
    if ! check_thunderduck; then
        exit 1
    fi

    # Run tests based on type
    case $TEST_TYPE in
        tpch)
            run_tpch_tests
            ;;
        tpcds)
            run_tpcds_tests
            ;;
        both)
            run_tpch_tests
            run_tpcds_tests
            ;;
        compare)
            run_comparison_tests
            ;;
        perf)
            run_performance_comparison
            ;;
        *)
            echo -e "${RED}Invalid test type: $TEST_TYPE${NC}"
            echo "Usage: $0 [tpch|tpcds|both|compare|perf] [scale_factor]"
            exit 1
            ;;
    esac

    echo ""
    echo -e "${BLUE}====================================================================${NC}"
    echo -e "${BLUE}Test Summary${NC}"
    echo -e "${BLUE}====================================================================${NC}"

    # Generate summary report
    if [ -f "target/test-reports/summary.json" ]; then
        python3 -c "
import json
with open('target/test-reports/summary.json') as f:
    data = json.load(f)
    print(f\"Total Tests: {data['total']}\")
    print(f\"Passed: {data['passed']}\")
    print(f\"Failed: {data['failed']}\")
    print(f\"Success Rate: {data['passed']/data['total']*100:.1f}%\")
"
    fi

    echo ""
    echo -e "${BLUE}Available Commands:${NC}"
    echo "  $0 tpch         # Run only TPC-H tests"
    echo "  $0 tpcds        # Run only TPC-DS tests"
    echo "  $0 both         # Run both TPC-H and TPC-DS tests"
    echo "  $0 compare      # Compare thunderduck vs Spark"
    echo "  $0 perf         # Run performance comparison"
    echo ""
    echo "Scale factors: 0.01, 0.1, 1, 10, 100"
}

# Run main function
main