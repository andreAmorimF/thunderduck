#!/bin/bash

# Script to run differential tests between thunderduck and Apache Spark
# Run from project root: ./tests/scripts/run-differential-tests.sh

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
SPARK_CONNECT_URL=${SPARK_CONNECT_URL:-"sc://localhost:15002"}
TEST_PATTERN=${1:-"*Test"}
VERBOSE=${VERBOSE:-false}

echo -e "${BLUE}====================================================================${NC}"
echo -e "${BLUE}Running Differential Tests: thunderduck vs Apache Spark${NC}"
echo -e "${BLUE}====================================================================${NC}"

# Function to check if Spark Connect is running
check_spark_connect() {
    echo -e "${BLUE}Checking Spark Connect server...${NC}"

    # Try to connect with pyspark
    python3 -c "
from pyspark.sql import SparkSession
try:
    spark = SparkSession.builder.remote('${SPARK_CONNECT_URL}').getOrCreate()
    print('✓ Successfully connected to Spark at ${SPARK_CONNECT_URL}')
    spark.stop()
    exit(0)
except Exception as e:
    print(f'✗ Failed to connect: {e}')
    exit(1)
" 2>/dev/null

    if [ $? -ne 0 ]; then
        echo -e "${YELLOW}WARNING: Cannot connect to Spark Connect at ${SPARK_CONNECT_URL}${NC}"
        echo -e "${YELLOW}Please start Spark Connect server first:${NC}"
        echo "  ./tests/scripts/start-spark-connect.sh"
        echo ""
        read -p "Do you want to start it now? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            "$SCRIPT_DIR/start-spark-connect.sh"
            sleep 5
        else
            echo -e "${RED}Exiting. Please start Spark Connect manually.${NC}"
            exit 1
        fi
    else
        echo -e "${GREEN}✓ Spark Connect server is running${NC}"
    fi
}

# Function to build thunderduck if needed
build_thunderduck() {
    echo -e "${BLUE}Checking thunderduck build...${NC}"

    if [ ! -f "connect-server/target/thunderduck-connect-server-1.0-SNAPSHOT.jar" ]; then
        echo -e "${YELLOW}thunderduck not built. Building now...${NC}"
        mvn clean package -DskipTests
        if [ $? -ne 0 ]; then
            echo -e "${RED}Build failed!${NC}"
            exit 1
        fi
    fi
    echo -e "${GREEN}✓ thunderduck is built${NC}"
}

# Main execution
main() {
    # Check if we're in the right directory
    if [ ! -f "pom.xml" ]; then
        echo -e "${RED}Error: Must run from thunderduck root directory${NC}"
        exit 1
    fi

    # Build thunderduck if needed
    build_thunderduck

    # Check Spark Connect
    check_spark_connect

    echo ""
    echo -e "${BLUE}====================================================================${NC}"
    echo -e "${BLUE}Running Tests${NC}"
    echo -e "${BLUE}====================================================================${NC}"

    # Prepare test command
    TEST_CMD="mvn test -pl tests"

    # Add test pattern
    if [ "$TEST_PATTERN" != "*Test" ]; then
        TEST_CMD="$TEST_CMD -Dtest=${TEST_PATTERN}"
    fi

    # Add Spark Connect URL
    TEST_CMD="$TEST_CMD -Dspark.connect.url=${SPARK_CONNECT_URL}"
    TEST_CMD="$TEST_CMD -Dspark.connect.enabled=true"

    # Add verbose flag if set
    if [ "$VERBOSE" == "true" ]; then
        TEST_CMD="$TEST_CMD -Ddifferential.verbose=true"
        TEST_CMD="$TEST_CMD -X"
    fi

    # Add memory settings
    export MAVEN_OPTS="-Xmx4g -XX:MaxPermSize=1024m"

    echo -e "${BLUE}Executing: ${TEST_CMD}${NC}"
    echo ""

    # Run tests and capture output
    TEMP_LOG=$(mktemp)
    $TEST_CMD 2>&1 | tee $TEMP_LOG

    # Check results
    if grep -q "BUILD SUCCESS" $TEMP_LOG; then
        echo ""
        echo -e "${GREEN}====================================================================${NC}"
        echo -e "${GREEN}✓ All tests passed!${NC}"
        echo -e "${GREEN}====================================================================${NC}"

        # Extract test summary
        echo ""
        echo -e "${BLUE}Test Summary:${NC}"
        grep -E "Tests run:|Failures:|Errors:|Skipped:" $TEMP_LOG | tail -1

    else
        echo ""
        echo -e "${RED}====================================================================${NC}"
        echo -e "${RED}✗ Some tests failed${NC}"
        echo -e "${RED}====================================================================${NC}"

        # Show failed tests
        echo ""
        echo -e "${RED}Failed Tests:${NC}"
        grep -E "FAILURE|ERROR" $TEMP_LOG | grep -E "test[A-Z]" | head -10

        # Show divergence report location
        echo ""
        echo -e "${YELLOW}Divergence reports saved to:${NC}"
        echo "  target/differential-reports/"
        echo ""
        echo "To view detailed report:"
        echo "  open target/differential-reports/summary.html"
    fi

    # Cleanup
    rm -f $TEMP_LOG

    echo ""
    echo -e "${BLUE}====================================================================${NC}"
    echo -e "${BLUE}Test Categories Available:${NC}"
    echo -e "${BLUE}====================================================================${NC}"
    echo ""
    echo "Run specific test categories:"
    echo "  ./tests/scripts/run-differential-tests.sh '*DataFrame*'     # DataFrame operations"
    echo "  ./tests/scripts/run-differential-tests.sh '*SQL*'           # SQL queries"
    echo "  ./tests/scripts/run-differential-tests.sh '*Type*'          # Type compatibility"
    echo "  ./tests/scripts/run-differential-tests.sh '*Window*'        # Window functions"
    echo "  ./tests/scripts/run-differential-tests.sh '*Aggregate*'     # Aggregations"
    echo "  ./tests/scripts/run-differential-tests.sh 'EndToEnd*'       # End-to-end tests"
    echo ""
    echo "Run with verbose output:"
    echo "  VERBOSE=true ./tests/scripts/run-differential-tests.sh"
    echo ""
    echo "Run against different Spark instance:"
    echo "  SPARK_CONNECT_URL=sc://spark-server:15002 ./tests/scripts/run-differential-tests.sh"
}

# Run main function
main