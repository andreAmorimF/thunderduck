#!/bin/bash

# Script to start Apache Spark Connect server in local mode for differential testing
# This allows testing thunderduck against a real Spark instance

SPARK_VERSION=3.5.3
SCALA_VERSION=2.12

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}====================================================================${NC}"
echo -e "${BLUE}Starting Apache Spark Connect Server for Differential Testing${NC}"
echo -e "${BLUE}====================================================================${NC}"

# Check if Spark is installed
if [ -z "$SPARK_HOME" ]; then
    echo -e "${YELLOW}SPARK_HOME not set. Looking for Spark installation...${NC}"

    # Try common locations
    if [ -d "/opt/spark" ]; then
        export SPARK_HOME="/opt/spark"
    elif [ -d "$HOME/spark" ]; then
        export SPARK_HOME="$HOME/spark"
    elif [ -d "/usr/local/spark" ]; then
        export SPARK_HOME="/usr/local/spark"
    else
        echo -e "${RED}ERROR: Spark not found. Please install Spark ${SPARK_VERSION} first.${NC}"
        echo ""
        echo "To install Spark:"
        echo "1. Download from: https://spark.apache.org/downloads.html"
        echo "2. Extract to /opt/spark or $HOME/spark"
        echo "3. Set SPARK_HOME environment variable"
        echo ""
        echo "Quick installation:"
        echo "  wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3-scala${SCALA_VERSION}.tgz"
        echo "  tar -xzf spark-${SPARK_VERSION}-bin-hadoop3-scala${SCALA_VERSION}.tgz"
        echo "  sudo mv spark-${SPARK_VERSION}-bin-hadoop3-scala${SCALA_VERSION} /opt/spark"
        echo "  export SPARK_HOME=/opt/spark"
        exit 1
    fi
fi

echo -e "${GREEN}Using Spark at: $SPARK_HOME${NC}"

# Check Spark version
INSTALLED_VERSION=$($SPARK_HOME/bin/spark-submit --version 2>&1 | grep "version" | head -1 | awk '{print $NF}')
echo -e "${GREEN}Spark version: $INSTALLED_VERSION${NC}"

# Check if Spark Connect is available
if [ ! -f "$SPARK_HOME/jars/spark-connect_${SCALA_VERSION}-${SPARK_VERSION}.jar" ]; then
    echo -e "${YELLOW}WARNING: spark-connect jar not found. Spark Connect might not be available.${NC}"
    echo "Make sure you have Spark ${SPARK_VERSION} with Spark Connect support."
fi

# Set up environment
export SPARK_LOCAL_IP=127.0.0.1

# Start Spark Connect server
echo ""
echo -e "${BLUE}Starting Spark Connect server on port 15002...${NC}"
echo -e "${BLUE}====================================================================${NC}"

# Create a temporary directory for Spark logs
SPARK_LOG_DIR="/tmp/spark-connect-logs"
mkdir -p $SPARK_LOG_DIR

# Start the Spark Connect server
$SPARK_HOME/sbin/start-connect-server.sh \
    --master "local[*]" \
    --conf spark.driver.host=localhost \
    --conf spark.driver.bindAddress=127.0.0.1 \
    --conf spark.connect.grpc.binding.port=15002 \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.sql.adaptive.enabled=false \
    --conf spark.ui.enabled=true \
    --conf spark.ui.port=4040 \
    --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file://$SPARK_HOME/conf/log4j.properties" \
    --packages org.apache.spark:spark-connect_${SCALA_VERSION}:${SPARK_VERSION}

# Wait for server to start
echo ""
echo -e "${BLUE}Waiting for Spark Connect server to start...${NC}"
sleep 5

# Check if server is running
if pgrep -f "org.apache.spark.sql.connect.service.SparkConnectServer" > /dev/null; then
    echo -e "${GREEN}✓ Spark Connect server is running${NC}"
    echo ""
    echo -e "${GREEN}Server Details:${NC}"
    echo "  - Connect URL: sc://localhost:15002"
    echo "  - Spark UI: http://localhost:4040"
    echo "  - Master: local[*]"
    echo ""
    echo -e "${BLUE}To test the connection:${NC}"
    echo "  python3 -c \"from pyspark.sql import SparkSession; spark = SparkSession.builder.remote('sc://localhost:15002').getOrCreate(); print('Connected to Spark', spark.version)\""
    echo ""
    echo -e "${BLUE}To run differential tests:${NC}"
    echo "  mvn test -pl tests -Dtest=DifferentialTestSuite -Dspark.connect.url=sc://localhost:15002"
    echo ""
    echo -e "${YELLOW}To stop the server:${NC}"
    echo "  $SPARK_HOME/sbin/stop-connect-server.sh"
    echo "  OR"
    echo "  ./tests/scripts/stop-spark-connect.sh"
else
    echo -e "${RED}✗ Failed to start Spark Connect server${NC}"
    echo "Check logs at: $SPARK_LOG_DIR"
    exit 1
fi

echo -e "${BLUE}====================================================================${NC}"