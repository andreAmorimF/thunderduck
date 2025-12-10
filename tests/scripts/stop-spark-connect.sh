#!/bin/bash

# Script to stop Apache Spark Connect server

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Stopping Spark Connect Server...${NC}"

# Check if SPARK_HOME is set
if [ -z "$SPARK_HOME" ]; then
    # Try to find Spark
    if [ -d "/opt/spark" ]; then
        export SPARK_HOME="/opt/spark"
    elif [ -d "$HOME/spark" ]; then
        export SPARK_HOME="$HOME/spark"
    elif [ -d "/usr/local/spark" ]; then
        export SPARK_HOME="/usr/local/spark"
    fi
fi

if [ -n "$SPARK_HOME" ] && [ -f "$SPARK_HOME/sbin/stop-connect-server.sh" ]; then
    # Use official stop script
    $SPARK_HOME/sbin/stop-connect-server.sh
    echo -e "${GREEN}✓ Spark Connect server stopped using official script${NC}"
else
    # Fallback: Kill the process directly
    PID=$(pgrep -f "org.apache.spark.sql.connect.service.SparkConnectServer")
    if [ -n "$PID" ]; then
        kill $PID
        echo -e "${GREEN}✓ Spark Connect server (PID: $PID) stopped${NC}"
    else
        echo -e "${YELLOW}No running Spark Connect server found${NC}"
    fi
fi

# Also kill any orphaned Spark processes
echo -e "${BLUE}Cleaning up any orphaned Spark processes...${NC}"
pkill -f "spark.*SparkSubmit" 2>/dev/null
pkill -f "spark.*CoarseGrainedExecutorBackend" 2>/dev/null

echo -e "${GREEN}Done!${NC}"