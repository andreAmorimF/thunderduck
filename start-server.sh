#!/bin/bash
# Start Spark Connect Server with required JVM args for ARM64 compatibility

export MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

echo "Starting Spark Connect Server..."
echo "Note: This script includes JVM args required for Apache Arrow on ARM64 platforms"
echo ""

mvn exec:java -pl connect-server \
    -Dexec.mainClass="com.thunderduck.connect.server.SparkConnectServer"
