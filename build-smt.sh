#!/bin/bash
set -e
echo "=== Chuan bi compile ==="
CP=$(find /usr/share/java/kafka -name "*.jar" | tr "\n" ":")
CP="${CP}$(find /usr/share/java/custom-smt -name "*.jar" | tr "\n" ":")"
echo "Classpath ready"

echo "=== Compile CustomCDCTransform.java ==="
mkdir -p /tmp/smt-build
javac -cp "$CP" \
  /tmp/custom-smt/src/main/java/com/example/kafka/connect/smt/CustomCDCTransform.java \
  -d /tmp/smt-build
echo "Compile OK!"

echo "=== Tao JAR ==="
jar cf /tmp/custom-cdc-transform-new.jar -C /tmp/smt-build .
ls -lh /tmp/custom-cdc-transform-new.jar
echo "JAR OK!"
