#!/bin/bash
set -e
echo "=== Build classpath ==="
CP=$(find /usr/share/java/kafka -name "*.jar" | tr "\n" ":")
CP="${CP}$(find /usr/share/java/custom-smt -name "*.jar" | tr "\n" ":")"
echo "Classpath ready"

echo "=== Compile CustomCDCTransform.java ==="
rm -rf /tmp/smt-build
mkdir -p /tmp/smt-build
javac -cp "$CP" \
  /work/custom-smt/src/main/java/com/example/kafka/connect/smt/CustomCDCTransform.java \
  -d /tmp/smt-build
echo "Compile OK!"

echo "=== Build JAR -> plugins/custom-smt ==="
jar cf /work/plugins/custom-smt/custom-cdc-transform.jar -C /tmp/smt-build .
ls -lh /work/plugins/custom-smt/custom-cdc-transform.jar
echo "JAR OK!"
