#!/bin/bash
set -euo pipefail

echo "=== Build classpath ==="
CP=$(find /usr/share/java/kafka -name "*.jar" | tr "\n" ":")
CP="${CP}$(find /usr/share/java/custom-smt -name "*.jar" | tr "\n" ":")"
echo "Classpath ready"

BUILD_DIR=/tmp/smt-build
JAR_DIR=/tmp/smt-jar
OUTPUT_JAR=/tmp/custom-cdc-transform.jar
RUNTIME_JAR=/work/plugins/custom-smt/custom-cdc-transform.jar

if [ ! -f "$RUNTIME_JAR" ]; then
  echo "Missing base fat JAR: $RUNTIME_JAR" >&2
  exit 1
fi

rm -rf "$BUILD_DIR" "$JAR_DIR" "$OUTPUT_JAR"
mkdir -p "$BUILD_DIR" "$JAR_DIR"

echo "=== Compile CustomCDCTransform.java ==="
javac --release 11 -proc:none -cp "$CP" \
  /work/custom-smt/src/main/java/com/example/kafka/connect/smt/CustomCDCTransform.java \
  -d "$BUILD_DIR"
echo "Compile OK!"

echo "=== Replace compiled classes while preserving bundled dependencies ==="
(
  cd "$JAR_DIR"
  jar xf "$RUNTIME_JAR"
)
cp -R "$BUILD_DIR"/. "$JAR_DIR"/
rm -f "$JAR_DIR"/META-INF/*.SF "$JAR_DIR"/META-INF/*.DSA "$JAR_DIR"/META-INF/*.RSA
jar cMf "$OUTPUT_JAR" -C "$JAR_DIR" .
cp "$OUTPUT_JAR" "$RUNTIME_JAR"
ls -lh "$RUNTIME_JAR"
jar tf "$RUNTIME_JAR" | grep -q 'com/example/kafka/connect/smt/CustomCDCTransform.class'
jar tf "$RUNTIME_JAR" | grep -q 'com/fasterxml/jackson/databind/ObjectMapper.class'
echo "Fat JAR OK!"