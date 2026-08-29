#!/bin/bash
# Drives the benchmark binary across a range of row counts.
# Emits one metrics line per run to stdout.
set -e
go build -o jsonstreamingbenchmark
bin=./jsonstreamingbenchmark

# Flush buffer size in megabytes for streaming benches (StreamWriter,
# StreamReader, MarshalStream). Override on the command line, e.g. `buf=4 ./bench.sh`.
buf=${buf:-1}

# StreamWriter (streaming writer, jsonstreaming.NewStreamWriter/SetRow/Flush)
$bin -func=StreamWriter -rows=200    -n=6 -buf=$buf
$bin -func=StreamWriter -rows=400    -n=6 -buf=$buf
$bin -func=StreamWriter -rows=800    -n=6 -buf=$buf
$bin -func=StreamWriter -rows=1600   -n=6 -buf=$buf
$bin -func=StreamWriter -rows=3200   -n=6 -buf=$buf
$bin -func=StreamWriter -rows=6400   -n=6 -buf=$buf
$bin -func=StreamWriter -rows=12800  -n=6 -buf=$buf
$bin -func=StreamWriter -rows=25600  -n=6 -buf=$buf
$bin -func=StreamWriter -rows=51200  -n=6 -buf=$buf
$bin -func=StreamWriter -rows=102400 -n=6 -buf=$buf

# StreamReader (streaming reader over files produced by StreamWriter)
$bin -func=StreamReader -rows=200    -buf=$buf
$bin -func=StreamReader -rows=400    -buf=$buf
$bin -func=StreamReader -rows=800    -buf=$buf
$bin -func=StreamReader -rows=1600   -buf=$buf
$bin -func=StreamReader -rows=3200   -buf=$buf
$bin -func=StreamReader -rows=6400   -buf=$buf
$bin -func=StreamReader -rows=12800  -buf=$buf
$bin -func=StreamReader -rows=25600  -buf=$buf
$bin -func=StreamReader -rows=51200  -buf=$buf
$bin -func=StreamReader -rows=102400 -buf=$buf

# Marshal (buffered baseline: json.Marshal(full slice))
$bin -func=Marshal -rows=200    -n=6
$bin -func=Marshal -rows=400    -n=6
$bin -func=Marshal -rows=800    -n=6
$bin -func=Marshal -rows=1600   -n=6
$bin -func=Marshal -rows=3200   -n=6
$bin -func=Marshal -rows=6400   -n=6
$bin -func=Marshal -rows=12800  -n=6
$bin -func=Marshal -rows=25600  -n=6
$bin -func=Marshal -rows=51200  -n=6
$bin -func=Marshal -rows=102400 -n=6

# Unmarshal (buffered baseline reader)
$bin -func=Unmarshal -rows=200
$bin -func=Unmarshal -rows=400
$bin -func=Unmarshal -rows=800
$bin -func=Unmarshal -rows=1600
$bin -func=Unmarshal -rows=3200
$bin -func=Unmarshal -rows=6400
$bin -func=Unmarshal -rows=12800
$bin -func=Unmarshal -rows=25600
$bin -func=Unmarshal -rows=51200
$bin -func=Unmarshal -rows=102400

# MarshalStream (channel-driven wrapper)
$bin -func=MarshalStream -rows=200    -n=6 -buf=$buf
$bin -func=MarshalStream -rows=400    -n=6 -buf=$buf
$bin -func=MarshalStream -rows=800    -n=6 -buf=$buf
$bin -func=MarshalStream -rows=1600   -n=6 -buf=$buf
$bin -func=MarshalStream -rows=3200   -n=6 -buf=$buf
$bin -func=MarshalStream -rows=6400   -n=6 -buf=$buf
$bin -func=MarshalStream -rows=12800  -n=6 -buf=$buf
$bin -func=MarshalStream -rows=25600  -n=6 -buf=$buf
$bin -func=MarshalStream -rows=51200  -n=6 -buf=$buf
$bin -func=MarshalStream -rows=102400 -n=6 -buf=$buf

# Cleanup
rm -f *.json
