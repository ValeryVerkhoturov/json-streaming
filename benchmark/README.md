# json-streaming benchmark

## Running

```sh
cd benchmark
./bench.sh
```

The script builds the binary, sweeps a size grid
(`rows ∈ {200, 400, ..., 102400}`, `n=6`).

## Flags

- `-func` — required, benchmark to run.
- `-rows` — number of rows in the array.
- `-n` — string field length (writer benches only).
- `-buf` — flush buffer size in **megabytes** for streaming benches (`StreamWriter`, `StreamReader`, `MarshalStream`). Default `1`. Sets both the file-side `bufio` buffer and the library's internal flush buffer (`WithBufferSize` / `WithMarshalerBufferSize`).

Example: `./jsonstreamingbenchmark -func=StreamWriter -rows=102400 -n=6 -buf=4` uses a 4 MiB flush buffer.

## Reader/writer pairing

`StreamReader` reads `StreamWriter_r{rows}.json`, and `Unmarshal` reads
`Marshal_r{rows}.json`. Run the writer benchmark at a given size before
running its matching reader.
