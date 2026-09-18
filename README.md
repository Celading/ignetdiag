# ignetdiag

A network diagnostics toolkit in pure Cangjie standard library: paced UDP throughput pairs with RFC 1889 jitter and loss accounting, TCP throughput, connects bounded by caller budgets, host listing, and trace utilities.

## Scope

- UDP iperf-style pairs: fixed-rate paced sending (never self-ramping), server-side loss/jitter accumulation, DONE-marker summary exchange.
- RFC 1889 A.8 single-ended jitter in integer microseconds.
- Honest failure staging: a dead endpoint returns bounded client-side statistics with `serverReplied = false` rather than hanging.

This is a measurement library, not a continuous monitor.

## Requirements and build

Verified with Cangjie/CJPM 1.1.3 on macOS arm64. From the source repository root:

```sh
cjpm build -j1
./target/release/bin/ignetdiag_test
```

## Use from a separate project

Keep the package beside your consumer and declare `ignetdiag = { path = "../ignetdiag-0.1.0" }`. Consumer `src/main.cj`:

```cangjie
package netdiag_example

import ignetdiag.Features.iperf.*

main(): Int64 {
    let acc = UdpIperfAccumulator()
    acc.feed(seq: 1, sendUs: 0, arrivalUs: 1000, bytes: 512)
    acc.feed(seq: 3, sendUs: 2000, arrivalUs: 3000, bytes: 512)
    if (acc.maxSeq != 3 || acc.lostPackets() != 2) { return 1 }
    println("received=${acc.receivedPackets} lost=${acc.lostPackets()} jitter_us=${acc.jitterUs}")
    0
}
```

Expected output (jitter depends on the fed arrivals):

```text
received=2 lost=2 jitter_us=0
```

## Errors and limits

Sequence gaps estimate loss as `(maxSeq + 1) - received` floored at zero; reordered or duplicated arrivals never produce negative loss. iperf3 interoperability is a distinct wire protocol (JSON control channel, specific test phases) that this package does not implement; it is a declared residual independent of tool availability — the package speaks its own iperf-style UDP protocol, not the iperf3 protocol. Installing iperf3 alone does not make the protocols interoperable.

No raw ICMP ping or per-hop ICMP address discovery is implemented. The CLI is a separate executable; this library example does not validate a throughput benchmark.

## Source preview layout

The repository contains the library at `ignetdiag-0.1.0/` and executable projects `ignetdiag_cli/`, `ignetdiag_demo/`, `ignetdiag_test/`. The versioned directory name, where present, is retained for path compatibility; this source update is not a new registry release.

Run the documented build and tests from the repository root. A standalone extracted library package contains only the library: run `cjpm build -j1` there, then use the separate-project example above. It does not contain the repository test executables.

The suite may create capture files or local test data in its working directory. Run each checkout serially in a disposable directory. Public-network tests and long-running soak modes are not enabled by default. A successful unit run does not establish full interoperability or production certification.

## License

See [LICENSE](LICENSE) and [NOTICE](NOTICE). Third-party notices, when present, remain separately applicable.
