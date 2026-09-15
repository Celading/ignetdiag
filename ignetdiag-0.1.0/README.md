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

Keep the package beside your consumer and declare `ignetdiag = { path = "../ignetdiag" }`. Consumer `src/main.cj`:

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

Sequence gaps estimate loss as `(maxSeq + 1) - received` floored at zero; reordered or duplicated arrivals never produce negative loss. Standard-peer (iperf3) interop requires the iperf3 tool, which this package does not bundle; without it the live interop leg stays honestly skipped rather than claimed.
