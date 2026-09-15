package ignetdiag.Commons

import std.collection.*
import std.convert.*
import std.net.*
import std.time.*

/// Statistics over latency samples (milliseconds).
public class RttStats {
    public let sent: Int64
    public let received: Int64
    public let minMs: Float64
    public let avgMs: Float64
    public let maxMs: Float64
    /// mean absolute deviation of the received samples (jitter estimate)
    public let mdevMs: Float64

    public init(sent!: Int64, received!: Int64, samples!: ArrayList<Float64>) {
        this.sent = sent
        this.received = received
        if (samples.size == 0) {
            this.minMs = 0.0
            this.avgMs = 0.0
            this.maxMs = 0.0
            this.mdevMs = 0.0
            return
        }
        var sum: Float64 = 0.0
        var lo = samples[0]
        var hi = samples[0]
        for (v in samples) {
            sum += v
            if (v < lo) { lo = v }
            if (v > hi) { hi = v }
        }
        let avg = sum / Float64(samples.size)
        var devSum: Float64 = 0.0
        for (v in samples) {
            let d = v - avg
            devSum += if (d < 0.0) { -d } else { d }
        }
        this.minMs = lo
        this.avgMs = avg
        this.maxMs = hi
        this.mdevMs = devSum / Float64(samples.size)
    }

    public func lossPct(): Float64 {
        if (sent == 0) { return 100.0 }
        100.0 - 100.0 * Float64(received) / Float64(sent)
    }

    public func renderJson(): String {
        "{\"sent\":${sent},\"received\":${received},\"loss_pct\":${lossPct()}," +
            "\"min_ms\":${minMs},\"avg_ms\":${avgMs}," +
            "\"max_ms\":${maxMs},\"mdev_ms\":${mdevMs}}"
    }
}

/// Args helpers for the CLI (production posture: usage errors are exit 2
/// with the usage text, never silent defaults).
public func usageText(): String {
    "usage:\n" +
    "  ignetdiag ping -h <host> -p <port> [-c count] [-i intervalMs] [-W timeoutMs] [--json]\n" +
    "  ignetdiag udpping -h <host> -p <port> [-c count] [-i intervalMs] [-W timeoutMs] [--json]\n" +
    "  ignetdiag echo -p <port>\n" +
    "  ignetdiag iperf-server -p <port> [-t seconds]\n" +
    "  ignetdiag iperf-client -h <host> -p <port> [-t seconds]\n" +
    "  ignetdiag scan -h <host> [-r first-last] [-c concurrency] [-W timeoutMs]\n" +
    "  ignetdiag trace -h <host> [-p port] [-m maxHops]\n" +
    "exit codes: 0 ok, 1 probe failures, 2 usage"
}

public const EXIT_OK: Int64 = 0
public const EXIT_FAIL: Int64 = 1
public const EXIT_USAGE: Int64 = 2

public func parseIntSafe(text: String): ?Int64 {
    let t = text.trimAscii()
    try {
        Some(Int64.parse(t))
    } catch (_: Exception) {
        None
    }
}

/// Free loopback port (bind 0, read, release — small race, acceptable).
public func pickFreePort(): UInt16 {
    let server = TcpServerSocket(bindAt: UInt16(0))
    var picked: UInt16 = 0
    server.bind()
    match (server.localAddress) {
        case a: IPSocketAddress => picked = a.port
        case _ => picked = 0
    }
    server.close()
    picked
}
