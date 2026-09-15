package ignetdiag.Features.iperf

import std.collection.*
import std.net.*
import std.sync.*
import std.time.*

/// ---- iperf-style TCP throughput pair ----

/// Server side: accept ONE connection, receive blocks until the client
/// closes, count bytes, then send back a summary line "BYTES <n>".
/// Returns the received byte count.
public func iperfServeOnce(port: UInt16, maxSeconds: Int64): Int64 {
    let server = TcpServerSocket(bindAt: port)
    server.bind()
    var sock: ?TcpSocket = None
    try {
        sock = Some(server.accept())
    } catch (_: Exception) {
        server.close()
        return 0
    }
    var total: Int64 = 0
    match (sock) {
        case Some(s) =>
            s.readTimeout = Duration.millisecond * 300
            let buf = Array<Byte>(65536, repeat: 0)
            let t0 = MonoTime.now()
            while (MonoTime.now() - t0 < Duration.second * maxSeconds) {
                var n: Int64 = 0
                try {
                    n = s.read(buf)
                } catch (_: Exception) {
                    break
                }
                if (n <= 0) { break }
                total += n
            }
            let ms = (MonoTime.now() - t0) / Duration.millisecond
            let msF = Float64(ms)
            let mbps = if (msF > 0.0) { Float64(total) * 8.0 / msF / 1000.0 } else { 0.0 }
            try {
                s.write("BYTES ${total} MS ${ms} MBPS ${mbps}\r\n".toArray())
            } catch (_: Exception) {
                ()
            }
            try {
                s.close()
            } catch (_: Exception) {
                ()
            }
        case None => ()
    }
    server.close()
    total
}

/// Client side: pump blocks for `seconds`, then read the server summary.
/// Returns (bytesSent, mbpsSent, serverReportText).
public func iperfPump(host: String, port: UInt16, seconds: Int64,
                      blockKb!: Int64 = 64,
                      connectTimeoutMs!: Int64 = 3000): (Int64, Float64, String) {
    let sock = TcpSocket(host, port)
    // bounded connect: an unreachable host must fail within the caller's
    // budget, never hang the measurement client (same posture as
    // tcpPing/tcpScan since the 035 audit)
    sock.connect(timeout: Duration.millisecond * connectTimeoutMs)
    sock.writeTimeout = Duration.second * 10
    sock.readTimeout = Duration.second * 5
    let block = Array<Byte>(blockKb * 1024, repeat: UInt8(65))
    let t0 = MonoTime.now()
    var sent: Int64 = 0
    while (MonoTime.now() - t0 < Duration.second * seconds) {
        var failed = false
        try {
            sock.write(block)
            sent += Int64(block.size)
        } catch (_: Exception) {
            failed = true
        }
        if (failed) { break }
    }
    let ms = (MonoTime.now() - t0) / Duration.millisecond
    let msF = Float64(ms)
    let mbps = if (msF > 0.0) { Float64(sent) * 8.0 / msF / 1000.0 } else { 0.0 }
    // read the server summary (bounded)
    var summary = ""
    let buf = Array<Byte>(4096, repeat: 0)
    let deadline = MonoTime.now() + Duration.second * 3
    while (MonoTime.now() < deadline) {
        var n: Int64 = 0
        try {
            n = sock.read(buf)
        } catch (_: Exception) {
            break
        }
        if (n <= 0) { break }
        // Summary is exactly one terminator-terminated line: stop at the
        // first \n and never absorb bytes coalesced after it into the
        // same read — they belong to no summary field and would leak
        // into the measured report.
        let chunk = sliceOf(buf, n)
        var cut = -1
        var bi: Int64 = 0
        while (bi < chunk.size) {
            if (chunk[bi] == 0x0A) {
                cut = bi
                break
            }
            bi += 1
        }
        if (cut >= 0) {
            let head = ArrayList<Byte>()
            var ki: Int64 = 0
            while (ki <= cut) {
                head.add(chunk[ki])
                ki += 1
            }
            summary = summary + String.fromUtf8(head.toArray())
            break
        }
        summary = summary + String.fromUtf8(chunk)
    }
    sock.close()
    (sent, mbps, summary)
}

func sliceOf(buf: Array<Byte>, n: Int64): Array<Byte> {
    let out = ArrayList<Byte>()
    var i: Int64 = 0
    while (i < n) {
        out.add(buf[i])
        i += 1
    }
    out.toArray()
}


// ---- UDP throughput pair with RFC 1889 jitter ----

/// Pure statistics core: packet/byte counts, highest sequence seen,
/// single-ended jitter (RFC 1889 A.8: D = |(R_i - R_{i-1}) - (S_i -
/// S_{i-1})| in arrival order; jitter += (D - jitter) / 16, computed
/// in microseconds to keep small-jitter fidelity) and the seq-gap
/// loss estimate (clamped at zero — duplicates must not invent loss).
public class UdpIperfAccumulator {
    public var receivedPackets: Int64 = 0
    public var receivedBytes: Int64 = 0
    public var maxSeq: Int64 = -1
    public var jitterUs: Int64 = 0
    private var prevArrivalUs: Int64 = -1
    private var prevSendUs: Int64 = -1

    public func feed(seq!: Int64, sendUs!: Int64, arrivalUs!: Int64,
                     bytes!: Int64): Unit {
        receivedPackets += 1
        receivedBytes += bytes
        if (seq > maxSeq) {
            maxSeq = seq
        }
        if (prevArrivalUs >= 0) {
            let d = absInt((arrivalUs - prevArrivalUs) - (sendUs - prevSendUs))
            jitterUs = jitterUs + (d - jitterUs) / 16
        }
        prevArrivalUs = arrivalUs
        prevSendUs = sendUs
    }

    /// Estimated lost packets = (maxSeq + 1) - received, floored at 0
    /// (reordered/duplicated arrivals must not produce negative loss).
    public func lostPackets(): Int64 {
        let expected = maxSeq + 1
        if (receivedPackets >= expected) {
            return 0
        }
        return expected - receivedPackets
    }
}

func absInt(v: Int64): Int64 {
    if (v < 0) {
        return -v
    }
    v
}

public class UdpIperfServerStats {
    public let receivedPackets: Int64
    public let receivedBytes: Int64
    public let maxSeq: Int64
    public let lostPackets: Int64
    public let jitterUs: Int64
    public let windowMs: Int64

    public init(receivedPackets!: Int64, receivedBytes!: Int64, maxSeq!: Int64,
                lostPackets!: Int64, jitterUs!: Int64, windowMs!: Int64) {
        this.receivedPackets = receivedPackets
        this.receivedBytes = receivedBytes
        this.maxSeq = maxSeq
        this.lostPackets = lostPackets
        this.jitterUs = jitterUs
        this.windowMs = windowMs
    }

    public func summaryLine(): String {
        "UDPIPSTAT ${receivedPackets} ${maxSeq} ${lostPackets} ${jitterUs}"
    }
}

/// Server side of the UDP pair: receive paced UDPIP datagrams for
/// `windowMs` (or until the client's DONE marker), then reply one
/// summary datagram to the client's source address. No retransmission
/// and no rate adaptation — the client chose its rate and owns the
/// congestion decision (RFC 8085 posture for a measurement tool).
public func udpIperfServeOnce(port: UInt16, windowMs!: Int64): UdpIperfServerStats {
    let sock = UdpSocket(bindAt: port)
    sock.bind()
    sock.receiveTimeout = Duration.millisecond * 100
    let acc = UdpIperfAccumulator()
    let buf = Array<Byte>(65536, repeat: 0)
    let t0 = MonoTime.now()
    var clientAddr: ?SocketAddress = None
    while (MonoTime.now() - t0 < Duration.millisecond * windowMs) {
        var n: Int64 = 0
        var from: ?SocketAddress = None
        try {
            let (src, count) = sock.receiveFrom(buf)
            n = count
            from = src
        } catch (_: Exception) {
            continue // receive timeout tick
        }
        if (n <= 0) {
            continue
        }
        clientAddr = from
        let text = asciiHead(buf, n)
        if (text.startsWith("UDPIPDONE")) {
            break
        }
        if (text.startsWith("UDPIP ")) {
            let parts = text.split(" ")
            if (parts.size >= 3) {
                let seq = parseI64Safe(parts[1])
                let sendUs = parseI64Safe(parts[2])
                let nowUs: Int64 = Int64((MonoTime.now() - t0) / Duration.microsecond)
                acc.feed(seq: seq, sendUs: sendUs, arrivalUs: nowUs,
                    bytes: n)
            }
        }
    }
    let stats = UdpIperfServerStats(receivedPackets: acc.receivedPackets,
        receivedBytes: acc.receivedBytes, maxSeq: acc.maxSeq,
        lostPackets: acc.lostPackets(), jitterUs: acc.jitterUs,
        windowMs: windowMs)
    match (clientAddr) {
        case Some(a) =>
            try {
                sock.sendTo(a, stats.summaryLine().toArray())
            } catch (_: Exception) {
                ()
            }
        case None => ()
    }
    sock.close()
    stats
}

public class UdpIperfClientStats {
    public let sentPackets: Int64
    public let sentBytes: Int64
    public let mbpsSent: Float64
    public let server: UdpIperfServerStats
    public let serverReplied: Bool

    public init(sentPackets!: Int64, sentBytes!: Int64, mbpsSent!: Float64,
                server!: UdpIperfServerStats, serverReplied!: Bool) {
        this.sentPackets = sentPackets
        this.sentBytes = sentBytes
        this.mbpsSent = mbpsSent
        this.server = server
        this.serverReplied = serverReplied
    }
}

/// Client side: paced send at `packetsPerSec` for `seconds` (fixed
/// user-chosen rate; the tool never ramps on its own), then the DONE
/// marker, then the server summary. Datagram payload:
/// "UDPIP <seq> <sendUs>" padded to blockSize bytes.
public func udpIperfPump(host: String, port: UInt16, seconds!: Int64,
    blockSize!: Int64 = 1024, packetsPerSec!: Int64 = 1000): UdpIperfClientStats {
    if (packetsPerSec < 1) {
        throw Exception("udpiperf: packetsPerSec must be >= 1")
    }
    if (blockSize < 32 || blockSize > 65507) {
        throw Exception("udpiperf: blockSize must be 32..65507: ${blockSize}")
    }
    let sock = UdpSocket(bindAt: 0)
    sock.bind()
    sock.receiveTimeout = Duration.millisecond * 1000
    sock.connect(IPSocketAddress.parse("${host}:${port}"))
    let payload = Array<Byte>(blockSize, repeat: UInt8(85))
    let intervalUs: Int64 = 1000000 / packetsPerSec
    let t0 = MonoTime.now()
    var sent: Int64 = 0
    var seq: Int64 = 0
    while (MonoTime.now() - t0 < Duration.second * seconds) {
        let slot = t0 + Duration.microsecond * (seq * intervalUs)
        while (MonoTime.now() < slot) {
            () // pace to the schedule; never burst past the chosen rate
        }
        let sendUs: Int64 = Int64((MonoTime.now() - t0) / Duration.microsecond)
        let head = "UDPIP ${seq} ${sendUs} "
        let hb = head.toArray()
        var i: Int64 = 0
        while (i < Int64(hb.size) && i < blockSize) {
            payload[i] = hb[i]
            i += 1
        }
        var failed = false
        try {
            sock.send(payload)
        } catch (_: Exception) {
            failed = true
        }
        if (failed) {
            break
        }
        sent += 1
        seq += 1
    }
    let ms = (MonoTime.now() - t0) / Duration.millisecond
    let msF = Float64(ms)
    let mbps = if (msF > 0.0) {
        Float64(sent * blockSize) * 8.0 / msF / 1000.0
    } else {
        0.0
    }
    try {
        sock.send("UDPIPDONE".toArray())
    } catch (_: Exception) {
        ()
    }
    // server summary (single datagram; bounded wait)
    let buf = Array<Byte>(4096, repeat: 0)
    var summary = ""
    try {
        let (_, n) = sock.receiveFrom(buf)
        if (n > 0) {
            summary = asciiHead(buf, n)
        }
    } catch (_: Exception) {
        ()
    }
    sock.close()
    var sPackets: Int64 = 0
    var sMaxSeq: Int64 = -1
    var sLost: Int64 = 0
    var sJitter: Int64 = 0
    var replied = false
    if (summary.startsWith("UDPIPSTAT ")) {
        replied = true
        let parts = summary.split(" ")
        if (parts.size >= 5) {
            sPackets = parseI64Safe(parts[1])
            sMaxSeq = parseI64Safe(parts[2])
            sLost = parseI64Safe(parts[3])
            sJitter = parseI64Safe(parts[4])
        }
    }
    UdpIperfClientStats(sentPackets: sent, sentBytes: sent * blockSize,
        mbpsSent: mbps,
        server: UdpIperfServerStats(receivedPackets: sPackets,
            receivedBytes: 0, maxSeq: sMaxSeq, lostPackets: sLost,
            jitterUs: sJitter, windowMs: 0),
        serverReplied: replied)
}

func asciiHead(buf: Array<Byte>, n: Int64): String {
    let capped = if (n > 256) { 256 } else { n }
    var out = ""
    var i: Int64 = 0
    while (i < capped) {
        let b = buf[i]
        if (b >= 0x20 && b <= 0x7E) {
            out = out + String.fromUtf8([b])
        } else {
            out = out + " "
        }
        i += 1
    }
    out
}

func parseI64Safe(text: String): Int64 {
    var v: Int64 = 0
    var any = false
    for (b in text.toArray()) {
        if (b < 0x30 || b > 0x39) {
            break
        }
        v = v * 10 + Int64(b - 0x30)
        any = true
        if (v > 4611686018427387903) {
            return 4611686018427387903
        }
    }
    if (!any) {
        return 0
    }
    v
}