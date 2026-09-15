package ignetdiag.Features.trace

import std.collection.*
import std.net.*
import std.time.*

/// ---- TTL walk (traceroute variant) ----
///
/// std.net gives TCP+UDP but no raw/ICMP sockets, so classic per-hop
/// ADDRESSES are out of reach. What IS observable per probe TTL on a
/// connected UDP socket:
///   - send/recv ERRORS as the ICMP-derived condition surfaces
///     (time-exceeded / port-unreachable family), and
///   - SUCCESS when the TTL is deep enough for the datagram to arrive at
///     a listening target.
/// The walker reports, per TTL, the condition class and the time it took
/// — giving hop count + per-hop timing. Hop ADDRESSES stay a recorded
/// provider gap.

public enum ProbeOutcome {
    | Delivered(Float64)          // got a reply from the target
    | HopError(Float64, String)   // ICMP-derived error surfaced (message)
    | Timeout                     // no error, no reply within the wait
}

public struct HopResult {
    public let ttl: Int64
    public let outcome: ProbeOutcome

    public init(ttl!: Int64, outcome!: ProbeOutcome) {
        this.ttl = ttl
        this.outcome = outcome
    }

    public func describe(): String {
        match (outcome) {
            case Delivered(ms) => "${ttl}  delivered ${ms} ms"
            case HopError(ms, msg) => "${ttl}  error after ${ms} ms: ${msg}"
            case Timeout => "${ttl}  * (no answer within wait)"
        }
    }
}

/// Set TTL via the raw socket-option seam and send one UDP probe.
func probeWithTtl(sock: UdpSocket, ttl: Int64, payload: Array<Byte>,
                  waitMs: Int64): ProbeOutcome {
    sock.setSocketOptionIntNative(0, OptionName.IP_TTL, IntNative(ttl))
    let t0 = MonoTime.now()
    var sentOk = false
    var sendErr: ?Exception = None
    try {
        sock.send(payload)
        sentOk = true
    } catch (e: Exception) {
        sendErr = Some(e)
    }
    if (!sentOk) {
        let ms = Float64((MonoTime.now() - t0) / Duration.millisecond)
        let msg = match (sendErr) {
            case Some(e) => e.message
            case None => "send failed"
        }
        return HopError(ms, msg)
    }
    // wait for either a datagram (delivered path) or an error (hop)
    let buf = Array<Byte>(2048, repeat: 0)
    let deadline = MonoTime.now() + Duration.millisecond * waitMs
    while (MonoTime.now() < deadline) {
        try {
            let n = sock.receiveFrom(buf)[1]
            if (n > 0) {
                let ms = Float64((MonoTime.now() - t0) / Duration.millisecond)
                return Delivered(ms)
            }
        } catch (e: Exception) {
            let ms = Float64((MonoTime.now() - t0) / Duration.millisecond)
            return HopError(ms, e.message)
        }
    }
    Timeout
}

/// Walk TTL 1..maxHops toward host:port. `targetReplies`: when true the
/// walker also treats a delivered datagram as the walk's end condition.
public func ttlWalk(host: String, port: UInt16, maxHops!: Int64 = 16,
                    probesPerHop!: Int64 = 2, waitMs!: Int64 = 700,
                    payloadSize!: Int64 = 24): ArrayList<HopResult> {
    let sock = UdpSocket(bindAt: 0)
    sock.bind()
    sock.receiveTimeout = Duration.millisecond * waitMs
    sock.connect(IPSocketAddress.parse("${host}:${port}"))

    let out = ArrayList<HopResult>()
    var ttl: Int64 = 1
    var reached = false
    while (ttl <= maxHops && !reached) {
        var last = ProbeOutcome.Timeout
        var p: Int64 = 0
        while (p < probesPerHop) {
            let payload = ArrayList<Byte>()
            let magic = "IGTR".toArray()
            for (b in magic) { payload.add(b) }
            let tt = ttl
            payload.add(UInt8(tt & 0xFF))
            var k: Int64 = 1
            while (k < payloadSize) { payload.add(UInt8(65 + (k % 26))); k += 1 }
            match (probeWithTtl(sock, ttl, payload.toArray(), waitMs)) {
                case Delivered(ms) =>
                    last = Delivered(ms)
                    reached = true
                case HopError(ms, msg) =>
                    last = HopError(ms, msg)
                case Timeout =>
                    last = Timeout
            }
            p += 1
        }
        out.add(HopResult(ttl: ttl, outcome: last))
        ttl += 1
    }
    sock.close()
    out
}
