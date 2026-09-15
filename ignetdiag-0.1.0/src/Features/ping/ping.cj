package ignetdiag.Features.ping

import ignetdiag.Commons.*
import std.collection.*
import std.net.*
import std.sync.*
import std.time.*

/// TCP-connect ping: `count` probes with `intervalMs` spacing; each probe
/// must connect within `timeoutMs` (enforced via the bounded connect
/// overload — a filtered/dropped target fails the probe at the deadline
/// instead of blocking on the OS-level TCP timeout). Returns the full report.
public func tcpPing(host: String, port: UInt16, count!: Int64,
                    intervalMs!: Int64 = 200, timeoutMs!: Int64 = 1000): RttStats {
    let samples = ArrayList<Float64>()
    var sent: Int64 = 0
    var received: Int64 = 0
    var i: Int64 = 0
    while (i < count) {
        sent += 1
        var connected = false
        let t0 = MonoTime.now()
        try {
            let sock = TcpSocket(host, port)
            sock.connect(timeout: Duration.millisecond * timeoutMs)
            connected = true
            sock.close()
        } catch (_: Exception) {
            connected = false
        }
        if (connected) {
            received += 1
            samples.add(Float64((MonoTime.now() - t0) / Duration.millisecond))
        }
        if (i + 1 < count) {
            sleep(Duration.millisecond * intervalMs)
        }
        i += 1
    }
    RttStats(sent: sent, received: received, samples: samples)
}

/// ---- UDP echo peer + UDP ping ----

/// UDP echo peer: echoes every datagram back to its sender. Runs until
/// stopped (the stop flag is checked on the read timeout path).
public class UdpEchoPeer {
    let sock: UdpSocket
    let stopFlag: AtomicBool

    public init(port!: UInt16) {
        this.sock = UdpSocket(bindAt: port)
        this.stopFlag = AtomicBool(false)
        sock.bind()
        sock.receiveTimeout = Duration.millisecond * 200
    }

    public func localPort(): UInt16 {
        match (sock.localAddress) {
            case a: IPSocketAddress => a.port
            case _ => 0
        }
    }

    public func start(): Unit {
        let f = spawn {
            let buf = Array<Byte>(65507, repeat: 0)
            while (!stopFlag.load()) {
                var from: ?SocketAddress = None
                var n: Int64 = 0
                try {
                    let (addr, count) = sock.receiveFrom(buf)
                    from = Some(addr)
                    n = count
                } catch (_: Exception) {
                    continue
                }
                match (from) {
                    case Some(addr) =>
                        var payload = ArrayList<Byte>()
                        var i: Int64 = 0
                        while (i < n) { payload.add(buf[i]); i += 1 }
                        try {
                            sock.sendTo(addr, payload.toArray())
                        } catch (_: Exception) {
                            ()
                        }
                    case None => ()
                }
            }
        }
        let _ = f
    }

    public func stop(): Unit {
        stopFlag.store(true)
        try {
            sock.close()
        } catch (_: Exception) {
            ()
        }
    }
}

/// UDP ping against an echo peer: each probe carries a per-probe payload
/// (unique token + send timestamp are implicit in the caller's RTT book);
/// a matching echo is a received probe. Unmatched/late datagrams are
/// counted as lost probes (unique token per probe prevents cross-match).
public func udpPing(host: String, port: UInt16, count!: Int64,
                    intervalMs!: Int64 = 200, timeoutMs!: Int64 = 1000,
                    payloadSize!: Int64 = 32): RttStats {
    let sock = UdpSocket(bindAt: 0)
    sock.bind()
    sock.receiveTimeout = Duration.millisecond * timeoutMs
    sock.connect(IPSocketAddress.parse("${host}:${port}"))

    let samples = ArrayList<Float64>()
    var sent: Int64 = 0
    var received: Int64 = 0
    var i: Int64 = 0
    while (i < count) {
        sent += 1
        // payload: [magic 'IG'][seq i][pad]
        let probe = ArrayList<Byte>()
        let magic = "IGND".toArray()
        for (b in magic) { probe.add(b) }
        var s = i
        var digits: Int64 = 0
        while (digits < 8) { probe.add(UInt8(48 + (s % 10))); s = s / 10; digits += 1 }
        let pad = payloadSize - Int64(probe.size) - 2
        var k: Int64 = 0
        while (k < pad) { probe.add(UInt8(65 + (k % 26))); k += 1 }
        probe.add(UInt8(35)) // '#'
        probe.add(UInt8(35)) // '#'

        let t0 = MonoTime.now()
        var got = false
        try {
            sock.send(probe.toArray())
            let buf = Array<Byte>(65507, repeat: 0)
            let deadline = MonoTime.now() + Duration.millisecond * timeoutMs
            while (MonoTime.now() < deadline) {
                var n: Int64 = 0
                try {
                    n = sock.receiveFrom(buf)[1]
                } catch (_: Exception) {
                    break
                }
                if (n >= 12 && buf[0] == UInt8(73) && buf[1] == UInt8(71) &&
                    buf[2] == UInt8(78) && buf[3] == UInt8(68)) {
                    // verify the sequence digits echo back ours
                    var s2 = i
                    var matchOk = true
                    var d: Int64 = 0
                    while (d < 8) {
                        let want = UInt8(48 + (s2 % 10))
                        if (buf[4 + d] != want) { matchOk = false; break }
                        s2 = s2 / 10
                        d += 1
                    }
                    if (matchOk) {
                        got = true
                        break
                    }
                }
            }
        } catch (_: Exception) {
            got = false
        }
        if (got) {
            received += 1
            samples.add(Float64((MonoTime.now() - t0) / Duration.millisecond))
        }
        if (i + 1 < count) {
            sleep(Duration.millisecond * intervalMs)
        }
        i += 1
    }
    sock.close()
    RttStats(sent: sent, received: received, samples: samples)
}
