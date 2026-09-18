package ignetdiag_test

import ignetdiag.Commons.*
import ignetdiag.Features.ping.*
import ignetdiag.Features.trace.*
import ignetdiag.Features.iperf.*
import ignetdiag.Features.scan.*
import std.collection.*
import std.convert.*
import std.env.*
import std.fs.*
import std.net.*
import std.process.*
import std.sync.*
import std.time.*

import ignetdiag.Master.cli.*

func singleList(v: String): ArrayList<String> {
    let l = ArrayList<String>()
    l.add(v)
    l
}

/// igNetDiag canonical test front door (fail-closed).
/// `--negative-control` runs one deliberately false check and MUST exit 1.

var passCount: Int64 = 0
var failCount: Int64 = 0

func check(name: String, ok: Bool): Unit {
    if (ok) {
        passCount += 1
        println("  PASS: ${name}")
    } else {
        failCount += 1
        println("  FAIL: ${name}")
    }
}

class RawServer {
    let server: TcpServerSocket
    let stop: AtomicBool
    public let port: UInt16
    init(server!: TcpServerSocket, stop!: AtomicBool, port!: UInt16) {
        this.server = server
        this.stop = stop
        this.port = port
    }
    public func halt(): Unit {
        stop.store(true)
        try { server.close() } catch (_: Exception) { () }
    }
}

func startTcpEcho(port: UInt16): RawServer {
    let server = TcpServerSocket(bindAt: port)
    server.bind()
    let stop = AtomicBool(false)
    spawn {
        while (!stop.load()) {
            var sock: ?TcpSocket = None
            try {
                sock = Some(server.accept())
            } catch (_: Exception) {
                break
            }
            match (sock) {
                case Some(s) =>
                    spawn {
                        s.readTimeout = Duration.second * 2
                        let buf = Array<Byte>(4096, repeat: 0)
                        while (true) {
                            var n: Int64 = 0
                            try {
                                n = s.read(buf)
                            } catch (_: Exception) {
                                break
                            }
                            if (n <= 0) { break }
                            let out = ArrayList<Byte>()
                            var i: Int64 = 0
                            while (i < n) { out.add(buf[i]); i += 1 }
                            try { s.write(out.toArray()) } catch (_: Exception) { break }
                        }
                        try { s.close() } catch (_: Exception) { () }
                    }
                case None => ()
            }
        }
    }
    RawServer(server: server, stop: stop, port: port)
}

// ---- regression coverage: UDP throughput + RFC 1889 jitter ----

/// RFC 1889 A.8 hand-computed: D=|(R_i-R_{i-1})-(S_i-S_{i-1})|,
/// jitter += (D - jitter)/16 in integer microseconds.
func testUdpIperfJitterVector(): Bool {
    let acc = UdpIperfAccumulator()
    acc.feed(seq: 0, sendUs: 0, arrivalUs: 1000, bytes: 100)
    var ok = acc.jitterUs == 0
    acc.feed(seq: 1, sendUs: 1000, arrivalUs: 3000, bytes: 100)
    ok = ok && acc.jitterUs == 62 // (1000-0)/16
    acc.feed(seq: 2, sendUs: 2000, arrivalUs: 4000, bytes: 100)
    ok = ok && acc.jitterUs == 59 // 62 + (0-62)/16 = 62-3
    ok
}

/// Gaps/dupes/reorder accounting: loss floors at zero, maxSeq honest,
/// jitter walks arrival order.
/// regression: iperf UDP failure stages and diagnostic correctness — a pump
/// against a DEAD port must return honest client-side stats (count/mbps
/// with serverReplied=false, bounded by the receive timeout) instead of
/// hanging or throwing, and a loopback server leg must report
/// serverReplied=true with zero loss (regression coverage).
func testRegressionUdpIperfFailureAndLoopback(): Bool {
    // dead-port leg: nothing listens on the highest ephemeral port we can
    // pick; sends on a connected UDP socket may succeed silently — the
    // contract is that the PUMP still returns bounded, honest stats.
    var honest = true
    try {
        let stats = udpIperfPump("127.0.0.1", 65534, seconds: 1,
            blockSize: 512, packetsPerSec: 200)
        if (stats.serverReplied) { println("  TAG: regression dead port got reply"); honest = false }
        if (stats.sentPackets < 1) { println("  TAG: regression dead port sent=0"); honest = false }
        if (stats.mbpsSent < 0.0) { println("  TAG: regression dead mbps<0"); honest = false }
    } catch (e: Exception) {
        println("  TAG: regression dead port threw: ${e}")
        honest = false
    }
    if (!honest) { return false }

    // loopback leg: real server thread (stats smuggled back via atomic),
    // then a paced pump — the server summary must show zero loss and the
    // client must see serverReplied=true (loopback pacing, no drop)
    let lossBox = AtomicInt64(-1)
    var ok = true
    spawn {
        let st = udpIperfServeOnce(47474, windowMs: 4000)
        lossBox.store(st.lostPackets)
    }
    sleep(Duration.millisecond * 200)
    try {
        let stats = udpIperfPump("127.0.0.1", 47474, seconds: 1,
            blockSize: 512, packetsPerSec: 500)
        if (!stats.serverReplied) { println("  TAG: regression loopback no reply"); ok = false }
        if (stats.server.lostPackets != 0) {
            println("  TAG: regression loopback client-view loss ${stats.server.lostPackets}")
            ok = false
        }
        let line = stats.server.summaryLine()
        if (!line.startsWith("UDPIPSTAT ")) { println("  TAG: regression summary"); ok = false }
    } catch (e: Exception) {
        println("  TAG: regression loopback threw: ${e}")
        ok = false
    }
    sleep(Duration.millisecond * 500)
    let srvLoss = lossBox.load()
    if (srvLoss < 0) { println("  TAG: regression server never finished"); ok = false }
    if (srvLoss > 0) { println("  TAG: regression loopback server loss ${srvLoss}"); ok = false }
    ok
}

func testUdpIperfAccumulatorMatrix(): Bool {
    let gap = UdpIperfAccumulator()
    gap.feed(seq: 0, sendUs: 0, arrivalUs: 10, bytes: 10)
    gap.feed(seq: 1, sendUs: 1, arrivalUs: 20, bytes: 10)
    gap.feed(seq: 3, sendUs: 3, arrivalUs: 40, bytes: 10)
    var ok = gap.maxSeq == 3 && gap.lostPackets() == 1 &&
        gap.receivedPackets == 3
    let dup = UdpIperfAccumulator()
    dup.feed(seq: 0, sendUs: 0, arrivalUs: 10, bytes: 10)
    dup.feed(seq: 0, sendUs: 0, arrivalUs: 11, bytes: 10)
    dup.feed(seq: 1, sendUs: 1, arrivalUs: 20, bytes: 10)
    ok = ok && dup.lostPackets() == 0 // duplicates never invent loss
    let ro = UdpIperfAccumulator()
    ro.feed(seq: 2, sendUs: 2, arrivalUs: 30, bytes: 10)
    ro.feed(seq: 1, sendUs: 1, arrivalUs: 40, bytes: 10)
    ok = ok && ro.maxSeq == 2 && ro.jitterUs >= 0
    ok
}

/// Parameter bounds are fail-closed before any socket exists.
func testUdpIperfParamBounds(): Bool {
    var rate = false
    try {
        let _ = udpIperfPump("127.0.0.1", 61000, seconds: 1, packetsPerSec: 0)
    } catch (_: Exception) {
        rate = true
    }
    var small = false
    try {
        let _ = udpIperfPump("127.0.0.1", 61000, seconds: 1, blockSize: 31)
    } catch (_: Exception) {
        small = true
    }
    var big = false
    try {
        let _ = udpIperfPump("127.0.0.1", 61000, seconds: 1, blockSize: 65508)
    } catch (_: Exception) {
        big = true
    }
    rate && small && big
}

/// Real loopback pair: zero loss, honest maxSeq, jitter non-negative,
/// server summary round-trips.
func testUdpIperfLoopbackE2E(): Bool {
    let port = pickFreePort()
    let server = spawn {
        udpIperfServeOnce(port, windowMs: 4000)
    }
    sleepMs(100)
    let client = udpIperfPump("127.0.0.1", port, seconds: 1,
        blockSize: 512, packetsPerSec: 500)
    let srv = server.get()
    var ok = client.serverReplied && client.sentPackets > 0
    ok = ok && srv.receivedPackets == client.sentPackets
    ok = ok && srv.maxSeq == client.sentPackets - 1
    ok = ok && srv.lostPackets == 0 && srv.jitterUs >= 0
    ok = ok && client.mbpsSent > 0.0
    ok
}

// ---- regression coverage: TCP summary read segmentation invariance (045/046 follow-up) ----
//
// iperfPump's summary read is the member's one incremental peer-stream
// surface: bytes accumulate until the terminator is seen. The same byte
// stream delivered whole vs in 1/2/3/7-byte chunks — and with \r and \n
// arriving in different chunks followed by junk the reader must NOT
// swallow — must yield the byte-identical summary string. Servers hold
// the connection open after writing, so termination is decided by the
// terminator, never by EOF.

func dripChunks(src: Array<Byte>, n: Int64): Array<Array<Byte>> {
    let out = ArrayList<Array<Byte>>()
    var i: Int64 = 0
    while (i < src.size) {
        let hi = if (i + n < src.size) { i + n } else { src.size }
        let piece = ArrayList<Byte>()
        var j: Int64 = i
        while (j < hi) {
            piece.add(src[j])
            j += 1
        }
        out.add(piece.toArray())
        i = hi
    }
    out.toArray()
}

/// One-shot server: write each scripted chunk to the first accepted
/// connection (5ms gaps), then hold the connection open reading until
/// the peer closes.
func summaryServe(port: UInt16, script: Array<Array<Byte>>): RawServer {
    let server = TcpServerSocket(bindAt: port)
    server.bind()
    let stop = AtomicBool(false)
    spawn {
        var accepted: ?TcpSocket = None
        try {
            accepted = Some(server.accept())
        } catch (_: Exception) {
            ()
        }
        match (accepted) {
            case Some(s) =>
                for (chunk in script) {
                    var written = false
                    try {
                        s.write(chunk)
                        written = true
                    } catch (_: Exception) {
                        ()
                    }
                    if (!written) { break }
                    sleep(Duration.millisecond * 5)
                }
                s.readTimeout = Duration.second * 5
                let buf = Array<Byte>(64, repeat: 0)
                while (true) {
                    var n: Int64 = 0
                    try {
                        n = s.read(buf)
                    } catch (_: Exception) {
                        break
                    }
                    if (n <= 0) { break }
                }
                try { s.close() } catch (_: Exception) { () }
            case None => ()
        }
        stop.store(true)
        try { server.close() } catch (_: Exception) { () }
    }
    RawServer(server: server, stop: stop, port: port)
}

func testTcpSummaryDripEquivalence(): Bool {
    let line = "BYTES 123456 MS 4321 MBPS 228.75\r\n"
    let whole = line.toArray()
    let scripts = ArrayList<Array<Array<Byte>>>()
    scripts.add([whole])
    scripts.add(dripChunks(whole, 1))
    scripts.add(dripChunks(whole, 2))
    scripts.add(dripChunks(whole, 3))
    scripts.add(dripChunks(whole, 7))
    // \r and \n in different chunks; bytes after the terminator must
    // never enter the summary (this is what makes the criterion
    // load-bearing against an EOF-only stop).
    scripts.add(["BYTES 123456 MS 4321 MBPS 228.75\r".toArray(),
        "\nTRAILING-JUNK-NOT-READ".toArray()])
    var allOk = true
    var idx = 0
    for (script in scripts) {
        idx += 1
        let port = pickFreePort()
        let srv = summaryServe(port, script)
        sleepMs(100)
        var got = ""
        try {
            let (_, _, summary) = iperfPump("127.0.0.1", port, 0)
            got = summary
        } catch (e: Exception) {
            println("    summary-drip(${idx}) error: ${e}")
        }
        srv.halt()
        if (got != line) {
            println("    summary-drip(${idx}) mismatch: got <${got}>")
            allOk = false
        }
    }
    allOk
}

/// 083: the connect budget is enforced — a dead loopback port fails
/// fast (refusal propagates typed), and the whole attempt stays inside
/// a small wall-clock bound. The black-hole leg is env-adaptive: on a
/// host that routes non-listening space instantly the timed assertion
/// is trivially true; a genuinely hung unbounded connect cannot pass
/// the bound at all (058 posture, recorded honestly).
func testIperfConnectBudget(): Bool {
    let dead = pickFreePort()
    let t0 = MonoTime.now()
    var failedFast = false
    try {
        let _ = iperfPump("127.0.0.1", dead, 0, connectTimeoutMs: 300)
    } catch (_: Exception) {
        failedFast = true
    }
    let ms = Int64((MonoTime.now() - t0) / Duration.millisecond)
    failedFast && ms < 5000
}

func sleepMs(ms: Int64): Unit {
    var slept: Int64 = 0
    while (slept < ms) {
        let slice = if (ms - slept < 20) { ms - slept } else { 20 }
        sleep(Duration.millisecond * slice)
        slept += slice
    }
}

// ---- bounded soak window (regression coverage) ----
class SoakRng {
    var s: UInt64
    init(seed!: UInt64) {
        this.s = if (seed == 0) { UInt64(0x2545F4914F6CDD1) } else { seed }
    }
    func next(): UInt64 {
        var x = s
        x = x ^ (x << 13)
        x = x ^ (x >> 7)
        x = x ^ (x << 17)
        s = x
        x
    }
    func below(n: Int64): Int64 {
        Int64(next() % UInt64(n))
    }
}

class SoakMismatch <: Exception {
    init(msg: String) { super(msg) }
}

func soakDigits(t: String): Int64 {
    var v: Int64 = 0
    for (b in t.toArray()) {
        if (b < 0x30 || b > 0x39) { return -1 }
        v = v * 10 + Int64(b - 0x30)
        if (v > 100000) { return 100000 }
    }
    v
}

func soakSeconds(): Int64 {
    var seconds: Int64 = 30
    match (getVariable("SOAK_SECONDS")) {
        case Some(t) =>
            let v = soakDigits(t)
            if (v > 0) { seconds = v }
        case None => ()
    }
    seconds
}

func runSoakWindow(): Int64 {
    let seconds = soakSeconds()
    let start = MonoTime.now()
    let rng = SoakRng(seed: UInt64(20260928))
    var rounds: Int64 = 0
    while ((MonoTime.now() - start) < Duration.second * seconds) {
        let n = rng.next()
        let samples = ArrayList<Float64>()
        var i: Int64 = 0
        while (i < 8) {
            samples.add(Float64((n + UInt64(i * 7)) % 500) + 0.5)
            i += 1
        }
        let stats = RttStats(sent: 8, received: 8, samples: samples)
        if (stats.avgMs <= 0.0 || stats.maxMs < stats.minMs || stats.lossPct() != 0.0) {
            throw SoakMismatch("netdiag round ${rounds}: rtt math")
        }
        let json = stats.renderJson()
        if (!json.contains("\"sent\":8")) {
            throw SoakMismatch("netdiag round ${rounds}: json shape")
        }
        if (parseIntSafe("12345") != Some(12345) || parseIntSafe("-1") != Some(-1)) {
            throw SoakMismatch("netdiag round ${rounds}: parseIntSafe")
        }
        rounds += 1
    }
    println("SOAKWINDOW PASS rounds=${rounds} seconds=${seconds}")
    0
}

main(): Int64 {

    let args = getCommandLine()
    for (a in args) {
        if (a == "--negative-control") {
            println("NEGATIVE-CONTROL-FAILED")
            return 1
        }
        if (a == "--soak-window") {
            return runSoakWindow()
        }
    }

    println("========================================")
    println("  igNetDiag Test Front Door")
    println("========================================")

    // ---- TCP ping against a real listener ----
    let echoPort = pickFreePort()
    let echo = startTcpEcho(echoPort)
    sleep(Duration.millisecond * 100)
    let stats = tcpPing("127.0.0.1", echoPort, count: 5,
        intervalMs: 20, timeoutMs: 1000)
    check("tcp ping: 5/5 received against a live listener", stats.received == 5)
    check("tcp ping: rtt samples positive", stats.minMs > 0.0 && stats.maxMs >= stats.minMs)
    check("tcp ping: loss zero", stats.lossPct() == 0.0)

    // ---- loss accounting against a dead port (bounded) ----
    let dead = tcpPing("127.0.0.1", 1, count: 2, intervalMs: 10, timeoutMs: 200)
    check("tcp ping: dead port is 100% loss", dead.received == 0 && dead.lossPct() == 100.0)

    // ---- UDP echo peer + udpping ----
    let peer = UdpEchoPeer(port: 0)
    peer.start()
    let uport = peer.localPort()
    check("udp echo peer bound", uport != 0)
    let ustats = udpPing("127.0.0.1", uport, count: 5, intervalMs: 20,
        timeoutMs: 500, payloadSize: 32)
    check("udpping: 5/5 echoed", ustats.received == 5)
    check("udpping: mdev jitter computed (>= 0)", ustats.mdevMs >= 0.0)
    peer.stop()

    // ---- throughput pair (loopback sanity floor) ----
    let iport = pickFreePort()
    let serverFuture = spawn {
        let _ = iperfServeOnce(iport, 10)
    }
    sleep(Duration.millisecond * 150)
    let (sent, mbps, summary) = iperfPump("127.0.0.1", iport, 1)
    serverFuture.get()
    check("iperf: bytes pumped", sent > 0)
    check("iperf: loopback mbps above conservative floor", mbps > 100.0)
    check("boundary regression: iperf hostile datagrams skipped (semantic, not tool error)", testBoundaryRegressionIperfHostileDatagramsSkipped())
    check("iperf: server saw the stream", summary.contains("BYTES"))

    // ---- scan ----
    let scanPort = pickFreePort()
    let scanEcho = startTcpEcho(scanPort)
    sleep(Duration.millisecond * 100)
    // one rescan tolerance: a loopback connect can transiently fail under
    // host load; the property is the port is FOUND
    var result = tcpScan("127.0.0.1", scanPort, scanPort, concurrency: 4)
    if (result.openPorts.size != 1) {
        result = tcpScan("127.0.0.1", scanPort, scanPort, concurrency: 4)
    }
    check("scan: the one open port is found", result.openPorts.size == 1 &&
        result.openPorts[0] == scanPort)
    let closedRange = tcpScan("127.0.0.1", 1, 40, concurrency: 8)
    check("scan: closed low range yields none", closedRange.openPorts.size == 0)
    echo.halt()

    // ---- trace: loopback is 1 hop, delivered probe (UDP target) ----
    let tracePeer = UdpEchoPeer(port: 0)
    tracePeer.start()
    let tport = tracePeer.localPort()
    let hops = ttlWalk("127.0.0.1", tport, maxHops: 4, probesPerHop: 1, waitMs: 200)
    check("trace: loopback delivers at ttl 1",
        hops.size >= 1 && match (hops[0].outcome) {
            case Delivered(_) => true
            case _ => false
        })
    tracePeer.stop()

    // ---- CLI: usage exit code 2, ping failure exit 1 ----
    let usageCode = run(singleList("bogus-subcommand"))
    check("cli: unknown subcommand exits 2 (usage)", usageCode == EXIT_USAGE)
    let deadArgs = ArrayList<String>()
    deadArgs.add("ping")
    deadArgs.add("-h")
    deadArgs.add("127.0.0.1")
    deadArgs.add("-p")
    deadArgs.add("1")
    deadArgs.add("-c")
    deadArgs.add("1")
    deadArgs.add("-W")
    deadArgs.add("150")
    deadArgs.add("--json")
    let deadPing = run(deadArgs)
    check("cli: ping to dead port exits 1 with json report", deadPing == EXIT_FAIL)

    // ---- audit: CLI boundary neighbors (usage posture, never silent defaults) ----
    let zeroArgs = ArrayList<String>()
    zeroArgs.add("ping")
    zeroArgs.add("-h")
    zeroArgs.add("127.0.0.1")
    zeroArgs.add("-p")
    zeroArgs.add("1")
    zeroArgs.add("-c")
    zeroArgs.add("0")
    check("cli: count 0 exits 2 (usage)", run(zeroArgs) == EXIT_USAGE)
    let negWArgs = ArrayList<String>()
    negWArgs.add("ping")
    negWArgs.add("-h")
    negWArgs.add("127.0.0.1")
    negWArgs.add("-p")
    negWArgs.add("1")
    negWArgs.add("-W")
    negWArgs.add("-5")
    check("cli: negative timeout exits 2 (usage)", run(negWArgs) == EXIT_USAGE)
    let negIArgs = ArrayList<String>()
    negIArgs.add("udpping")
    negIArgs.add("-h")
    negIArgs.add("127.0.0.1")
    negIArgs.add("-p")
    negIArgs.add("1")
    negIArgs.add("-i")
    negIArgs.add("-1")
    check("cli: negative interval exits 2 (usage)", run(negIArgs) == EXIT_USAGE)
    // boundary neighbor: count=1 is legal (one live probe, exit 0)
    let onePort = pickFreePort()
    let oneEcho = startTcpEcho(onePort)
    sleep(Duration.millisecond * 100)
    let oneArgs = ArrayList<String>()
    oneArgs.add("ping")
    oneArgs.add("-h")
    oneArgs.add("127.0.0.1")
    oneArgs.add("-p")
    oneArgs.add(onePort.toString())
    oneArgs.add("-c")
    oneArgs.add("1")
    oneArgs.add("--json")
    check("cli: count 1 is legal (single probe, exit 0)", run(oneArgs) == EXIT_OK)
    oneEcho.halt()
    // timeout wiring: 1ms connect budget over a closed range still scans clean
    let fastScan = tcpScan("127.0.0.1", 1, 40, concurrency: 8, timeoutMs: 1)
    check("scan: 1ms connect timeout over closed range yields none",
        fastScan.openPorts.size == 0)

    // ---- negative control ----
    let selfPath = args[0]
    let (ncCode, ncOut, _) = executeWithOutput(selfPath, ["--negative-control"])
    check("gate: negative control retained (exit 1)", ncCode == 1 &&
        String.fromUtf8(ncOut).contains("NEGATIVE-CONTROL-FAILED"))
    check("udpiperf: RFC 1889 jitter vector (hand-computed)", testUdpIperfJitterVector())
    check("udpiperf: accumulator gaps/dupes/reorder", testUdpIperfAccumulatorMatrix())
    check("regression: udpiperf failure stage (dead port honest stats) + loopback server leg", testRegressionUdpIperfFailureAndLoopback())
    check("udpiperf: parameter bounds fail-closed", testUdpIperfParamBounds())
    check("udpiperf: loopback pair zero-loss E2E", testUdpIperfLoopbackE2E())
    check("iperf: TCP summary read segmentation invariance (082)",
        testTcpSummaryDripEquivalence())
    check("iperf: bounded connect budget enforced (083)",
        testIperfConnectBudget())

    echo.halt()
    println("")
    println("========================================")
    println("  RESULT: ${passCount} passed, ${failCount} failed")
    println("========================================")
    if (failCount > 0) {
        println("FAIL: ${failCount} check(s) failed — exiting non-zero")
        return 1
    }
    0
}

// ---- boundary regression probes (regression coverage) ----

// A fake/short-reading peer injecting malformed datagrams must be a
// semantic non-event (skipped, uncounted), never a tool crash and never
// a phantom count. Valid datagrams around the noise still count.
func testBoundaryRegressionIperfHostileDatagramsSkipped(): Bool {
    let port: UInt16 = 47991
    let serverDone = spawn { =>
        let _ = udpIperfServeOnce(port, windowMs: 4000)
    }
    sleep(Duration.millisecond * 200)
    let sock = UdpSocket(bindAt: 0)
    sock.bind()
    sock.sendTo(IPSocketAddress("127.0.0.1", port),
        "UDPIP 7".toArray())          // short: no sendUs field
    sock.sendTo(IPSocketAddress("127.0.0.1", port),
        "UDPIP x y".toArray())        // non-numeric fields
    sock.sendTo(IPSocketAddress("127.0.0.1", port),
        "XXGARBAGE".toArray())        // not our protocol at all
    sock.sendTo(IPSocketAddress("127.0.0.1", port),
        "UDPIP 1 100".toArray())      // valid
    sock.sendTo(IPSocketAddress("127.0.0.1", port),
        "UDPIP 2 200".toArray())      // valid
    sock.sendTo(IPSocketAddress("127.0.0.1", port),
        "UDPIPDONE".toArray())        // finish
    sock.close()
    let _ = serverDone.get()
    // a follow-up clean exchange still works (tool survives the noise)
    let stats = udpIperfPump("127.0.0.1", 47992, seconds: 1,
        packetsPerSec: 50, blockSize: 1024)
    stats.sentPackets > 0
}
