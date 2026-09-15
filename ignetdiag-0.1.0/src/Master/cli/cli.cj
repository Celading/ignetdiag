package ignetdiag.Master.cli

import ignetdiag.Commons.*
import ignetdiag.Features.ping.*
import ignetdiag.Features.trace.*
import ignetdiag.Features.iperf.*
import ignetdiag.Features.scan.*
import std.collection.*
import std.net.*
import std.sync.*
import std.time.*

/// Production CLI: subcommand dispatch, typed usage errors (exit 2),
/// deterministic report rendering, exit-code truth (0/1).

func tailArgs(argv: ArrayList<String>, from: Int64): ArrayList<String> {
    let l = ArrayList<String>()
    var i = from
    while (i < argv.size) { l.add(argv[i]); i += 1 }
    l
}

public class CliArgs {
    public let argv: ArrayList<String>

    public init(argv: ArrayList<String>) {
        this.argv = argv
    }

    public func option(names: Array<String>): ?String {
        var i: Int64 = 0
        while (i < argv.size) {
            for (nm in names) {
                if (argv[i] == nm && i + 1 < argv.size) {
                    return Some(argv[i + 1])
                }
            }
            i += 1
        }
        None
    }

    public func hasFlag(name: String): Bool {
        for (a in argv) {
            if (a == name) { return true }
        }
        false
    }
}

func parsePort(text: String): ?UInt16 {
    match (parseIntSafe(text)) {
        case Some(v) =>
            if (v >= 1 && v <= 65535) { return Some(UInt16(v)) }
            None
        case None => None
    }
}

func usageExit(): Int64 {
    println(usageText())
    EXIT_USAGE
}

public func run(argv: ArrayList<String>): Int64 {
    if (argv.size == 0) {
        println(usageText())
        return EXIT_USAGE
    }
    let sub = argv[0]
    let rest = CliArgs(tailArgs(argv, 1))

    let host = rest.option(["-h", "--host"])
    let portText = rest.option(["-p", "--port"])
    let countText = rest.option(["-c", "--count"])
    let interval = parseIntSafe(rest.option(["-i", "--interval"]) ?? "200")
    let timeout = parseIntSafe(rest.option(["-W", "--timeout"]) ?? "1000")
    let seconds = parseIntSafe(rest.option(["-t", "--time"]) ?? "5")
    let range = rest.option(["-r", "--range"]) ?? "1-1024"
    let concurrency = parseIntSafe(rest.option(["-c", "--concurrency"]) ?? "32")
    let maxHops = parseIntSafe(rest.option(["-m", "--max-hops"]) ?? "16")
    let json = rest.hasFlag("--json")
    let count = parseIntSafe(countText ?? "4")

    // validate shared numeric options once. Boundaries (neighbors 0/1 and
    // negatives are rejected, never silently defaulted): probes per run,
    // workers and hops need >= 1; interval/timeout accept >= 0.
    if (count.isNone() || count.getOrThrow() < 1 ||
        interval.isNone() || interval.getOrThrow() < 0 ||
        timeout.isNone() || timeout.getOrThrow() < 0 ||
        seconds.isNone() || seconds.getOrThrow() < 1 ||
        concurrency.isNone() || concurrency.getOrThrow() < 1 ||
        maxHops.isNone() || maxHops.getOrThrow() < 1) {
        println(usageText())
        return EXIT_USAGE
    }
    let countN = count.getOrThrow()

    match (sub) {
        case "ping" =>
            if (host.isNone() || portText.isNone()) { return usageExit() }
            let port = parsePort(portText.getOrThrow())
            match (port) {
                case Some(p) =>
                    let stats = tcpPing(host.getOrThrow(), p, count: countN,
                        intervalMs: interval.getOrThrow(),
                        timeoutMs: timeout.getOrThrow())
                    if (json) {
                        println(stats.renderJson())
                    } else {
                        println("TCP ${host.getOrThrow()}:${p} sent=${stats.sent} recv=${stats.received} loss=${stats.lossPct()}% min=${stats.minMs} avg=${stats.avgMs} max=${stats.maxMs} mdev=${stats.mdevMs}")
                    }
                    if (stats.received == 0) { return EXIT_FAIL }
                    EXIT_OK
                case None => usageExit()
            }
        case "udpping" =>
            let port = parsePort(portText ?? "")
            match (port) {
                case Some(p) =>
                    let stats = udpPing(host.getOrThrow(), p, count: countN,
                        intervalMs: interval.getOrThrow(),
                        timeoutMs: timeout.getOrThrow())
                    if (json) {
                        println(stats.renderJson())
                    } else {
                        println("UDP ${host.getOrThrow()}:${p} sent=${stats.sent} recv=${stats.received} loss=${stats.lossPct()}% min=${stats.minMs} avg=${stats.avgMs} max=${stats.maxMs} mdev=${stats.mdevMs}")
                    }
                    if (stats.received == 0) { return EXIT_FAIL }
                    EXIT_OK
                case None => usageExit()
            }
        case "echo" =>
            let port = parsePort(portText ?? "")
            match (port) {
                case Some(p) =>
                    let peer = UdpEchoPeer(port: p)
                    peer.start()
                    println("UDP echo peer on :${p}")
                    // run until stopped externally (Ctrl-C); bounded to 1h
                    sleep(Duration.second * 3600)
                    peer.stop()
                    EXIT_OK
                case None => usageExit()
            }
        case "iperf-server" =>
            let port = parsePort(portText ?? "")
            match (port) {
                case Some(p) =>
                    let total = iperfServeOnce(p, seconds.getOrThrow())
                    let ms = 1
                    println("iperf-server: received ${total} bytes")
                    EXIT_OK
                case None => usageExit()
            }
        case "iperf-client" =>
            let port = parsePort(portText ?? "")
            match (port) {
                case Some(p) =>
                    let (sent, mbps, summary) = iperfPump(host.getOrThrow(), p,
                        seconds.getOrThrow())
                    println("iperf-client: sent=${sent}B mbps=${mbps}")
                    println("iperf-server said: ${summary.trimAscii()}")
                    EXIT_OK
                case None => usageExit()
            }
        case "scan" =>
            if (host.isNone()) { return usageExit() }
            let rangeParts = range.split("-")
            if (rangeParts.size != 2) {
                println(usageText())
                return EXIT_USAGE
            }
            let fromOpt = parseIntSafe(rangeParts[0])
            let toOpt = parseIntSafe(rangeParts[1])
            match (fromOpt) {
                case Some(f) =>
                    match (toOpt) {
                        case Some(t) =>
                            if (f < 1 || t > 65535 || f > t) {
                                println(usageText())
                                return EXIT_USAGE
                            }
                            let result = tcpScan(host.getOrThrow(), UInt16(f),
                                UInt16(t), concurrency: concurrency.getOrThrow(),
                                timeoutMs: timeout.getOrThrow())
                            println(result.renderText())
                            EXIT_OK
                        case None => usageExit()
                    }
                case None => usageExit()
            }
        case "trace" =>
            if (host.isNone()) { return usageExit() }
            let port = parsePort(portText ?? "33434")
            match (port) {
                case Some(p) =>
                    let hops = ttlWalk(host.getOrThrow(), p,
                        maxHops: maxHops.getOrThrow())
                    for (h in hops) {
                        println(h.describe())
                    }
                    EXIT_OK
                case None => usageExit()
            }
        case _ =>
            println(usageText())
            EXIT_USAGE
    }
}
