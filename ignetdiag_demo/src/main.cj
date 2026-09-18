package ignetdiag_demo

import ignetdiag.Commons.*
import ignetdiag.Features.ping.*
import ignetdiag.Features.trace.*
import ignetdiag.Features.iperf.*
import ignetdiag.Features.scan.*
import std.collection.*
import std.net.*
import std.sync.*
import std.time.*

/// igNetDiag live demo: ping / udpping / throughput / scan / trace
/// against loopback. Exit 0 on success.

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

main(): Int64 {
    println("========================================")
    println("  igNetDiag Live Demo (loopback)")
    println("========================================")

    let echoPort = pickFreePort()
    let echo = startTcpEcho(echoPort)
    sleep(Duration.millisecond * 100)

    let stats = tcpPing("127.0.0.1", echoPort, count: 10,
        intervalMs: 30, timeoutMs: 1000)
    println("ping: sent=${stats.sent} recv=${stats.received} loss=${stats.lossPct()}% avg=${stats.avgMs}ms mdev=${stats.mdevMs}ms")
    check("tcp ping loss zero", stats.lossPct() == 0.0)

    let peer = UdpEchoPeer(port: 0)
    peer.start()
    let uport = peer.localPort()
    let ustats = udpPing("127.0.0.1", uport, count: 10, intervalMs: 30,
        timeoutMs: 500, payloadSize: 48)
    println("udpping: sent=${ustats.sent} recv=${ustats.received} loss=${ustats.lossPct()}% avg=${ustats.avgMs}ms mdev=${ustats.mdevMs}ms")
    check("udp ping loss zero", ustats.lossPct() == 0.0)

    let iport = pickFreePort()
    let serverFuture = spawn {
        let _ = iperfServeOnce(iport, 10)
    }
    sleep(Duration.millisecond * 150)
    let (sent, mbps, summary) = iperfPump("127.0.0.1", iport, 1)
    serverFuture.get()
    println("throughput: sent=${sent}B mbps=${mbps}")
    check("throughput above loopback floor", mbps > 100.0)
    check("server confirmed bytes", summary.contains("BYTES"))

    let tracePeer = UdpEchoPeer(port: 0)
    tracePeer.start()
    let tport = tracePeer.localPort()
    let hops = ttlWalk("127.0.0.1", tport, maxHops: 4, probesPerHop: 1, waitMs: 200)
    check("trace: loopback delivered at ttl 1", hops.size >= 1 &&
        match (hops[0].outcome) {
            case Delivered(_) => true
            case _ => false
        })
    for (h in hops) {
        println("trace: ${h.describe()}")
    }

    let scan = tcpScan("127.0.0.1", echoPort, echoPort, concurrency: 2)
    check("scan: echo port found", scan.openPorts.size == 1)
    tracePeer.stop()

    echo.halt()

    println("  RESULT: ${passCount} passed, ${failCount} failed")
    if (failCount > 0) {
        println("DEMO FAIL")
        return 1
    }
    println("DEMO PASS")
    0
}
