package ignetdiag.Features.scan

import std.collection.*
import std.net.*
import std.sync.*
import std.time.*

/// ---- TCP connect port scan (concurrency-bounded) ----

public class ScanResult {
    public let host: String
    public let from: UInt16
    public let to: UInt16
    public let openPorts: ArrayList<UInt16>
    public var scanned: Int64
    let mutex: Mutex = Mutex()

    public init(host!: String, from!: UInt16, to!: UInt16) {
        this.host = host
        this.from = from
        this.to = to
        this.openPorts = ArrayList<UInt16>()
        this.scanned = 0
    }

    public func renderText(): String {
        if (openPorts.size == 0) {
            return "no open ports in ${from}..${to}"
        }
        var list = ""
        var i: Int64 = 0
        while (i < openPorts.size) {
            if (i > 0) { list = list + ", " }
            list = list + openPorts[i].toString()
            i += 1
        }
        "open: ${list}"
    }
}

/// Scan host:port range with `concurrency` workers. Open ports are
/// collected sorted ascending (workers claim the next port via the
/// shared counter).
public func tcpScan(host: String, from: UInt16, to: UInt16,
                    concurrency!: Int64 = 32, timeoutMs!: Int64 = 400): ScanResult {
    let result = ScanResult(host: host, from: from, to: to)
    let next = AtomicInt64(Int64(from))
    let workers = if (concurrency < 1) { 1 } else { concurrency }
    var futures = ArrayList<Future<Unit>>()
    var w: Int64 = 0
    while (w < workers) {
        futures.add(spawn {
            while (true) {
                let current = next.load()
                if (current > Int64(to)) { break }
                next.store(current + 1)
                var open = false
                try {
                    let s = TcpSocket(host, UInt16(current))
                    s.connect(timeout: Duration.millisecond * timeoutMs)
                    open = true
                    s.close()
                } catch (_: Exception) {
                    open = false
                }
                if (open) {
                    synchronized(result.mutex) {
                        result.openPorts.add(UInt16(current))
                    }
                }
            }
        })
        w += 1
    }
    for (f in futures) {
        f.get()
    }
    // sort ascending (insertion sort over a typically-small open list)
    let n = result.openPorts.size
    var i: Int64 = 1
    while (i < n) {
        let key = result.openPorts[i]
        var j = i - 1
        while (j >= 0 && result.openPorts[j] > key) {
            result.openPorts[j + 1] = result.openPorts[j]
            j -= 1
        }
        result.openPorts[j + 1] = key
        i += 1
    }
    result.scanned = Int64(to) - Int64(from) + 1
    result
}
