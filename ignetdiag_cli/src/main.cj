package ignetdiag_cli

import ignetdiag.Commons.*
import ignetdiag.Master.cli.*
import std.collection.*
import std.env.*

main(): Int64 {
    let args = getCommandLine()
    // drop argv[0] (the binary path)
    let rest = ArrayList<String>()
    var i: Int64 = 1
    while (i < args.size) {
        rest.add(args[i])
        i += 1
    }
    if (rest.size == 0) {
        println(usageText())
        return EXIT_USAGE
    }
    run(rest)
}
