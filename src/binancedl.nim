import binancedlpkg/submodule
import argparse

from std/cmdline import commandLineParams

proc main() = 
  let 
    args: seq[string] = commandLineParams()
    p = Prefix(asset: Futures,
      coin: COIN,
      timeFrame: Daily,
      marketDataKind: Trades,
      token: "BTCUSD_PERP")
  var 
    links: seq[string] = newSeq[string]()
    c = BinanceBulkDownloader.new
    d: DownloadConfig
    par = newParser: 
      flag("-s", "--skipchecksum")
      option("-c", "--config", help="Path for config file")
 
  try:
    var opts = par.parse(args)
    d = loadDownloadConfig(opts.config)
    let dest = d.destinationDirectory
    c.lastSeen = d.lastSeen
    c.skipChecksum = opts.skipchecksum
    c.downloadList = links
    c.destinationDirectory = dest
    c.prefix = p
    crawl(c, d)
    
    d.lastSeen = c.lastSeen
    saveDownloadConfig(d, opts.config)
  except:
    echo getCurrentExceptionMsg()
  
when isMainModule:
  main()