import binancedlpkg/submodule
import argparse

from std/cmdline import commandLineParams

proc main() =
  let
    args: seq[string] = commandLineParams()
  var
    c = BinanceBulkDownloader.new
    d: DownloadConfig
    par = newParser:
      flag("-s", "--skipchecksum")
      option("-c", "--config", help="Path for config file")
      
  try:
    var opts = par.parse(args)
    c.configPath = opts.config
    d = loadDownloadConfig(c.configPath)
    c.skipChecksum = opts.skipchecksum
    c.downloadList = newSeq[string]()
    crawl(c, d)
  except:
    echo repr(getCurrentException()) & " message:" & getCurrentExceptionMsg()

when isMainModule:
  main()
