import json, options, os, parsexml, streams, strformat, strutils, times
import argparse
import curly
import malebolgia
import zippy/ziparchives

let 
  curl = newCurly()
const
  BUCKET_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
  BUCKET_WEBSITE_URL = "https://data.binance.vision"

type
  MissingConfigError = object of Exception

  AssetKind* = enum
    Futures = "futures",
    Options = "option",
    Spot = "spot" 

  CoinKind* = enum
    USD = "um",
    COIN = "cm"

  TimeFrame* = enum
    Daily = "daily",
    Monthly = "monthly"

  MarketDataKind* = enum
    AggTrades = "aggTrades",
    BookDepth = "bookDepth",
    BookTicker = "bookTicker",
    IndexPriceKlines = "indexPriceKlines",
    Klines = "klines",
    LiquidationSnapshot = "liquidationSnapshot",
    MarkPriceKlines = "markPriceKlines",
    Metrics = "metrics",
    PremiumIndexKlines = "premiumIndexKlines",
    Trades = "trades"

  Prefix* = object
    asset*: AssetKind
    coin*: CoinKind
    timeFrame*: TimeFrame
    marketDataKind*: MarketDataKind
    token*: string

  Asset* = object 
    daily*, monthly*: seq[string]

  Assets* = seq[Asset]

  TradeFile = object
    ticker, extension: string
    kind: MarketDataKind
    date: DateTime

  BinanceBulkDownloader* = ref object
    # Contains information relevant to downloading and managing future downloads
    destinationDirectory*: string
    prefix*: Prefix
    lastSeen*: TradeFile
    skipChecksum*: bool
    downloadList*: seq[string]
    downloadedList*: seq[string]
  
  DownloadConfig* = object
    destinationDirectory*: string
    lastSeen*: TradeFile
    batchSize*: int

# TODO
proc validatePrefix(prefix: Prefix): bool = true

proc `$`(p: Prefix): string = 
  assert p.validatePrefix, "The prefix must be valid"
  result = fmt"data/{p.asset}/{p.coin}/{p.timeFrame}/{p.marketDataKind}/{p.token}/"

proc `$`(t: TradeFile): string = 
  result = join([t.ticker, $t.kind, t.date.format("yyyy-MM-dd")], "-")
  result = result & t.extension

proc saveDownloadConfig*(config: DownloadConfig, filename: string) =
  let configJson = %*{
    "batchSize": config.batchSize,
    "destinationDirectory": config.destinationDirectory,
    "lastSeen": {
      "ticker": config.lastSeen.ticker,
      "extension": config.lastSeen.extension,
      "kind": $config.lastSeen.kind,
      "date": $config.lastSeen.date.format("yyyy-MM-dd")
    }
  }
  writeFile(filename, pretty(configJson))

proc loadDownloadConfig*(filename: string): DownloadConfig =
  if filename.len <= 0:
    raise newException(MissingConfigError, "Please provide a configuration file to download.")

  try:
    let configJson = parseJson(readFile(filename))
    assert configJson.hasKey("batchSize"), fmt"Config error [{filename}]: batchSize is mandatory in your config file"
    assert configJson.hasKey("destinationDirectory"), fmt"Config error [{filename}]: destinationDirectory is mandatory in your config file"

    result.batchSize = configJson["batchSize"].getInt()
    result.destinationDirectory = configJson["destinationDirectory"].getStr()
    if configJson.hasKey("lastSeen"):
      let lastSeenJson = configJson["lastSeen"]
      result.lastSeen = TradeFile(
        ticker: lastSeenJson["ticker"].getStr(),
        extension: lastSeenJson["extension"].getStr(),
        kind: parseEnum[MarketDataKind](lastSeenJson["kind"].getStr()),
        date: parse(lastSeenJson["date"].getStr(), "yyyy-MM-dd")
      )
    else:
      result.lastSeen = TradeFile( # fill the TradeFile with defaults
        ticker: "",
        extension: "",
        kind: AggTrades,
        date: now()
      )
  except:
    raise newException(IOError, fmt"Please ensure you have provided a config file (json) which exists! Could not find or open {filename}")

proc isDirectoryEmpty(directory: string): bool =
  for kind, path in walkDir(directory):
    if kind == pcFile or kind == pcDir:
      return false
  return true

proc parseFile(filename, extension: string = ""): Option[TradeFile] = 
  let fileparts = filename.split("-")
  if fileparts.len == 5:
    let 
      ticker = fileparts[^5]
      kind = parseEnum[MarketDataKind](fileparts[^4])
      year = parseInt(fileparts[^3])
      month = parseInt(fileparts[^2])
      day = parseInt(fileparts[^1])
      fileDate = dateTime(year, Month(month), MonthdayRange(day))
    return some(TradeFile(ticker: ticker, kind: kind, date: fileDate, extension: extension))

  return none(TradeFile)

proc mostRecentDownload(dir, extension: string): TradeFile = 
  var 
    file: TradeFile
    date = dateTime(1900, Month(1), MonthdayRange(1))
    
  for kind, path in walkDir(dir):
    if kind == pcFile and path.endsWith(extension):
      let parts = splitFile(path)
      let filename = parts.name
      let parsed = parseFile(filename, extension)
      if isSome(parsed):
        let unpacked: TradeFile = parsed.get
        if unpacked.date > date:
          file = unpacked
          date = unpacked.date

  return file

proc isValidDay(d1, d2: DateTime): bool =
  let
    dt1 = dateTime(d1.year, d1.month, d1.monthday)
    dt2 = dateTime(d2.year, d2.month, d2.monthday)

  if dt1 < dt2 and abs(dt1 - dt2) > initDuration(days = 1):
    return true
  else:
    return false

proc retrieveLinks(tmpFile: string, initial: var bool, c: var BinanceBulkDownloader) = 
  var
    x: XmlParser
    nextMarker: string
    links = c.downloadList
  let 
    filename = tmpFile
    p = c.prefix
    lastSeen = c.lastSeen

  if initial:
    if fileExists(filename):
      removeFile(fileName)

    var url: string
    if not isNil(addr lastSeen):
      url = fmt"{BUCKET_URL}?delimeter=/&prefix={$p}&marker={$p}{$lastSeen}" 
    else:
      url = fmt"{BUCKET_URL}?delimeter=/&prefix={$p}" 

    let r = curl.get(url)

    var tmp = newFileStream(filename, fmWrite)
    tmp.writeLine(r.body)
    tmp.close()
    initial = false

  var s = openFileStream(filename, fmRead)
  assert s != nil, fmt"There is an error opening {filename}"
  open(x, s, filename)
  x.next # skip first node
  block mainLoop:
    while true:
      case x.kind
      of xmlElementStart:
        if x.elementName == "Contents":
          while true:
            x.next
            case x.kind
            of xmlElementStart: 
              if x.elementName == "Key":
                x.next
                let key = x.charData
                if key.endsWith(".zip"):
                  links.add(x.charData)
                  nextMarker = key
                elif key.endsWith(".CHECKSUM"):
                  if not c.skipChecksum:
                    links.add(x.charData)
                break
            else:
              x.next
        else:
          x.next
      of xmlEof: 
        break
      of xmlError:
        x.next
      else: 
        x.next

  x.close()
  s.close()
  c.downloadList = links

  if nextMarker.len > 0:
    let filename = split(splitPath(nextMarker).tail, ".")[0]
    let tradeFile = filename.parseFile
    if isSome(tradeFile):
      let tf = tradeFile.get
      if not isValidDay(tf.date, now()):
        removeFile(filename)
        return

    let url = fmt"{BUCKET_URL}?delimeter=/&prefix={$p}&marker={nextMarker}/"
    let r = curl.get(url)   
    if r.body.len <= 0:
      removeFile(filename)
      return
    else:
      var tmp = newFileStream(filename, fmWrite)
      tmp.writeLine(r.body)
      tmp.close()
      retrieveLinks(tmpFile, initial, c)

proc batchDownload(c: var BinanceBulkDownloader, d: DownloadConfig) = 
  var
    links = c.downloadList
  let 
    batchSize = min(d.batchSize, links.len - 1)
    dir = d.destinationDirectory

  if links.len > 0:
    if not dirExists(dir):
      createDir(dir)

    for i in countup(0, links.len, batchSize):
      let 
        batchEnd = min(i + batchSize - 1, links.len - 1)
        sbatch = links[i..batchEnd]

      var batch: RequestBatch
      for l in sbatch:
        batch.get(fmt"{BUCKET_WEBSITE_URL}/{l}")

      for (response, error) in curl.makeRequests(batch):
        if error == "":
          let 
            filename = response.url.split("/")[^1]
            filePath = dir / filename
          writeFile(filePath, response.body)


proc unzipp(kind: PathComponent, path: string, dl: seq[string], dir: string) = 
  let 
    filename = splitPath(path).tail
    isDownloaded = any(dl, proc(f: string): bool = filename in splitPath(f).tail)
  if kind == pcFile and path.endswith(".zip") and isDownloaded:
    let 
      reader = openZipArchive(path)
      csv = filename.split(".")[0] & ".csv"
    try:
      writeFile(dir/csv, reader.extractFile(csv))
      removeFile(path)
    except:
      raise newException(IOError, fmt"There was an error reading/writing {csv} from its associated zip archive.")

proc processFiles(list: seq[string], dir: string, m: MasterHandle) = 
  if list.len > 0:
    for kind, path in walkDir(dir):
      m.spawn unzipp(kind, path, list, dir)

proc crawl*(c: var BinanceBulkDownloader, d: DownloadConfig) = 
  let tmpFile = "crawl_tmp.xml"
  var 
    initial = true
    m = createMaster()
  retrieveLinks(tmpFile, initial, c)
  batchDownload(c, d)
  m.awaitAll:
    m.spawn processFiles(c.downloadList, d.destinationDirectory, getHandle m)