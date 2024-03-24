import json, options, os, parsexml, streams, strformat, strutils, times
import argparse
import curly
import malebolgia
import malebolgia/ticketlocks
import zippy/ziparchives

from sequtils import mapIt

let 
  curl = newCurly()
const
  TMP_FILE = "crawl_tmp.xml"
  BUCKET_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
  BUCKET_WEBSITE_URL = "https://data.binance.vision"

type
  MissingConfigError = object of IOError
  ParseJsonError = object of IOError

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

  TradeFile = object
    ticker, extension: string
    kind: MarketDataKind
    date: DateTime

  Prefix* = object
    dir*: string
    asset*: AssetKind
    coin*: CoinKind
    timeFrame*: TimeFrame
    marketDataKind*: MarketDataKind
    token*: string
    extension*: string # not mandatory
    date*: DateTime # not mandatory

  Asset* = object 
    daily*, monthly*: seq[string]

  Assets* = seq[Asset]

  BinanceBulkDownloader* = ref object
    # Contains information relevant to downloading and managing future downloads
    prefix*: Prefix
    skipChecksum*: bool
    downloadList*: seq[string]
    downloadedList*: seq[string]
  
  DownloadConfig* = object
    filename*: string
    batchSize*: int
    prefixes*: seq[Prefix]

# TODO
proc validatePrefix(prefix: Prefix): bool = true

proc `$`(p: Prefix): string = 
  assert p.validatePrefix, "The prefix must be valid"
  result = fmt"data/{p.asset}/{p.coin}/{p.timeFrame}/{p.marketDataKind}/{p.token}"

proc toJson(p: Prefix): JsonNode = 
  var date: DateTime
  if $p.date == "Uninitialized DateTime":
    date = now()
  else:
    date = p.date 
  %*{
    "directory": p.dir,
    "asset": p.asset,
    "coin": p.coin,
    "timeFrame": p.timeFrame,
    "marketDataKind": p.marketDataKind,
    "token": p.token,
    "extension": p.extension,
    "date": date.format("yyyy-MM-dd")
  }

proc fromJson(p: JsonNode): Prefix  {.raises: [ParseJsonError].} =
  var didRaise = false
  try:
    result = Prefix(
      dir: p["directory"].getStr(),
      asset: parseEnum[AssetKind](p["asset"].getStr()),
      coin: parseEnum[CoinKind](p["coin"].getStr()),
      timeFrame: parseEnum[TimeFrame](p["timeFrame"].getStr()),
      marketDataKind: parseEnum[MarketDataKind](p["marketDataKind"].getStr()),
      token: p["token"].getStr(),
      extension: p{"extension"}.getStr() # not mandatory
    )
    if p{"date"}.getStr() != "":
      result.date = parse(p["date"].getStr(), "yyyy-MM-dd")
    
  except:
    didRaise = true
  if didRaise:
    let msg = "Could not parse prefix, please verify all fields are correct: " & $p
    raise newException(ParseJsonError, msg)

proc `$`(t: TradeFile): string = 
  result = join([t.ticker, $t.kind, t.date.format("yyyy-MM-dd")], "-")
  result = result & t.extension

proc saveDownloadConfig*(config: DownloadConfig) =
  var configJson = %*{
    "filename": config.filename,
    "batchSize": config.batchSize,
    "prefixes": []
  }
  if config.prefixes.len > 0:
    for p in config.prefixes:
      configJson["prefixes"].add(p.toJson)
  writeFile(config.filename, pretty(configJson))

proc loadDownloadConfig*(filename: string): DownloadConfig =
  if filename.len <= 0:
    raise newException(MissingConfigError, "Please provide a configuration file to download.")
  try:
    let configJson = parseJson(readFile(filename))
    assert configJson.hasKey("batchSize"), fmt"Config error [{filename}]: batchSize is mandatory in your config file"
    result.filename = filename
    result.batchSize = configJson["batchSize"].getInt()
    if configJson.hasKey("prefixes"):
      result.prefixes = configJson["prefixes"].getElems.mapIt(it.fromJson)
  except:
    raise newException(IOError, fmt"Please ensure you have provided a valid config file (json) which exists! Could not correctly parse {filename}")

proc getFilenamesInDir(dir: string): seq[string] =
  result = newSeq[string]()
  for kind, path in walkDir(dir):
    if kind == pcFile and path.endsWith(".csv"):
      result.add(path)

proc isDirectoryEmpty(dir: string): bool =
  for kind, path in walkDir(dir):
    if kind == pcFile or kind == pcDir:
      return false
  return true

proc parseFile(filename, extension: string = ""): Option[TradeFile] = 
  let fileparts = filename.split("-")
  # we expect a filename in the following format: [TOKEN]-[MARKET DATA KIND]-[YYYY]-[MM]-[DD]
  if fileparts.len == 5:
    let 
      ticker = fileparts[0]
      kind = parseEnum[MarketDataKind](fileparts[1])
      year = parseInt(fileparts[2])
      month = parseInt(fileparts[3])
      day = parseInt(fileparts[4])
      fileDate = dateTime(year, Month(month), MonthdayRange(day))
    return some(TradeFile(ticker: ticker, kind: kind, date: fileDate, extension: extension))
  return none(TradeFile)

proc mostRecentDownload(dir, extension: string): TradeFile = 
  var date = dateTime(1900, Month(1), MonthdayRange(1))
  for kind, path in walkDir(dir):
    if kind == pcFile and path.endsWith(extension):
      let parts = splitFile(path)
      let filename = parts.name
      let parsed = parseFile(filename, extension)
      if isSome(parsed):
        let unpacked: TradeFile = parsed.get
        if unpacked.date > date:
          result = unpacked
          date = unpacked.date

proc isValidDay(d1, d2: DateTime): bool =
  let
    dt1 = dateTime(d1.year, d1.month, d1.monthday)
    dt2 = dateTime(d2.year, d2.month, d2.monthday)
  return dt1 < dt2 and abs(dt1 - dt2) > initDuration(days = 1)

proc markerFromPrefix(p: Prefix): string = 
  result = join(@[p.token, $p.marketDataKind, p.date.format("yyyy-MM-dd")], "-")
  result = $p & "/" & result & p.extension

proc retrieveLinks(initial: bool, c: ptr BinanceBulkDownloader, L: ptr TicketLock) = 
  var
    x: XmlParser
    nextMarker: string
    links = c[].downloadList
    downloadedLinks = c[].downloadedList

  let 
    filename = TMP_FILE
    p = c[].prefix

  if initial:
    if fileExists(filename):
      removeFile(fileName)

    var url: string
    if $p.date != "Uninitialized DateTime":
      url = fmt"{BUCKET_URL}?delimeter=/&prefix={$p}&marker={p.markerFromPrefix}" 
    else:
      url = fmt"{BUCKET_URL}?delimeter=/&prefix={$p}" 

    let r = curl.get(url)
    var tmp = newFileStream(filename, fmWrite)
    tmp.writeLine(r.body)
    tmp.close()

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
                if key.endsWith(".zip") or (key.endsWith(".CHECKSUM") and not c[].skipChecksum):
                  if not downloadedLinks.contains(x.charData):
                    links.add(x.charData)
                    nextMarker = key
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

  withLock L[]:
    c[].downloadList = links

  if nextMarker.len > 0:
    let s = split(splitPath(nextMarker).tail, ".")
    let tradeFileName = s[0]
    let tradeFile = tradeFileName.parseFile
    if isSome(tradeFile):
      if not isValidDay(tradeFile.get.date, now()):
        withLock L[]:
          c[].prefix.date = tradeFile.get.date
          c[].prefix.extension = s[1]
        removeFile(filename)
        return
    let url = fmt"{BUCKET_URL}?delimeter=/&prefix={$p}&marker={nextMarker}/"
    let r = curl.get(url)   
    if r.body.len <= 0:
      removeFile(filename)
    else:
      var tmp = newFileStream(filename, fmWrite)
      tmp.writeLine(r.body)
      tmp.close()
      retrieveLinks(false, c, L)

proc retrieveLinks(c: ptr BinanceBulkDownloader, L: ptr TicketLock) =  
  retrieveLinks(true, c, L)

proc batchDownload(links: seq[string], dir: string) = 
  if links.len > 0:
    if not dirExists(dir):
      createDir(dir)

    var batch: RequestBatch
    for l in links:
      batch.get(fmt"{BUCKET_WEBSITE_URL}/{l}")

    for (response, error) in curl.makeRequests(batch):
      if error == "":
        let 
          filename = response.url.split("/")[^1]
          filePath = dir / filename
        writeFile(filePath, response.body)
        let 
          reader = openZipArchive(filePath)
          csv = filename.split(".")[0] & ".csv"
        try:
          writeFile(dir/csv, reader.extractFile(csv))
          removeFile(filePath)
        except:
          raise newException(IOError, fmt"There was an error reading/writing {csv} from its associated zip archive.")

proc crawl*(c: var BinanceBulkDownloader, d: DownloadConfig) = 
  var m = createMaster()
  for p in d.prefixes:
    var tmp = c
    tmp.downloadList = @[]
    tmp.downloadedList = getFilenamesInDir(p.dir)
    m.awaitAll:
      var 
        L = initTicketLock()
      tmp.prefix = p
      m.spawn retrieveLinks(addr tmp, addr L)
    batchDownload(tmp.downloadList, p.dir)
