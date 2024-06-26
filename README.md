# Binance Market Data Downloader
This is a rudimentary application to *crudely* download data from [Binance](https://www.binance.com/en/support/faq/how-to-download-historical-market-data-on-binance-5810ae42176b4770b880ce1f14932262).

## Usage

Build with something such as `nim c -d:release -o:binancedl src/binancedl.nim`.

To use, run the following command:
```
./src/binancedl -c=your_config.json [-s]
```

### Parameters
- `-c` or `--config` (mandatory): Specifies the location of your configuration file
- `-s` or `--skipchecksums` (optional): Allows you to specify whether you want to download the `*.zip.CHECKSUM` files. If this parameter is provided, the checksum files will be skipped during download

### Configuration File
The configuration file (`$YOUR_CONFIG.json`) is specified as follows:
```json
{
  "batchSize": 10,
  "prefixes": [
    {
      "directory": "data/btc/kline",
      "asset": "futures",
      "coin": "cm",
      "timeFrame": "daily",
      "marketDataKind": "klines",
      "granularity": "15m",
      "token": "BTCUSD_PERP",
      "extension": ".zip",
      "date": "2024-01-01"
    },
    {
      "directory": "data/btc/candles",
      "asset": "futures",
      "coin": "cm",
      "timeFrame": "daily",
      "marketDataKind": "bookTicker",
      "token": "BTCUSD_PERP",
      "extension": ".zip",
      "date": "2024-01-01"
    }
  ]
}
```


## TODOs
There's a lot to do to make this more ergonomic and efficient.

Primary TODOs:
* Checksum validation
* More ergonomic CLI
* Background processing
* There is a notion of a `Prefix` which dictates what data will be downloaded. There are restrictions on how `Prefix`'s may be defined, there should be a function to validate that a `Prefix` is valid before continuing with any downloading
* API changes so this can be better utilized in an application (think downloading historical files on the fly, loading into memory [mmap files, sqlite, etc])

