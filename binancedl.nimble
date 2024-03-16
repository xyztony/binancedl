# Package
version       = "0.1.0"
author        = "xyztony"
description   = "Binance Bulk Download CLI"
license       = "MIT"
srcDir        = "src"
installExt    = @["nim"]
bin           = @["binancedl"]


# Dependencies
requires "argparse >= 4.0.1"
requires "curly >= 1.0.1"
requires "malebolgia >= 1.3.1"
requires "nim >= 2.0.0"
requires "zippy >= 0.10.11"