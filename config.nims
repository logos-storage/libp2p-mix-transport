switch("nimcache", "nimcache")
switch("path", thisDir())

# begin Nimble config (version 2)
when withDir(thisDir(), system.fileExists("nimble.paths")):
  include "nimble.paths"
# end Nimble config

# Compile trace logs and enable runtime filtering.
switch("define", "chronicles_runtime_filtering=on")
switch("define", "chronicles_log_level=TRACE")
