switch("nimcache", "nimcache")
switch("path", thisDir() & "/mix_transport")

# begin Nimble config (version 2)
when withDir(thisDir(), system.fileExists("nimble.paths")):
  include "nimble.paths"
# end Nimble config
