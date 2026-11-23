.PHONY: all setup test example format clean clean-all clean-nimble-cache clean-nimbledeps

NIMC ?= nim
NIMFLAGS ?=
STYLE_FLAGS := --styleCheck:usages --styleCheck:error
NIMBLE_FLAGS ?=
NIMBLE_DIR ?= $(HOME)/.nimble

# Choosenim puts a proxy in ~/.nimble/bin, but Nimble needs the underlying
# installation containing nim.nimble in order to recognize a system compiler.
ifeq ($(origin NIMBLE_NIM), undefined)
NIMBLE_NIM := $(shell if command -v choosenim >/dev/null 2>&1; then printf '%s/bin/nim' "$$(choosenim show path | tail -n 1)"; else command -v nim; fi)
endif

NIMBLE = nimble --useSystemNim --nim:"$(NIMBLE_NIM)"
NPH_FILES = $(shell git ls-files --cached --others --exclude-standard -- '*.nim' '*.nimble' '*.nims')

all: test example

setup:
	$(NIMBLE) setup -l $(NIMBLE_FLAGS)

test:
	"$(NIMC)" c $(STYLE_FLAGS) $(NIMFLAGS) tests/test_all.nim
	./tests/test_all
	$(RM) tests/test_all

example:
	"$(NIMC)" c $(STYLE_FLAGS) $(NIMFLAGS) examples/mix_ping_tcp.nim
	$(RM) examples/mix_ping_tcp
	"$(NIMC)" c $(STYLE_FLAGS) $(NIMFLAGS) examples/mix_ping_quic.nim
	$(RM) examples/mix_ping_quic

format:
	nph $(NPH_FILES)

clean:
	$(RM) -r nimbledeps nimcache
	$(RM) nimble.paths nimble.develop
	$(RM) examples/mix_ping_tcp examples/mix_ping_quic tests/test_all

clean-nimble-cache:
	$(RM) "$(NIMBLE_DIR)/pkgcache/tagged_versions.json"
	$(RM) "$(NIMBLE_DIR)/packages_official.json"
	$(RM) "$(NIMBLE_DIR)/packages_temp.json"

clean-nimbledeps:
	$(RM) -r nimbledeps
	$(RM) nimble.paths nimble.develop

clean-all: clean clean-nimble-cache
