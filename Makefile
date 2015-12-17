DIR       := $(dir $(abspath src))
DIR       := $(DIR:%/=%)
build     ?= Debug
prefix     = install
toolchain ?= gcc
HAS_MAKE  := $(wildcard $(DIR)/build/Makefile)
HAS_NINJA := $(wildcard $(DIR)/build/build.ninja)
GEN_FILE  := $(if $(HAS_MAKE)$(HAS_NINJA),true,false)
DEF_GEN   := $(if $(HAS_MAKE),make,$(if $(HAS_NINJA),ninja,$(shell which ninja 2>/dev/null)))
GENERATOR := $(if $(generator),$(generator),$(if $(DEF_GEN),$(DEF_GEN),make))
IS_NINJA  := $(findstring ninja,$(GENERATOR))
GEN_TYPE  := $(if $(IS_NINJA),-GNinja, -G"Unix Makefiles")
VERBOSE   := $(if $(verbose),$(if $(IS_NINJA),-v,VERBOSE=1))
GEN_OPTS  := $(if $(IS_NINJA),,--no-print-directory -j$(shell nproc)) $(VERBOSE)

all install test help:
	@if $(GEN_FILE); then \
        CTEST_OUTPUT_ON_FAILURE=TRUE $(GENERATOR) -C build \
            $(GEN_OPTS) $@; \
    else \
    	echo "Run: make bootstrap [toolchain=gcc|clang|intel]"; \
    	echo; \
    	echo "To customize variables for cmake, create a local file with VAR=VALUE pairs:"; \
    	echo "  .cmake-args.\$$HOSTNAME"; \
    fi

bootstrap:
	cmake -H$(DIR) -B$(DIR)/build $(if $(verbose),-DCMAKE_VERBOSE_MAKEFILE=true) \
        -DTOOLCHAIN=$(toolchain) $(GEN_TYPE) \
        -DCMAKE_USER_MAKE_RULES_OVERRIDE=$(DIR)/build-aux/CMakeInit.txt \
        -DCMAKE_INSTALL_PREFIX=$(prefix) \
        -DCMAKE_BUILD_TYPE=$(build) $(if $(debug),-DDEBUG=vars) \
        $(patsubst %,-D%,$(filter-out toolchain=% generator=% build=% verbose=%,$(MAKEOVERRIDES))) \
        $(patsubst %,-D%,$(shell F=.cmake-args.$$HOSTNAME && [ -f "$$F" ] && cat $$F | xargs))

distclean:
	rm -fr $(DIR)/build

info:
	@echo "Generator : $(GENERATOR)"
	@echo "GenOptions: $(GEN_OPTS)"

vars:
	@cmake -H$(DIR) -B$(DIR)/build -LA

%:
	$(GENERATOR) -C build $(GEN_OPTS) $@

.PHONY: bootstrap test all install help distclean info vars
