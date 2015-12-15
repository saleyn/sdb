DIR       := $(dir $(abspath src))
DIR       := $(DIR:%/=%)
build     ?= Debug
prefix     = install
toolchain ?= gcc

all install test help:
	@if [ -f build/Makefile ]; then \
        CTEST_OUTPUT_ON_FAILURE=TRUE $(MAKE) -C build $(if $(verbose),VERBOSE=1) \
            -j$(shell nproc) $@; \
    else \
    	echo "Run: make bootstrap [toolchain=gcc|clang|intel]"; \
    	echo; \
    	echo "To customize variables for cmake, create a local file with VAR=VALUE pairs:"; \
    	echo "  .cmake-args.\$$HOSTNAME"; \
    fi

bootstrap:
	cmake -H$(DIR) -B$(DIR)/build $(if $(verbose),-DCMAKE_VERBOSE_MAKEFILE=true) \
        -DTOOLCHAIN=$(toolchain) \
        -DCMAKE_USER_MAKE_RULES_OVERRIDE=$(DIR)/build/CMakeInit.txt \
        -DCMAKE_INSTALL_PREFIX=$(prefix) \
        -DCMAKE_BUILD_TYPE=$(build) $(if $(debug),-DDEBUG=vars) \
        $(patsubst %,-D%,$(filter-out toolchain=% generator=% build=% verbose=%,$(MAKEOVERRIDES))) \
        $(patsubst %,-D%,$(shell F=.cmake-args.$$HOSTNAME && [ -f "$$F" ] && cat $$F | xargs))

%:
	$(MAKE) -C build -j$(shell nproc) $@

.PHONY: bootstrap test all
