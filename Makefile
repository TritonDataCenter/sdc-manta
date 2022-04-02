#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2022 Joyent, Inc.
#

#
# Makefile: basic Makefile to build deployment image
#
# This Makefile should contain only repo-specific logic and uses included
# makefiles to supply common targets (javascriptlint, jsstyle, restdown, etc.),
# which are used by other repos as well.
#
# If you find yourself adding support for new targets that could be useful for
# other projects too, you should add these to the original versions of the
# included Makefiles (in eng.git) so that other teams can use them too.
#

#
# Programs
#
CATEST		 = deps/catest/catest
PROBECHK	 = node ./tools/probecfgchk.js

#
# Options and overrides
#

# Overrides needed to use v8plus for binary modules
NPM_ENV		 = MAKE_OVERRIDES="CTFCONVERT=/bin/true CTFMERGE=/bin/true"

#
# Files
#
BASH_FILES	 = scripts/user-script.sh  \
		   tools/add-dev-user      \
		   bin/manta-deploy-lab \
		   networking/manta-net.sh \
		   tools/rsync-to
DOC_FILES	 = index.md
ESLINT_FILES	:= $(shell find cmd lib test networking tools -name '*.js')
JSON_FILES	 = package.json \
		   $(shell find config \
				manifests \
				sapi_manifests -name '*.json*')
PROBE_FILES	 = $(wildcard alarm_metadata/probe_templates/*.yaml)

# Set these just so that 'make check' validates the xml
SMF_MANIFESTS	= networking/smf/manta-nic.xml \
		  networking/smf/xdc-route.xml

ENGBLD_USE_BUILDIMAGE	= true
ENGBLD_REQUIRE		:= $(shell git submodule update --init deps/eng)
include ./deps/eng/tools/mk/Makefile.defs
TOP ?= $(error Unable to access eng.git submodule Makefiles.)

NODE_PREBUILT_VERSION=v6.17.1
# Even though sdc-manta is deployed in its own zone, the executable programs
# that it ships are often executed from the GZ, so we need to ship a runtime
# that is able to run in the GZ.
NODE_PREBUILT_TAG=zone64
# minimal-64-lts 21.4.0
NODE_PREBUILT_IMAGE=a7199134-7e94-11ec-be67-db6f482136c2

BUILD_PLATFORM  = 20210826T002459Z

ifeq ($(shell uname -s),SunOS)
	include ./deps/eng/tools/mk/Makefile.node_prebuilt.defs
	include ./deps/eng/tools/mk/Makefile.agent_prebuilt.defs
	include ./deps/eng/tools/mk/Makefile.smf.defs
else
	NPM=npm
	NODE=node
	NPM_EXEC=$(shell which npm)
	NODE_EXEC=$(shell which node)
endif

MAN_INROOT	 = docs/man
MAN_OUTROOT	 = man
MAN_SECTION	:= 1
include ./deps/eng/tools/mk/Makefile.manpages.defs

#
# MG variables
#
NAME		= manta-deployment
RELEASE_TARBALL := $(NAME)-pkg-$(STAMP).tar.gz

# triton-origin-x86_64-21.4.0
BASE_IMAGE_UUID = 502eeef2-8267-489f-b19c-a206906f57ef
BUILDIMAGE_NAME = mantav2-deployment
BUILDIMAGE_DESC	= Manta deployment tools
BUILDIMAGE_PKGSRC = openldap-client-2.6.0
AGENTS		= amon config

#
# Packaging variables
#
TOP		:= $(shell pwd)
PROTO		:= $(TOP)/proto
INSTDIR		:= $(PROTO)/root/opt/smartdc/$(NAME)


#
# Repo-specific targets
#
.PHONY: all
all: $(SMF_MANIFESTS) deps sdc-scripts

.PHONY: manpages
manpages: $(MAN_OUTPUTS)

check:: $(NODE_EXEC)

#
# We'd like to run "check-probe-files" under "check", but this requires
# MANTA-3251 in order to work in the context of CI checks.  However, we can at
# least put this under "prepush", which (for better or worse) already can't
# generally be run in anonymous CI environments.
#
check-probe-files:
	$(PROBECHK) $(PROBE_FILES)

# Just lint check (no style)
.PHONY: lint
lint: | $(ESLINT)
	$(ESLINT) --rule 'prettier/prettier: off' $(ESLINT_FILES)

.PHONY: fmt
fmt: | $(ESLINT)
	$(ESLINT) --fix $(ESLINT_FILES)

prepush: check-probe-files

.PHONY: test
test: | $(CATEST)
	PATH="$(TOP)/build/node/bin:$$PATH" $(CATEST) -a

$(CATEST): deps/catest/.git

.PHONY: deps
deps: | $(REPO_DEPS) $(NPM_EXEC)
	$(NPM_ENV) $(NPM) install

.PHONY: release
release: all deps docs $(SMF_MANIFESTS)
	@echo "Building $(RELEASE_TARBALL)"
	@rm -rf $(PROTO)
	@mkdir -p $(INSTDIR)
	cp -r   $(TOP)/alarm_metadata \
		$(TOP)/bin \
		$(TOP)/build \
		$(TOP)/cmd \
		$(TOP)/config \
		$(TOP)/docs \
		$(TOP)/lib \
		$(TOP)/man \
		$(TOP)/manifests \
		$(TOP)/networking \
		$(TOP)/node_modules \
		$(TOP)/package.json \
		$(TOP)/sapi_manifests \
		$(TOP)/scripts \
		$(TOP)/tools \
		$(INSTDIR)
	@mkdir $(PROTO)/site
	@mkdir $(INSTDIR)/etc
	@mkdir $(INSTDIR)/log
	mkdir -p $(PROTO)/root/opt/smartdc/boot
	cp -R $(TOP)/deps/sdc-scripts/* $(PROTO)/root/opt/smartdc/boot/
	cp -R $(TOP)/boot/* $(PROTO)/root/opt/smartdc/boot/
	(cd $(PROTO) && $(TAR) -I pigz -cf $(TOP)/$(RELEASE_TARBALL) root site)

.PHONY: publish
publish: release
	mkdir -p $(ENGBLD_BITS_DIR)/$(NAME)
	cp $(TOP)/$(RELEASE_TARBALL) $(ENGBLD_BITS_DIR)/$(NAME)/$(RELEASE_TARBALL)


CLEAN_FILES += node_modules

include ./deps/eng/tools/mk/Makefile.deps
ifeq ($(shell uname -s),SunOS)
    include ./deps/eng/tools/mk/Makefile.node_prebuilt.targ
    include ./deps/eng/tools/mk/Makefile.agent_prebuilt.targ
    include ./deps/eng/tools/mk/Makefile.smf.targ
endif
include ./deps/eng/tools/mk/Makefile.targ

MAN_SECTION	:= 1
include ./deps/eng/tools/mk/Makefile.manpages.targ

sdc-scripts: deps/sdc-scripts/.git
