#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

#
# Makefile: basic Makefile to build deployment tarball
#

#
# Programs
#
CATEST		 = deps/catest/catest

#
# Files
#
BASHSTYLE	 = $(NODE) tools/bashstyle
BASH_FILES	 = scripts/user-script.sh  \
		   tools/add-dev-user      \
		   bin/manta-deploy-lab \
		   networking/manta-net.sh
DOC_FILES	 = $(shell find docs -name '*.md' | cut -d '/' -f 2)
JS_FILES	:= $(shell find cmd lib test -name '*.js')
JSON_FILES	 = package.json \
		   $(shell find config \
				manifests \
				sapi_manifests -name '*.json*')
JSL_CONF_NODE	 = tools/jsl.node.conf
JSL_FILES_NODE	 = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
JSSTYLE_FLAGS	 = -o doxygen
NPM_ENV		 = MAKE_OVERRIDES="CTFCONVERT=/bin/true CTFMERGE=/bin/true"

NODE_PREBUILT_VERSION=v0.10.32
NODE_PREBUILT_TAG=zone
NODE_PREBUILT_IMAGE=fd2cc906-8938-11e3-beab-4359c665ac99


include ./tools/mk/Makefile.defs
include ./tools/mk/Makefile.node_prebuilt.defs
include ./tools/mk/Makefile.node_deps.defs


#
# MG variables
#
NAME		= manta-deployment
RELEASE_TARBALL := $(NAME)-pkg-$(STAMP).tar.bz2


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

check:: $(NODE_EXEC)

.PHONY: test
test: | $(CATEST)
	$(CATEST) -a

$(CATEST): deps/catest/.git

.PHONY: deps
deps: | $(REPO_DEPS) $(NPM_EXEC)
	$(NPM_ENV) $(NPM) install

.PHONY: shrinkwrap
shrinkwrap: | $(NPM_EXEC)
	$(NPM) shrinkwrap

.PHONY: release
release: all deps docs $(SMF_MANIFESTS)
	@echo "Building $(RELEASE_TARBALL)"
	@rm -rf $(PROTO)
	@mkdir -p $(INSTDIR)
	cp -r   $(TOP)/bin \
		$(TOP)/build \
		$(TOP)/cmd \
		$(TOP)/config \
		$(TOP)/docs \
		$(TOP)/lib \
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
	(cd $(PROTO) && $(TAR) -jcf $(TOP)/$(RELEASE_TARBALL) root site)

.PHONY: publish
publish: release
	@if [[ -z "$(BITS_DIR)" ]]; then \
    echo "error: 'BITS_DIR' must be set for 'publish' target"; \
    exit 1; \
  fi
	mkdir -p $(BITS_DIR)/$(NAME)
	cp $(TOP)/$(RELEASE_TARBALL) $(BITS_DIR)/$(NAME)/$(RELEASE_TARBALL)


CLEAN_FILES += node_modules

include ./tools/mk/Makefile.deps
include ./tools/mk/Makefile.node_prebuilt.targ
include ./tools/mk/Makefile.node_deps.targ
include ./tools/mk/Makefile.targ

sdc-scripts: deps/sdc-scripts/.git
