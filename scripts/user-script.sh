#!/usr/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

set -o xtrace
set -o errexit
set -o pipefail

#
# To use the same convention as SDC instances, the presence of the
# /var/svc/.ran-user-script file indicates that the instance has already been
# setup (i.e. the instance has booted previously).
#
# Upon first boot, run the setup.sh script if present.  On all boots including
# the first one, run the configure.sh script if present.
#
SENTINEL=/var/svc/.ran-user-script

DIR=/opt/smartdc/boot


if [[ ! -e ${SENTINEL} ]]; then
	if [[ -f ${DIR}/setup.sh ]]; then
		${DIR}/setup.sh
	fi

	touch ${SENTINEL}
fi

if [[ -f ${DIR}/configure.sh ]]; then
	${DIR}/configure.sh
fi
