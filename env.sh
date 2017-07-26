#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2017, Joyent, Inc.
#

#
# bash environment file for working on this repository.
# This puts this repository's command-line tools onto the PATH, along with the
# "node" that's bundled with this component.
#

env_sh_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -o xtrace
export PATH="$env_sh_dir/build/node/bin:$env_sh_dir/bin:$PATH"
set +o xtrace
