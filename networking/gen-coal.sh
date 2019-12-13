#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2019 Joyent, Inc.
#

#
# Generate a manta networking json configuration file for coal. If you change
# the coal defaults, this will not work.
#
unalias -a
set -o pipefail

gc_arg0=$(basename $0)
gc_server=
gc_mac=

function fatal
{
	local msg="$*"
	[[ -z "$msg" ]] && msg="failed"
	echo "$gc_arg0: $msg" >&2
	exit 1
}

function get_server
{
	local json count uuid

	json=$(sdc-cnapi /servers?hostname=headnode | json -H)
	[[ $? -eq 0 ]] || fatal "failed to search for headnode in cnapi"
	[[ -z "$json" ]] && fatal "got an empty entry for hostname"
	count=$(echo $json -a uuid | wc -l)
	[[ $? -eq 0 ]] || fatal "failed to count cnapi results"
	[[ $count -eq 1 ]] || fatal "found multimple headnode entries in cnapi"
	uuid=$(echo $json | json -a uuid)
	[[ $? -eq 0 ]] || fatal "failed to get extract uuid"
	[[ -z "$uuid" ]] && fatal "found an empty uuid"
	gc_server=$uuid
}

function get_mac
{
	local mac

	mac=$(nictagadm list -p -d, | awk -F, '$1 == "external" { print $2 }')
	[[ $? -eq 0 ]] || fatal "failed to run nictagadm/awk"
	[[ -z "$mac" ]] && fatal "found an empty mac address"
	gc_mac=$mac
}

function give_warning
{
	cat >&2 <<EOF
This is about to generate a Coal-based configuration for manta networking. If
you do not use the default networking settings, it is very likely that this will
not work for you.
EOF
}

function write_config
{
	cat <<EOF
{
	"this_az": "coal",
        "manta_nodes": [ "$gc_server" ],
	"azs": [ "coal" ],
	"admin": {
		"nic_tag": "admin",
		"network": "admin",
		"coal": {
			"subnet": "10.99.99.0/24",
			"gateway": "10.99.99.7"
		}
	},
        "manta": {
		"nic_tag": "manta",
		"network": "manta",
                "coal": {
                        "vlan_id": 0,
                        "subnet":  "10.77.77.0/24",
                        "start":   "10.77.77.5",
                        "end":  "10.77.77.254",
                        "gateway": "10.77.77.2"
                }
	}, "mac_mappings": {
		"$gc_server": {
			"manta": "$gc_mac"
		}
	}
}
EOF
}

give_warning
get_mac
get_server
write_config
