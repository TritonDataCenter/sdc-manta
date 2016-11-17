#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2016, Joyent, Inc.
#

#
# Set up manta networking.  This script does many things, but at a high level,
# it creates the manta and mantanat networks, and configures all servers in a
# datacenter with those networks.
#

set -o xtrace
unalias -a
set -o pipefail

mn_arg0=$(basename $0)
mn_config=
mn_confchecker="/usr/node/bin/node ./validate.js"
mn_ouraz=
mn_azlist=
mn_otheraz=
mn_ufds_adminid=
mn_out_dir="output.$$"
mn_manta_vnic="manta0"

#
# Variables for passing around return data.
#
mn_tmp_gw=
mn_tmp_routes=

function fatal
{
	local msg="$*"
	[[ -z "$msg" ]] && msg="failed"
	echo "$mn_arg0: $msg" >&2
	exit 1
}

function warn
{
	local msg="$*"
	[[ -z "$msg" ]] && msg="failed"
	echo "$mn_arg0: $msg" >&2
}

function uniqueify_routes
{
	local cgate croutes nroutes nkeys
	local tmpfile="/tmp/route.$$"
	local r g out
	cgate=$1
	croutes=$2
	nroutes=$3
	[[ -z "$cgate" ]] && fatal "missing config gateway"
	[[ -z "$croutes" ]] && fatal "missing config routes"
	# nroutes may be empty
	rm -f $tmpfile
	for r in $croutes; do
		echo "$r $cgate" >> $tmpfile
	done
	if [[ -n "$nroutes" ]]; then
		nkeys=$(echo "$nroutes" | json -ak)
		[[ $? -eq 0 ]] || fatal "failed to parse route object"
		for r in $nekys; do
			g=$(echo $nroutes | json $r)
			[[ -z "$g" ]] && fatal "failed to find routes"
			echo "$r $g" >> $tmpfile
		done
	fi
	sort < $tmpfile | uniq > $tmpfile.sort
	# Check for dups that disagree about gateways
	out=$(awk '{ print $1 }' < $tmpfile.sort | uniq -d)
	[[ -z "$out" ]] || fatal "mismatching gateways"
	nawk 'BEGIN{ print "{" }
	    { if ( NR > 1 ) { printf "," } print "\t\""$1"\": \""$2"\"" }
	    END{ print "}" }' < $tmpfile.sort  | json > $tmpfile.json
	[[ $? -eq 0 ]] || fatal "failed to construct json file"
	mn_tmp_routes=$(cat $tmpfile.json)
	[[ $? -eq 0 ]] || fatal "failed to cat json file"
	rm -f $tmpfile $tmpfile.sort $tmpfile.json
}

#
# Determine the set of routes for a stanza in a config file. Return that value
# in the mn_tmp_routes variable.
#
function fetch_routes
{
	local stanza a out rt
	stanza=$1
	[[ -z "$stanza" ]] && fatal "missing required stanza"
	out=
	for a in $mn_otheraz; do
		rt=$(json $stanza.$a.subnet < $mn_config)
		[[ $? -eq 0 ]] || fatal "failed to run json command for routes"
		[[ -z "$rt" ]] && fatal "empty subnet definition"
		out="$rt $out"
	done
	mn_tmp_routes=$out
}

#
# Add a new nic tag
#
function add_nic_tag
{
	local tag=$1

	[[ -n "$tag" ]] || fatal "missing nic tag"
	sdc-napi /nic_tags -f -X POST -d "{ \"name\": \"$tag\" }" \
          >/dev/null 2>&1
	[[ $? -eq 0 ]] || fatal "failed to add nic tag: $tag"
	warn "successfully added nic tag: $tag"
}

function add_network
{
	local stanza name tag
	local vlan subnet start end gateway routes resolvers
	local json croutes tmpfile

	stanza=$1
	[[ -z "$stanza" ]] && fatal "missing required stanza to add_network"
	name=$(json $stanza.network < $mn_config)
	[[ $? -eq 0 ]] || fatal "failed to get network name"
	[[ -z "$name" ]] && fatal "empty network name"
	tag=$(json $stanza.nic_tag < $mn_config)
	[[ $? -eq 0 ]] || fatal "failed to get nic tag"
	[[ -z "$name" ]] && fatal "empty nic tag"
	vlan=$(json $stanza.$mn_ouraz.vlan_id < $mn_config)
	[[ $? -eq 0 ]] || fatal "failed to get vlan id"
	[[ -z "$vlan" ]] && fatal "vlan cannot be empty"
	subnet=$(json $stanza.$mn_ouraz.subnet < $mn_config)
	[[ $? -eq 0 ]] || fatal "failed to get subnet id"
	[[ -z "$subnet" ]] && fatal "subnet cannot be empty"
	start=$(json $stanza.$mn_ouraz.start < $mn_config)
	[[ $? -eq 0 ]] || fatal "failed to get start ip"
	[[ -z "$start" ]] && fatal "start ip cannot be empty"
	end=$(json $stanza.$mn_ouraz.end < $mn_config)
	[[ $? -eq 0 ]] || fatal "failed to get end ip"
	[[ -z "$end" ]] && fatal "gateway ip cannot be empty"
	gateway=$(json $stanza.$mn_ouraz.gateway < $mn_config)
	[[ $? -eq 0 ]] || fatal "failed to get gateway ip"
	[[ -z "$gateway" ]] && fatal "gateway ip cannot be empty"

	tmpfile=/tmp/net_create.$$
	#
	# Things are a bit different depending on whether we are in the manta
	# stanza or the marlin stanza. In the former we have to assemble the
	# other routes to DCs, but don't set resolvers. In the latter, we have
	# to set resolvers and a gateway, but not routes.
	#
	if [[ "$stanza" == "marlin" ]]; then
		# Steal the external networks resolvers for now
		resolvers=$(sdc-napi /networks?name=external | \
		    json -Ha resolvers)
		[[ $? -eq 0 ]] || fatal "failed to get external resolvers"
		[[ -z "$resolvers" ]] && fatal "no external network resolvers"
		json > $tmpfile <<EOF
{
	"name": "$name",
	"vlan_id": $vlan,
	"subnet": "$subnet",
	"provision_start_ip": "$start",
	"provision_end_ip": "$end",
	"nic_tag": "$tag",
	"gateway": "$gateway",
	"resolvers": $resolvers,
	"owner_uuids": [ "$mn_ufds_adminid" ]
}
EOF
	else
		# Grab our routes to other AZs
		fetch_routes $stanza
		croutes="$mn_tmp_routes"
		mn_tmp_routes=
		if [[ -n "$croutes" ]]; then
			uniqueify_routes "$gateway" "$croutes"
			routes=$mn_tmp_routes
		else
			routes="{}"
		fi
		json > $tmpfile <<EOF
{
	"name": "$name",
	"vlan_id": $vlan,
	"subnet": "$subnet",
	"provision_start_ip": "$start",
	"provision_end_ip": "$end",
	"nic_tag": "$tag",
	"routes": $routes,
	"owner_uuids": [ "$mn_ufds_adminid" ]
}
EOF
	fi
	json=$(cat $tmpfile | json)
	[[ $? -eq 0 ]] || fatal "failed to get json for the network"
	# XXX Curl failures, seriously.
	sdc-napi /networks -X POST -d "$json"
	[[ $? -eq 0 ]] || fatal "failed to create network"
	rm -f $tmpfile
}

function validate_config
{
	[[ -z "$mn_config" ]] && fatal "missing config file"
	$mn_confchecker $mn_config || fatal "failed to validate config"
}

function tag_exists
{
	local tags found target t

	target=$1
	[[ -z "$target" ]] && fatal "missing tag to search for"
	tags=$(sdc-napi /nic_tags -f | json -Ha name)
	[[ $? -eq 0 ]] || fatal "failed to list nic tags"
	[[ -z "$tags" ]] && fatal "Got an empty list of tags"

	found=0
	for t in $tags; do
		if [[ "$t" == "$target" ]]; then
			found=1
		fi
	done
	[[ $found -eq 1 ]]
}

function network_exists
{
	local target found networks n

	target=$1
	[[ -z "$target" ]] && fatal "missing network to search for"
	networks=$(sdc-napi /networks -f | json -Ha name)
	for n in $networks; do
		if [[ "$n" == "$target" ]]; then
			found=1
		fi
	done
	[[ $found -eq 1 ]]
}

function handle_tag
{
	local section ftag

	section=$1
	[[ -z "$section" ]] && fatal "missing the section to find nic tag"
	ftag=$(json $section.nic_tag < $mn_config)
	[[ $? -eq 0 ]] || fatal "json command failed"
	[[ -z "$ftag" ]] && fatal "can't find $section's nic tag"
	if tag_exists $ftag; then
		warn "nic_tag '$ftag' already exists"
	else
		warn "nic_tag '$ftag' does not exist, creating it"
		add_nic_tag $ftag || fatal "failed to create nic tag $ftag"
	fi
}

function handle_network
{
	local section network

	section=$1
	[[ -z "$section" ]] && fatal "missing the section to look for network"
	network=$(json $section.network < $mn_config)
	[[ $? -eq 0 ]] || ftal "json command failed"
	[[ -z "$network" ]] && fatal "can't find $section's network section"
	if network_exists $network; then
		warn "network '$network' already exists"
	else
		[[ "$network" == "admin" ]] && fatal \
		     "admin network doesn't exist! hard stop."
		add_network $section
	fi
}

function setup_output_dir
{
	mkdir -p $mn_out_dir || fatal "failed to create $mn_out_dir/"
}

function fetch_az
{
	local az list

	az=$(json this_az < $mn_config)
	[[ $? -eq 0 ]] || fatal "failed to run json"
	[[ -z "$az" ]] && fatal "no az defined"
	mn_ouraz="$az"
	list=$(json azs < $mn_config | json -a)
	[[ $? -eq 0 ]] || fatal "failed to run json"
	[[ -z "$list" ]] && fatal "missing az list"
	mn_azlist=$list
	mn_otheraz=$(echo $mn_azlist | sed "s/$mn_ouraz//g")
	[[ $? -eq 0 ]] || fatal "failed to manipulate az list"
	warn "Our az list is: $mn_azlist"
	warn "preparing to setup networking for our local az: $az"
	warn "The other azs we care about are: $mn_otheraz (may be empty)"
}

#
# Grab out a gateway from the config file and return it in the mn_tmp_gw
# variable.
#
function fetch_gateway
{
	local gw stanza az
	stanza=$1
	az=$2
	[[ -z "$stanza" ]] && fatal "missing stanza for fetch_gateway"
	[[ -z "$az" ]] && fatal "missing az for fetch_gateway"
	gw=$(json $stanza.$az.gateway < $mn_config)
	[[ $? -eq 0 ]] || fatal "failed to get json gateway"
	[[ -z "$gw" ]] && fatal "no gateway defined"
	mn_tmp_gw=$gw
}

function update_admin_routes
{
	local json count routes nroutes croutes gateway
	local uroutes uuid

	# Get the admin gw
	fetch_gateway 'admin' $mn_ouraz
	gateway=$mn_tmp_gw
	mn_tmp_gw=''

	# Determine the set of routes we care about from the config
	fetch_routes 'admin'
	croutes=$mn_tmp_routes
	mn_tmp_routes=
	warn "update_admin_routes: gateway: [$gateway], routes: [$croutes]"
	# Single AZ, no routes
	[[ -z "$croutes" ]] && return

	# Grab the current admin routes
	json=$(sdc-napi /networks?name=admin -f | json -H)
	[[ $? -eq 0 ]] || fatal "failed to query napi"
	count=$(echo $json | json -a uuid | wc -l)
	[[ $? -eq 0 ]] || fatal "failed to count entries"
	[[ $count -eq 1 ]] || fatal "found more than one admin network"
	routes=$(echo $json | json -a routes)
	[[ $? -eq 0 ]] || fatal "failed to look for routes"
	# Always uniqueify
	uniqueify_routes "$gateway" "$croutes" "$routes"
	uroutes=$mn_tmp_routes
	mn_tmp_routes=
	warn "Uniqueified routes: $uroutes"
	uuid=$(echo $json | json -a uuid)
	[[ $? -eq 0 ]] || fatal "failed to get uuid out of json blob"
	[[ -z "$uuid" ]] && fatal "missing uuid for admin network"
	# XXX This probably doesn't properly catch curl errors
	sdc-napi /networks/$uuid -f -X PUT -d "{ \"routes\": $uroutes }"
	[[ $? -eq 0 ]] || fatal "failed to update napi"
}

function fetch_ufds_ids
{
	local id

	id=$(sdc-ldap search login=admin | grep ^uuid | awk '{ print $2 }')
	[[ $? -eq 0 ]] || fatal "failed to get admin uuid from ufds"
	[[ -z $id ]] && fatal "failed to find the admin uuid"
	mn_ufds_adminid=$id
	warn "UFDS admin UUID: $mn_ufds_adminid"
}

function add_tags
{
	local stanza nid nodes tag n map if_type interface
	stanza=$1
	nid=$2
	[[ -z "$stanza" ]] && fatal "missing required stanza for add_tags"
	[[ -z "$nid" ]] && fatal "mising required node id key for add_tags"
	nodes=$(json $nid < $mn_config | json -a)
	[[ $? -eq 0 ]] || fatal "failed to list nodes"
	[[ -z "$nodes" ]] && fatal "found an empty list of nodes"
	tag=$(json $stanza.nic_tag < $mn_config)
	[[ $? -eq 0 ]] || fatal "failed to get nic tag for stanza $stanza"
	[[ -z "$tag" ]] && fatal "unexpected empty nic tag"
	for n in $nodes; do
		map=$(json mac_mappings.$n.$tag \
		    nic_mappings.$n.$tag < $mn_config)
		[[ $? -eq 0 ]] || fatal "failed to get nic mapping via json"
		[[ -z "$map" ]] && fatal "empty $tag interface mapping " \
		    "for $n"

		# validate.js enforces $map to either be a JSON String or
		# Object, which represents the old and new style, respectively,
		# of defining what nics should be tagged.
		if echo $map | json --validate -q; then
			if_type=$(echo $map | json -ka)
			[[ $? -eq 0 ]] || fatal "failed to translate mapping"
			[[ -z "$if_type" ]] && fatal "unexpected empty " \
			    "interface type for nic_mappings.$n.$tag"
			interface=$(echo $map | json $if_type)
			[[ $? -eq 0 ]] || fatal "failed to translate mapping"
			[[ -z "$interface" ]] && fatal "unexpected empty " \
			    "interface value for nic_mappings.$n.$tag"
		else
			if_type="mac"
			interface=$map
		fi

		if [[ "$if_type" == "mac" ]]; then
			add_tag_to_mac $interface $n $tag
		elif [[ "$if_type" == "aggr" ]]; then
			add_tag_to_aggr $interface $n $tag
		fi
	done
}

function add_tag_to_mac
{
	local interface node tag mac nmap nres ouuid
	interface=$1
	node=$2
	tag=$3
	[[ -z "$interface" ]] && fatal "missing required interface " \
	    "for add_tag_to_mac"
	[[ -z "$node" ]] && fatal "missing required node for add_tag_to_mac"
	[[ -z "$tag" ]] && fatal "missing required tag for add_tag_to_mac"
	mac=$(echo $interface | sed -e 's/\<.\>/0&/g')
	[[ $? -eq 0 ]] || fatal "failed to translate mac"
	[[ -z "$mac" ]] && fatal "unexpected empty mac address after " \
	    "translation from $interface"
	nmap=$(echo $mac | sed -e 's/://g')
	[[ $? -eq 0 ]] || fatal "failed to translate mac"
	[[ -z "$nmap" ]] && fatal "unexpected empty napi identifier after " \
	    "translation from $mac"
	nres=$(sdc-napi /nics/$nmap | json -H)
	[[ $? -eq 0 ]] || fatal "unexpected failure reaching napi"
	# we don't check for an empty $nres here because napi may respond with
	# a 404, but also a "not found"-type object. We check if we have a 200
	# on a nic later by checking for belongs_to_uuid in the response
	ouuid=$(echo $nres | json belongs_to_uuid)
	[[ $? -eq 0 ]] || fatal "failed to get server uuid for $interface"
	# we use the existence of belongs_to_uuid in the napi response to tell
	# if we've found a nic or not. if not, our message will contain enough
	# information for an operator to manually confirm
	[[ -z "$ouuid" ]] && fatal "failed to get nic details" \
	    "for $interface at /nics/$nmap"
	[[ "$ouuid" == "$n" ]] || fatal "mapping does not match nic owner" \
	    "for $interface, expected it to be $n, found $ouuid"
	echo $nres | json nic_tags_provided | json -a | \
	    grep -q "^$tag$" && continue
	sdc-server update-nictags -s $n "${tag}_nic=$mac"
	[[ $? -eq 0 ]] || fatal "failed to add nic tag"
}

function add_tag_to_aggr
{
	local interface node tag aggr nres ouuid existing_tags update_tags
	interface=$1
	node=$2
	tag=$3
	[[ -z "$interface" ]] && fatal "missing required interface " \
	    "for add_tag_to_aggr"
	[[ -z "$node" ]] && fatal "missing required node for add_tag_to_aggr"
	[[ -z "$tag" ]] && fatal "missing required tag for add_tag_to_aggr"
	aggr=$n-$interface
	nres=$(sdc-napi /aggregations/$aggr | json -H)
	[[ $? -eq 0 ]] || fatal "unexpected failure reaching napi"
	ouuid=$(echo $nres | json belongs_to_uuid)
	[[ $? -eq 0 ]] || fatal "failed to get server uuid for $aggr"
	[[ -z "$ouuid" ]] && fatal "failed to get aggr details" \
	    "for $interface at /aggregations/$aggr"
	[[ "$ouuid" == "$n" ]] || fatal "mapping does not match aggr owner " \
	    "for $aggr, expected it to be $n, found $ouuid"
	echo $nres | json nic_tags_provided | json -a | \
	    grep -q "^$tag$" && continue
	existing_tags=$(echo $nres | json -a nic_tags_provided)
	[[ $? -eq 0 ]] || fatal "failed to get existing nic tags for $aggr"
	update_tags=$(echo $existing_tags $'\n' "[\"$tag\"]" | json -g)
	[[ $? -eq 0 ]] || fatal "failed to merge nic tags for $aggr"
	[[ -z "$update_tags" ]] && fatal "unexpected empty updated nic tags " \
	    "list for $aggr"
	# the following will update the aggregation, but we pipe the response
	# directly into `json` to confirm the response contains the requested
	# nic tag. The final conditional will protect us from non-0 exits from
	# sdc-napi, as well as any other napi http error, in which case our
	# `grep` will not match and exit non-0
	sdc-napi /aggregations/$aggr -X PUT -d "{
		\"nic_tags_provided\": $update_tags
	}" | json -H nic_tags_provided | json -a | \
	    grep -q "^$tag$" || fatal "failed to add nic tag"
}

function append_routes
{
	local routes outfile
	local keys k gw

	routes=$1
	outfile=$2
	[[ -z "$routes" ]] && fatal "missing routes object for append_routes"
	[[ -z "$outfile" ]] && fatal "missing outfile for append_routes"
	keys=$(echo $routes | json -ak)
	[[ $? -eq 0 ]] || fatal "failed to extract route keys"
	[[ -z "$keys" ]] && fatal "no keys are defined"
	for k in $keys; do
		gw=$(echo $routes | json "[ \"$k\" ]")
		[[ $? -eq 0 ]] || fatal "failed to get gateway via json"
		[[ -z "$gw" ]] && fatal "got a null gateway"
		cat >> $tmpfile <<EOF
route add -net $k $gw
[[ \$? -eq 0 ]] || exit \$SMF_EXIT_ERR_FATAL
EOF
	done
}

#
# We need to create a service for all nodes that imports the routes for the
# alternate admin networks as well as the alternate manta networks. We do not
# need to worry about the marlin network here.
#
function create_smf_route_svc
{
	local routes tmpfile net

	tmpfile="/tmp/smfroute.$$"

	cat > $tmpfile <<EOF
#!/usr/bin/bash

#
# This is the start method for the cross datacenter SMF hack service. This
# exists because we do not currently have a good way to pass additional routes
# to the global zones of either the headnode or the compute node.
#
unalias -a

. /lib/svc/share/smf_include.sh
EOF
	routes=$(sdc-napi /networks | json -Hc 'this.name === "admin"' \
	    json -a routes)
	[[ $? -eq 0 ]] || fatal "failed to get routes"
	[[ -n "$routes" ]] && append_routes "$routes" "$tmpfile"
	net=$(json manta.network < $mn_config)
	[[ $? -eq 0 ]] || fatal "failed to get network name"
	[[ -z "$net" ]] && fatal "empty network name"
	routes=$(sdc-napi /networks | json -Hc "this.name === \"$net\"" \
	    json -a routes)
	[[ $? -eq 0 ]] || fatal "failed to get routes"
	[[ -n "$routes" ]] && append_routes "$routes" "$tmpfile"
	cat >> $tmpfile <<EOF
exit 0
EOF
	cp $tmpfile $mn_out_dir/xdc-route.sh
	rm -f $tmpfile
}

function distribute_smf_route_svc
{
	local nodes n

	nodes=$(json $nid < $mn_config | json manta_nodes | json -a)
	[[ $? -eq 0 ]] || fatal "failed to list nodes"
	[[ -z "$nodes" ]] && fatal "found an empty list of nodes"
	for n in $nodes; do
		sdc-oneachnode >/dev/null -n $n 'mkdir -p /opt/custom/smf \
		    /opt/custom/mnet/bin' || fatal "failed to make directories"

		sdc-oneachnode -n $n -d /tmp -g $mn_out_dir/xdc-route.sh \
		    || fatal "failed to place routing service method"
		sdc-oneachnode -n $n 'mv /tmp/xdc-route.sh \
		    /opt/custom/mnet/bin && chmod +x \
		    /opt/custom/mnet/bin/xdc-route.sh' \
		    || fatal "failed to install xdc-route.sh"

		sdc-oneachnode -n $n -d /tmp -g ./smf/xdc-route.xml \
		    || fatal "failed to place routing service manifest"
		sdc-oneachnode -n $n 'mv /tmp/xdc-route.xml /opt/custom/smf' \
		    || fatal "failed to install xdc-route.xml"

		sdc-oneachnode -n $n 'svcs smartdc/hack/xdc-routes || svccfg \
		    import /opt/custom/smf/xdc-route.xml' \
		    || fatal "failed to import xdc-route service"
	done
}

function has_manta_ip
{
	local server net nuuid json count

	server=$1
	nuuid=$2
	[[ -z "$server" ]] && fatal "missing required server for has_manta_ip"
	[[ -z "$nuuid" ]] && fatal "missing required network uuid"
	json=$(sdc-napi /networks/$nuuid/ips?belongs_to_uuid=$server | json -H)
	[[ $? -eq 0 ]] || fatal "failed to get ip listing"
	[[ "$json" != "[]" ]]
}

#
# Reserve an IP address for a given server for the manta network by create a NIC
# for it. The nice thing about this is some day we can just drop the
# /opt/custom/smf bs and instead move to the supported version of getting that
# to exist.
#
function create_manta_nic
{
	local server net

	server=$1
	net=$2
	[[ -z "$server" ]] && fatal "missinge required server uuid"
	[[ -z "$net" ]] && fatal "missinge required network uuid"

	sdc-napi /nics -X POST -d "{
	    \"belongs_to_uuid\": \"$server\",
	    \"belongs_to_type\": \"server\",
	    \"network_uuid\": \"$net\",
	    \"owner_uuid\": \"$mn_ufds_adminid\",
	    \"reserved\": \"true\"
	}"
	[[ $? -eq 0 ]] || fatal "failed to create nic for server $server"
}

function allocate_manta_ips
{
	local nodes n net json count nuuid
	nodes=$(json $nid < $mn_config | json manta_nodes | json -a)
	[[ $? -eq 0 ]] || fatal "failed to list nodes"
	[[ -z "$nodes" ]] && fatal "found an empty list of nodes"
	net=$(json manta.network < $mn_config)
	[[ $? -eq 0 ]] || fatal "failed to get network name"
	[[ -z "$net" ]] && fatal "empty network name"
	json=$(sdc-napi /networks?name=$net | json -H)
	[[ $? -eq 0 ]] || fatal "failed to get json data"
	count=$(echo $json | json -a uuid | wc -l)
	[[ $? -eq 0 ]] || fatal "failed to count uuids"
	[[ $count -eq 1 ]] || fatal "didn't find exactly one $net network"
	nuuid=$(echo $json | json -a uuid)

	for n in $nodes; do
		has_manta_ip $n $nuuid || create_manta_nic $n $nuuid
	done
}

function node_has_manta_nic
{
	local server tmpfile

	server=$1
	[[ -z "$server" ]] && fatal "missing required server argument"
	tmpfile="/tmp/dladm.nodes.$$"

	sdc-oneachnode -n $server "dladm show-vnic -po link > $tmpfile" \
	    || fatal "failed to generate list of vnics"
	sdc-oneachnode -n $server -p $tmpfile -d /tmp \
	    || fatal "failed to grab the vnic list"
	grep -q "^$mn_manta_vnic$" /tmp/$server
}

function setup_manta_nic
{
	local server
	local nuuid nic net nuuid ip tag mac vlan netmask
	local outfile

	server=$1
	[[ -z "$server" ]] && fatal "missing server for setup_manta_nic"
	outfile="$mn_out_dir/$server-manta-nic.sh"

	#
	# We need the nic on the manta network. We can do that by searching for
	# our ip and then filtering the nics for this server on that ip.
	#
	net=$(json manta.network < $mn_config)
	[[ $? -eq 0 ]] || fatal "failed to get network name"
	[[ -z "$net" ]] && fatal "empty network name"
	json=$(sdc-napi /networks?name=$net | json -H)
	[[ $? -eq 0 ]] || fatal "failed to get json data"
	count=$(echo $json | json -a uuid | wc -l)
	[[ $? -eq 0 ]] || fatal "failed to count uuids"
	[[ $count -eq 1 ]] || fatal "didn't find exactly one $net network"
	nuuid=$(echo $json | json -a uuid)
	[[ $? -eq 0 ]] || fatal "failed to get out the uuid"
	[[ -z "$nuuid" ]] && fatal "missing network uuid"
	json=$(sdc-napi /networks/$nuuid/ips?belongs_to_uuid=$server | json -H)
	[[ $? -eq 0 ]] || fatal "failed to fetch json data"
	count=$(echo $json | json -a ip | wc -l)
	[[ $? -eq 0 ]] || fatal "failed to count uuids"
	[[ $count -eq 1 ]] || fatal "didn't find exactly one $net network"
	ip=$(echo $json | json -a ip)
	[[ $? -eq 0 ]] || fatal "failed to run json to get the ip"
	[[ -z "$ip" ]] && fatal "somehow got an empty ip"
	json=$(sdc-napi /nics?belongs_to_uuid=$server | json -Hc \
	    "this.ip == \"$ip\"")
	[[ $? -eq 0 ]] || fatal "failed to get nics"
	[[ -z "$json" ]] && fatal "somehow found an empty ip"

	vlan=$(echo $json | json -a vlan_id)
	[[ $? -eq 0 ]] || fatal "failed to get vlan"
	[[ -z "$vlan" ]] && fatal "found empty vlan"

	tag=$(echo $json | json -a nic_tag)
	[[ $? -eq 0 ]] || fatal "failed to get nic_tag"
	[[ -z "$tag" ]] && fatal "found empty nic_tag"

	mac=$(echo $json | json -a mac)
	[[ $? -eq 0 ]] || fatal "failed to get mac"
	[[ -z "$mac" ]] && fatal "found empty mac"

	netmask=$(echo $json | json -a netmask)
	[[ $? -eq 0 ]] || fatal "failed to get netmask"
	[[ -z "$netmask" ]] && fatal "found empty netmask"

	cat > $outfile <<EOF
#!/bin/bash

#
# Create a vnic to run on the manta network. This is a hack, don't pretend it's
# not. Lasciate ogni speranza voi che entrate.
#
set -o pipefail
set -o xtrace

mn_nictag="$tag"
mn_vnname="$mn_manta_vnic"
mn_vmac="$mac"
mn_ip="$ip"
mn_subnet="$netmask"
mn_vlan="$vlan"
mn_link=

. /lib/svc/share/smf_include.sh

function fatal
{
        local msg="\$*"
        [[ -z "\$msg" ]] && msg="failed"
        echo "\$mn_arg0: \$msg" >&2
        exit \$SMF_EXIT_ERR_FATAL
}

#
# If we're on a system with boot-time modules, then we need to verify that it
# hasn't beaten us to the punch. If it has, then we basically disable ourselves
# if it's already gotten here. In addition, if we're disabling ourselves, then
# we need to also disable the dependent xdc-routes script; however, it may not
# always exist. Therefore, we don't explicitly fail if we fail to disable it.
#
function check_bootime
{
	if dladm show-vnic \$mn_vnname >/dev/null 2>/dev/null; then
		svcadm disable svc:/smartdc/hack/xdc-routes:default
                svcadm disable \$SMF_FMRI
                exit 0
	fi
}

check_bootime
mn_link=\$(nictagadm list \
  | awk "{ if (\\\$1 == \"\$mn_nictag\") { print \\\$3 } }")
[[ \$? -eq 0 ]]  || fatal "failed to get link for tag \$mn_nictag"
[[ -z "\$mn_link" ]] && fatal "empty link name"
if [[ \$mn_vlan -eq 0 ]]; then
	mn_vlan=
else
	mn_vlan="-v \$mn_vlan"
fi
dladm create-vnic -t -l \$mn_link -m \$mn_vmac \$mn_vlan \$mn_vnname
[[ \$? -eq 0 ]] || fatal "failed to create nic"
ifconfig \$mn_vnname plumb up || fatal "failed to bring up \$mn_vnname"
ifconfig \$mn_vnname \$mn_ip netmask \$mn_subnet || fatal "failed to assign ip"

#
# Update sysinfo and then tell CNAPI to refresh it.  This is necessary for
# other parts of Manta (e.g., the marlin dashboard configurator) to see the
# newly created VNIC.  These steps are both best-effort.  If CNAPI is
# temporarily down, we don't want to stop this service from coming up.
#
server_uuid=\$(sysinfo -f | json UUID) &&
    sdc-cnapi /servers/\$server_uuid/sysinfo-refresh -X POST

exit \$SMF_EXIT_OK
EOF
	[[ $? -eq 0 ]] || fatal "failed to generate init script"

	sdc-oneachnode -n $server 'mkdir -p /opt/custom/mnet/bin \
	    /opt/custom/smf' \
	    || fatal "failed to create necessary directories"

	# Install it on the node
	sdc-oneachnode -n $server -d /tmp -g $outfile \
	    || fatal "failed to copy over manta-nic.sh"
	sdc-oneachnode -n $server "mv /tmp/$(basename $outfile) \
	    /opt/custom/mnet/bin/manta-nic.sh && chmod +x \
	    /opt/custom/mnet/bin/manta-nic.sh" \
	    || fatal "failed to install manta-nic.sh"

	sdc-oneachnode -n $server -d /tmp -g ./smf/manta-nic.xml \
	    || fatal "failed to copy over manta-nic.sh"
	sdc-oneachnode -n $server 'mv /tmp/manta-nic.xml /opt/custom/smf' \
	    || fatal "failed to install manta-nic.xml manifest"
	sdc-oneachnode -n $server 'svccfg import \
	    /opt/custom/smf/manta-nic.xml' \
	    || fatal "failed to import service manifest"

	# Update sysinfo
	sdc-oneachnode -n $server 'sysinfo -fu' \
	    || fatal "failed to have server update sysinfo"
	sdc-cnapi /servers/$server/sysinfo-refresh -X POST \
	    || fatal "failed to refresh sysinfo in cnapi"
}

#
# For each node make sure we have a nic in place on the node already to handle
# this.
#
function handle_manta_nics
{
	local n nodes

	nodes=$(json $nid < $mn_config | json manta_nodes | json -a)
	[[ $? -eq 0 ]] || fatal "failed to list nodes"
	[[ -z "$nodes" ]] && fatal "found an empty list of nodes"
	for n in $nodes; do
		node_has_manta_nic $n || setup_manta_nic $n
	done
}

#
# resolve_path: given a relative path name, resolve it to a full path.
#
function resolve_path
{
	local rdir rbase absdir
	rdir="$(dirname $1)"
	rbase="$(basename $1)"
	absdir=$(cd "$rdir" 2>/dev/null && echo $PWD) || return 1
	echo "$absdir/$rbase"
}

if [[ $# -ne 1 ]]; then
	fatal "<config.json>"
fi
mn_config=$(resolve_path $1) || fatal "configuration file not found"

mn_dir="$(dirname $0)"
cd "$mn_dir" || fatal "failed to cd to \"$mn_dir\""

validate_config || fatal "failed to validate config"
fetch_az || fatal "failed to determine our AZ"
fetch_ufds_ids || fatal "failed to fetch ufds admin id"
handle_tag 'admin' || fatal "failed to handle tag for admin nic_tag"
handle_tag 'manta' || fatal "failed to handle manta nic_tag"
handle_tag 'marlin' || fatal "failed to handle marlin nic_tag"

handle_network 'admin' || fatal "failed to handle admin network"
handle_network 'manta' || fatal "failed to handle manta network"
handle_network 'marlin' || fatal "failed to handle marlin network"

update_admin_routes || fatal "failed to fix up admin networks"

add_tags 'manta' 'manta_nodes' || fatal "failed to add manta nic tag to CNs"
add_tags 'marlin' 'marlin_nodes' || fatal "failed to add marlin nic tag to CNs"

setup_output_dir || fatal "failed to setup output directory"
allocate_manta_ips || fatal "failed to allocate ips for manta nics for GZs"
handle_manta_nics || fatal "failed to create and setup manta nics in GZs"

if [[ -n "$mn_otheraz" ]]; then
	create_smf_route_svc || fatal \
	    "failed to create smf routing hack service"
	distribute_smf_route_svc || fatal \
	    "failed to distribute smf routing hack service"
fi

exit 0
