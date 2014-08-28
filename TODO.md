<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

# On-prem Manta Standup Notes

These notes arise mainly from the C8000 demo standup in the Engineering
lab in Emeryville.  For reference, this is a 3-node standup: a single
HN, a single CN for infrastructure, and a single storage node.  The
standup was done using the same number of instances of each service as
the lab deployment scripts provide.  Only the networking setup was
different, along with deployment of each instance onto an explicit CN in
manta-deploy-*.

## Multi-node configuration

Currently if you want to stand up a Manta with components on more than
one CN (which must also be the HN), you're more or less on your own.  It
would be nice to have a mechanism into which one feeds a few basic
pieces of information for each CN one wishes to use, producing a JSON
blob containing the complete assignment of instances to CNs and NICs.
This JSON blob could be consumed by monitoring tools if desired (see
below), but more importantly could be fed to tools like deploy-manta-XXX
to manage the deployment of a production-scale service.

The minimal inputs for each CN seem to be:

- UUID or other unique identifier resolvable to UUID
- NIC tag to MAC address mappings (alternately, NIC tag to some physical
  descriptor mapping such as slot number and PCI function ID, but
  practically speaking most people probably won't be any better off than
  with the MACs)
- intended use (storage vs. infrastructure); this could be finer-grained
  if really needed but the only reasons for that would be more than 2
  types of HW which we will never support or connectivity limitations on
  certain CNs which we shouldn't support either.

The tool would be responsible for allocating instances to CNs based on
those CN inputs and a few parameters describing the desired scale of the
system -- number of shards in particular -- to avoid duplicating
services on a single node if possible given the constraints.

While manually changing these mappings is not terribly difficult in a
small-scale installation, it becomes more error-prone at larger scale.

## Setup Checkpoints

This set of suggestions comes mainly from Mr. Clulow.

One frustrating aspect of the standup is that it is possible to deploy a
large number of services without any feedback.  Even if instances
deployed earlier are misconfigured or failed to run properly
post-provisioning, the existing deployment tools will drive on.  This
can result in a lot of wasted time if an error was made in an early
phase; this manifested itself on my first try as a single spurious
Zookeeper instance in SAPI metadata; the instance was never provisioned,
but other ZKs refused to work properly.  Nevertheless, it was possible
to complete all deployments without any apparent errors.

It would be nice if there were some way (even just a documented set of
steps to perform manually) to validate the state of the system at
important milestones during deployment, before continuing.

Filed tickets:

- MANTA-1930 Create health checklist for each Manta service
- MANTA-1931 Create manta deployment hooks so that services are verified healthy
  before more deployments

## Hard-coded emoray Rings

Currently the emoray configuration is given as a leveldb blob.  This
blob is tied to both the size (number of shards and CNs) and the name of
the Manta installation.  This requires either that a new blob be
generated for every installation or that every installation have the
same name and size.  Neither seems tenable...

Suggestions:

- Remove the domain suffix from the instance names in the blob, leaving
  only the prefixes.  This would allow each ring blob for a given size
  installation to be reused for every new installation of that same
  size.  The Manta name can be pulled from metadata and appended by the
  consumer after retrieving the instance name from emoray.
- Pre-generate several blobs for installation sizes we believe will be
  common or that we want to offer as prepackaged options.

This issue is basically the same as MORAY-143, in that it deals with how
the ring is generated and stored.

Short term fix:

- MANATEE-141 Ring topologies in emoray should only contain prefixes

## Zone Sizing

The zone sizing appears to be wrong for a number of the services.  In
particular:

- haproxy in the moray zones was repeatedly bumping up against the
  zone's 256 MB cap, causing moray to die (we have a core, but it's
  useless because of DATASET-923; see also joyent/pkgsrc#121)
- the jobsupervisor zones were capped at 256 MB but have bumped against
  that cap repeatedly although they do not seem to be using much more:

<pre>
 e63a40ab-afc1-49b7-a33d-d7d41a39fbde      285     4096    17317 25087
 00ce82e0-a597-4a6a-9b53-069dee4606a6      138     4096    17677 24900
</pre>

- muskies use considerably more than their standard allocation as well:

<pre>
 a738e8be-1794-4da2-ba9b-b5195967e36a     1211     4096        0 0
 ff7347a6-d294-43ac-b8a0-a44fd41f81c5     1152     4096        0 0
</pre>

These issues require investigation, with bugs filed either against
whatever is consuming more memory than expected or against the
deployment tools for allocating caps that are too low.  The cause is not
necessarily obvious; prod, which is similar in scale and layout, uses
drastically higher caps, while the lab deploy, using the same scripts as
this deployment, seems to cope fine with the lower caps.

Filed:

- MANTA-1933 Zone sizing for new manta standups are *way* off.

## Log Overflow

If log upload into manta doesn't work, there is a horrible cascading
failure mode:

- logs grow rapidly because of whatever failure is causing manta not to
  work in the first place
- logs can't be uploaded, so they accumulate in /var/log/manta/upload
- the logs exhaust the disk quota, causing further components to fail
  and hindering debugging

Not sure it's worth filing a bug on this since I expect to be told that
if Manta is down, that's the real problem and fallout from that is not
interesting.

## Inconsistent Logs

Different services log into different places.  Since the main mechanism
for understanding Manta failures is examining log files, this is a
significant annoyance.  The two main approaches seem to be:

- Use the service log via SMF
- Pipe through rsyslog into something in /var/log/service.log

This appears to be the result of an agree to disagree decision we made
during development; see MANTA-913, MANTA-1502, etc.  I'm not sure where
the decision is documented but I remember it being made during a
meeting.  Having been on the receiving end of this, I can't imagine
customers or Support finding this acceptable.

There are a few things to come out of this:

- MANTA-1936 Rip out syslog logging
- MANTA-1935 Use NFS mount for log uploading to Manta
- MANTA-1933 Zone sizing for new manta standups are *way* off. (if the zones are
  bigger, then they won't cause cascading failures).

## Redundant Log Entries

Repeated strings of log entries identical other than for timestamp
should be rolled up to save space on disk and time when manually
reviewing log entries.  This may be somewhat complicated with respect to
logs used for billing.

## Inscrutable Log Entries

A number of the above issues encountered during bringup were diagnosed
by examining the log files; however, the content of the logs was usable
only by Engineering.  The best example here is "the nscd issue", except
that the only bug ID that seems relevant is OS-2148 which is already
fixed.  The log messages in this case were of the form "ERROR: name
service lookup attempted for ..." which does not indicate what the error
actually was.

## Factory Reset Doesn't

This is really only a problem if you encounter other issues, such as the
ZK one described above.  See MANTA-1790, which tracks not only the issue
observed here (not cleaning up marlin agent configuration) but others as
well.  The manifestation in the case of the marlin agent in this case
was that the agent spewed errors about connections to IP addresses that
are on the same subnets but different from any address assigned to an
instance.  Obviously, arbitrary divergence is possible depending on what
one has changed since the previous configuration.

## SAPI Manifest Divergence

It's not possible to deploy versions of certain components that do not
match the version of the manta-deployment zone.  This is because the
SAPI manifest is contained within the manta-deployment zone itself.  The
manifestation observed in this case was that manatee-sitter refused to
start, throwing an exception on an undefined property of a configuration
object generated from the template.  See MANATEE-136.

The goal is to get rid of manta-deployment.git/manifests.  As of this writing,
there are ~8 services that have registrar configs in manta-deployment, a
manifest for muppet and a manifest for the manatee sitter.  The latter two are
needed because of the differences between sdc and manta deployments.

## Get rid of hack svcs

```
[root@headnode (us-east-3) ~]# sdc-oneachnode -n RM08212 "svcs | grep hack"
=== Output from 00000000-0000-0000-0000-002590935960 (RM08212):
online         Oct_24   svc:/smartdc/hack/manta-nic:default
online         Oct_24   svc:/smartdc/hack/xdc-routes:default
```

## Mantamon

Needs to work x-dc.  That's MANTA-1833.

# Addressed Issues

## Networking Setup Assumptions

The networking setup scripts (which create and assign nic tags) assume
that the manta and mantanat networks are associated with tagged VLANs on
the same physical device as the external nic tag.  In this environment,
that assumption does not hold; the admin nic tag is associated with the
same device as the tagged manta and mantanat tags, while the external
tag is associated with a different device.  This needs to be made more
flexible, as described in part above.

In addition, when changing the nic tags to account for this, we
encountered NET-230.  A workaround using NAPI directly along with
nictagadm on each CN got us past this.

Filed:

- MANTA-1932 manta-deployment.git/networking/manta-net.sh should allow more
  flexible nic tag assignments
