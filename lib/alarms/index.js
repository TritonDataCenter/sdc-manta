/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * Manta Alarm Management
 *
 *
 * INTRODUCTION
 *
 * Manta uses Triton's Amon facilities to define checks and other conditions
 * that will raise alarms and notify operators when problems arise.  Operators
 * are expected to run "manta-adm" during deployment to configure Amon with all
 * of the expected checks.  (Other than running the command itself, this process
 * is fully automatic.)  Later, when alarms are opened in response to failures,
 * operators also use "manta-adm" to list alarms, fetch details about them,
 * suspend notifications in cases of known issues, and ultimately close alarms
 * for which the underlying issue is believed to be resolved.
 *
 * Amon is configured in terms of probes.  Most probes are either commands that
 * run periodically or log file watchers that continuously monitor the contents
 * of log files.  Each probe is attached to a specific instance, which is either
 * a Triton VM or a Triton CN.  Of course, the set of VMs and CNs used for Manta
 * is not static, and certainly not known at build time, so probes must be
 * dynamically generated based on metadata (which is stored in this repository)
 * and the set of components actually deployed at any given time.  This process
 * is described in more detail below.
 *
 * Within Amon's configuration, probes are gathered into probe groups, which are
 * mainly useful because they define how the corresponding alarms get organized.
 * When multiple probes in the same group fail, those failures are generally
 * collected into a single alarm.  That's useful to group multiple instances of
 * the same problem (e.g., multiple "webapi" components reporting the same
 * error).  Unlike previous versions of this software, distinct failure modes
 * generally result in different alarms.  This makes it possible to silence
 * individual alarms corresponding to known issues without squelching
 * notifications about new issues as well.
 *
 *
 * LOCAL METADATA AND KNOWN FAILURE MODES
 *
 * The metadata contained in this repository enumerates all of the known Manta
 * failure modes, defines checks for identifying them, and provides useful
 * information for an operator about each one.  Inspired by the illumos Fault
 * Management Architecture ("FMA"), the metadata describes one or more _event
 * classes_, each having a unique name in a hierarchical, dot-delimited
 * namespace.  Each of these corresponds to a distinct failure mode.  For
 * example, the event:
 *
 *     upset.manta.loadbalancer.no_backends
 *
 * indicates that a loadbalancer has no available backends.  ("upset" is an
 * existing top-level FMA event class that covers soft errors.  Since we expect
 * future events to fall into the same bucket of Manta-related soft errors, we
 * currently validate that all event names start with "upset.manta".  There's no
 * intrinsic reason that new events must start with this prefix, and this
 * subsystem should not assume that they do except for validation purposes.  If
 * you're looking to add events in different top-level classes, make sure you
 * understand how they fit into the broader FMA event schema.)
 *
 * In our model, these event classes correspond one-to-one with Amon probe
 * groups.
 *
 * For operator-visible events like these, FMA supports the idea of _knowledge
 * articles_, which provide content written for operators that describes a
 * problem's severity, impact, any automated response (if any), and suggested
 * action.
 *
 * Putting all this together: each piece of metadata defined in this repository
 * is called a _probe template_.  Each template describes a known Manta failure
 * mode, a list of checks for identifying it, and knowledge article content.
 * Specifically, each template has:
 *
 *     - an FMA event class name unique to this failure mode.  This is used as a
 *       primary key to refer to this particular failure mode.
 *
 *     - a scope, which describes what kinds of components this template applies
 *       to (e.g., "loadbalancer" zones)
 *
 *     - a list of checks for identifying this failure mode.  These are used to
 *       create Amon probes to detect this failure.
 *
 *     - knowledge article content
 *
 * The specific format is described in lib/alarms/metadata.js.
 *
 * As an example, the aforementioned event has FMA event class
 * "upset.manta.loadbalancer.no_backends".  Its scope would be "loadbalancer",
 * and it would define a check script to run in each loadbalancer zone to count
 * the backends and fail if the count is zero.  To implement this, the
 * deployment tooling creates one probe group for the failure mode itself and
 * a probe to run the check _for each_ loadbalancer instance.
 *
 * More sophisticated configurations are also possible.  See the comments in
 * config.js for details.
 *
 * FMA supports a sophisticated system of telemetry, diagnosis, reporting, and
 * retirement of faulty components.  We only use the concepts of events, the
 * event hierarchy, and knowledge articles, and this implementation shares no
 * code with FMA itself.
 *
 *
 * IMPLEMENTATION OVERVIEW
 *
 * Putting this together, there are basically three sources of information
 * related to probes and alarms:
 *
 *    (1) The list of instances of each component that are currently deployed.
 *        This includes the lists of SAPI instances, VMs, and CNs, and the
 *        information comes from SAPI, VMAPI, and CNAPI.
 *
 *    (2) Local metadata that describes the probe groups and probes that should
 *        exist in a Manta deployment.  This metadata also includes knowledge
 *        articles that provide additional information for the operator for each
 *        failure mode (like instructions about how to respond to various types
 *        of alarms).
 *
 *    (3) The list of probes and probe groups that are actually deployed, and
 *        the list of open alarms and the events associated with each alarm.
 *        This comes from Amon, but Amon only knows about its own agents, which
 *        have uuids corresponding to VM and CN uuids.  To make sense of this
 *        information, it has to be at least joined with the list of components
 *        deployed, but likely also the local metadata associated with probe
 *        groups.
 *
 *        This source can be split further into the list of alarms and probe
 *        groups and (separately) the list of probes.  The list of probes is
 *        much more expensive to gather, and is only necessary when
 *        verifying or updating the Amon configuration.
 *
 * Using this information, we want to support a few different stories:
 *
 *    (1) List open alarms or detailed information about specific alarms.
 *        ("manta-adm alarm show" and related commands)
 *
 *        We want to present the list of known, active problems.  This is the
 *        list of open alarms, which we can fetch from Amon.  We want to
 *        associate each problem with the affected components using their
 *        service names.  That requires joining the "machine" that's provided
 *        in each fault with the information we fetched separately about
 *        deployed VMs and CNs.  We also want to provide knowledge article
 *        content about each alarm by joining with the local configuration,
 *        based on the alarm's probe group name.
 *
 *    (2) List configured probes and probe groups.
 *        ("manta-adm alarm config show" and related commands)
 *
 *        It's useful for operators to see what probes have been configured.
 *        This involves fetching probes and probe groups from Amon and combining
 *        that information with the local knowledge articles for each one and
 *        possibly the list of VMs and CNs deployed.
 *
 *    (3) Update the probe and probe group configuration.
 *        ("manta-adm alarm config verify", "manta-adm alarm config update")
 *
 *        For both initial deployment and subsequent updates, it's important to
 *        have an idempotent operation that compares what probes and probe
 *        groups are supposed to be configured with what's actually deployed and
 *        then updates the deployment to match what's expected.  This also
 *        involves joining all three sources of information.
 *
 * Adding to the complexity, there are several other types of probes or probe
 * groups that we may encounter:
 *
 *     - Probes and probe groups added by operators for their own custom
 *       monitoring.  This is fully supported, though it cannot be configured
 *       using the Manta tools.  We present these as best we can -- using
 *       whatever metadata is in the probe groups rather than knowledge article
 *       information.
 *
 *     - Probes and probe groups added by previous versions of this software
 *       before any of the local metadata was provided.  These groups are
 *       explicitly deprecated: we want operators to move away from them because
 *       they're very hard to use.  For display, we treat these like probes and
 *       probe groups that operators added, where we have no local knowledge
 *       article information about them.  For update, we'll remove these
 *       altogether, since they're replaced by other probes and groups that we
 *       deploy.
 *
 *     - Other probes and probe groups added by other versions of this software
 *       (either older or newer) that had local metadata at the time.  We can
 *       distinguish these because of the way probe groups are named.  We treat
 *       older objects similar to probes and probe groups that were added before
 *       this metadata was available: we'll consider them removable during
 *       "manta-adm alarm config verify/update".  We'll ignore newer objects.
 *
 * The implementation of these facilities is divided into:
 *
 *     - lib/alarms/index.js (this file): general documentation and symbols
 *       exported from this subsystem.
 *
 *     - lib/alarms/alarms.js: defines data structures and functions for
 *       managing alarms themselves.
 *
 *     - lib/alarms/amon_objects.js: defines classes and loaders for low-level
 *       Amon objects, including input validation.
 *
 *     - lib/alarms/config.js: defines data structures and functions for
 *       managing Amon configuration (namely, a set of probes and probe groups).
 *       This includes functions that fetch probes and probe groups from Amon,
 *       and classes for walking these structures for the purpose of verifying
 *       or updating the configuration.
 *
 *     - lib/alarms/metadata.js: defines data structures and functions for
 *       working with the locally provided metadata for known failure modes.
 *
 *     - lib/alarms/update.js: defines data structures and functions for
 *       updating the Amon configuration.  This includes functions for comparing
 *       two sets of configuration (usually a "deployed" configuration and a
 *       "desired" configuration), generating a plan to move from one to
 *       another, and applying that plan.
 */

var alarm_metadata = require('./metadata');
var alarm_alarms = require('./alarms');
var alarm_config = require('./config');
var alarm_update = require('./update');

/* Exported interfaces */

/* Alarms */
exports.amonLoadAlarmsForState = alarm_alarms.amonLoadAlarmsForState;
exports.amonLoadAlarmsForIds = alarm_alarms.amonLoadAlarmsForIds;
exports.amonCloseAlarms = alarm_alarms.amonCloseAlarms;
exports.amonUpdateAlarmsNotification =
    alarm_alarms.amonUpdateAlarmsNotification;

/* Configuration */
exports.amonLoadProbeGroups = alarm_config.amonLoadProbeGroups;
exports.amonLoadComponentProbes = alarm_config.amonLoadComponentProbes;
exports.amonConfigSummarize = alarm_config.amonConfigSummarize;

/* Configuration updates */
exports.amonUpdatePlanCreate = alarm_update.amonUpdatePlanCreate;
exports.amonUpdatePlanSummarize = alarm_update.amonUpdatePlanSummarize;
exports.amonUpdatePlanApply = alarm_update.amonUpdatePlanApply;

/* Local metadata */
exports.loadMetadata = alarm_metadata.loadMetadata;
