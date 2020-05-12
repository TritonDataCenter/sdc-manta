# sdc-manta changelog

# 1.7.3

- MANTA-5252 Fix `mantav2-migrate snaplink-cleanup` to work with rack-aware
  networking (RAN) usage on webapi instances.

# 1.7.2

- MANTA-5206 On Manta's more than 7 years old, it is possible to have
  snaplinks that surprised "delink" script generation in the
  'mantav2-migrate snaplink-cleanup' command. This is fixed now.

# 1.7.1

- MANTA-5154 'mantav2-migrate snaplink-cleanup' fix when there is a moray
  index shard with no snaplinks.

# 1.7.0

- MANTA-5142, MANTA-5154 mantav2-migrate fixes.
- MANTA-4874 Add 'mantav2-migrate snaplink-cleanup' command for driving
  snaplink removal, a major part of mantav2 migration.

# 1.6.1

- MANTA-5127 manta-init should check its command line arguments

# 1.6.0

- MANTA-5113 Add garbage-collector to developer coal/lab deployments using
  `manta-deploy-dev`.

# 1.5.2

- MANTA-4708 create tools for cleaning up snaplinks

# 1.5.1

- MANTA-5072 'picker' is missing from mLegacySvcNames

# 1.5.0

- MANTA-4965 manta-deploy-lab should support multiple servers
- MANTA-4969 correct amon probes for loadbalancer
- MANTA-4957 deploy buckets and rebalancer infra by default with manta-deploy-coal/lab

# 1.4.0

- MANTA-4861 manta-adm should have a -C option to override the channel

# 1.3.1

- MANTA-4920 fix and doc Manta service deployment ordering (#45)
- MANTA-4628 Rename extant buckets components such that the name reflects the function (#41)
- MANTA-4909 manta-factoryreset does not handle removing 'dclocalconfig' on the poseidon account (#44)
- MANTA-4931 Can't remove electric-boray if electric-boray is not a valid name

# 1.3.0

- MANTA-4881 make picker a first class Manta component

# 1.2.0

- MANTA-4533 make 'rebalancer' a first class Manta service

# 1.0.0

- Update to node v6 and bump a number of deps in the process.

# 0.0.7

older releases
