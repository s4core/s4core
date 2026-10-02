# Federation Erasure Coding (Enterprise)

S4 Federation Erasure Coding (EC) is the Enterprise-only storage-class
foundation for lower-overhead distributed pools. It is configured with
`S4_POOL_TYPE=ec` in federation cluster mode and is guarded by the EE
`erasure_coding` license capability.

> **Current v1 runtime status:** EC pool validation, fixed-width erasure-set
> metadata, codec/runtime limits, staged transcode, EC cutover reads, repair,
> anti-entropy, scrub, tombstone/orphan GC, hot-copy purge gates, metrics, and
> the normal S3 hot RF=3 object path are wired in the Enterprise runtime.
> Admin health separates `runtime_ready` from `production_ready`:
> `runtime_ready=true` means the local runtime workers and data paths are
> active, while `production_ready=true` also requires an EE build marked as
> accepted for production with `S4_EC_V1_CI_ACCEPTANCE_PASSED=true`.

## Requirements

- Enterprise binary: `cargo build --release --features enterprise`
- Valid Enterprise license with `erasure_coding=true`
- `S4_MODE=cluster`
- `S4_POOL_TYPE=ec`
- One immutable fixed-width pool per EC erasure set
- `S4_POOL_NODES` count exactly equal to the selected profile's total shards

CE builds and unlicensed EE builds reject `S4_POOL_TYPE=ec`.

> A starting node has to **name** every node of its pool, not see it. Identities it cannot get
> from gossip come from what it recorded the last time that address answered, so a node starts
> while its neighbours are down and a rolling restart works with one node dead. See
> [Starting while the pool is short a node](#starting-while-the-pool-is-short-a-node).

## Model

An EC pool is a federation pool with a fixed-width erasure set. In v1, one EC
pool has one active erasure set. Pool membership is immutable after startup:
scale-out means adding a new pool/set and migrating data in a future workflow,
not adding nodes to an existing set.

The profile defines:

- `k`: data shard count
- parity shards: RS `m`, or LRC local/global parity
- total shard slots: `k + parity`
- minimum normal read: `k`
- manifest quorum: majority of total shard slots

Objects below `S4_EC_MIN_OBJECT_SIZE` are modeled as explicitly replicated
small objects inside the EC pool. Larger objects are modeled as chunk manifests
with per-shard CRC32/SHA-256 authority.

"Explicit" means the decision is recorded, not inferred. A small object has no
manifest — the coding step never runs for it — so the placement itself is the
record, and it is written to the three nodes that hold the object's replicas
rather than only to the node that took the write. Kept on the coordinator alone
it would be indistinguishable from the implicit "no EC manifest" fallback the
layout exists to replace: lose that one node and the rest of the pool would have
nothing saying the object was placed deliberately. A node that has never
resolved the object reads the record from the pool the first time it is asked,
and keeps it.

**The placement publisher targets a majority of the replicas.** The `PUT`
does not currently wait for that separate metadata majority: its data write
has already reached quorum, and the placement shortfall becomes a local note
(`s4_ec_small_placement_hints_total{outcome="recorded"}`). The anti-entropy tick
delivers what is owed and clears the note; a note whose object has since been
deleted is dropped with it. Loss of this coordinator and its disk before
delivery can lose the explicit placement fact even though data replicas
survive. Do not treat the local hint as proof of replicated placement durability.

Hint delivery advances a separate durable cursor after every examined page,
including a page whose recipients cannot be reached, then starts a new round.
Restart may repeat a page, but does not permanently pin the sweep to its head.
Clearing/updating a hint compares the observed snapshot atomically with the
stored one; delayed acknowledgements and deletes cannot clear a newer write's
debt. The cursor certifies examination, not successful delivery.

**Reads ask the object's own replicas for the record**, which is a computation
over the ring rather than a question anybody has to be asked, so a resolve costs
RF requests and not one per node in the pool. Falling back to the whole pool
happens when no owner returned a decodable record. Every nonempty fallback is
counted before its I/O in `s4_ec_small_placement_fallback_attempts_total`, including
a complete miss. `s4_ec_small_placement_resolves_total{source="pool_fanout"}` counts
only records actually found there, not all attempts. Both metrics are registered
at zero; an absent series is not equivalent to zero. An object with no
record at all is remembered as such for
`S4_EC_SMALL_PLACEMENT_NEGATIVE_TTL_SECS`, so that the objects the hot path is
simply still holding — which answer "no manifest" on exactly this path — do not
pay a resolve on every read.

## Current Release Gates

The EC admin health surface reports release gates so operators can see what is
wired at runtime and what still blocks final production readiness:

| Gate | Current status |
|------|----------------|
| EC pool/profile/topology validation | Implemented |
| Manifest-authoritative shard hashes | Implemented in foundation |
| Local metadata miss quorum read | Implemented in foundation |
| Corrupt shard no-client-leak behavior | Implemented in foundation |
| Bounded repair/GC resources | Implemented in foundation |
| Repair worker runtime | Wired when `S4_EC_REPAIR_WORKER_ENABLED` is enabled |
| Anti-entropy/scrub runtime | Wired when AE or scrub workers are enabled |
| S3 EC read/write path | Wired: hot RF=3 ACK path, EC read after cutover |
| Transcode worker runtime | Wired for staged hot-to-EC conversion |
| Multipart staged EC | Wired through hot RF=3 completion and staged EC metadata |
| Orphan physical shard purger | Wired through admin GC and runtime ports |
| Build accepted for production (`S4_EC_V1_CI_ACCEPTANCE_PASSED`) | Required for `production_ready=true` |
| Inline EC writes | Disabled by default |
| Auto-tiering | Disabled by default |
| Multi-set pools | Disabled by default; adding a set needs every pool node on EC v2 |

## Built-In Profiles

| Profile | Layout | Required pool nodes | Storage overhead | Notes |
|---------|--------|---------------------|------------------|-------|
| `ec-rs-small` | RS(4,2) | 6 | 1.50x | Smallest RS pool |
| `ec-rs-standard` | RS(6,3) | 9 | 1.50x | Default profile |
| `ec-rs-dense` | RS(8,3) | 11 | 1.375x | Lower overhead, wider set |
| `ec-lrc-small` | LRC(4,2,1) | 7 | 1.75x | Local repair groups |
| `ec-lrc-standard` | LRC(6,2,2) | 10 | 1.67x | Balanced LRC profile |
| `ec-lrc-dense` | LRC(10,2,2) | 14 | 1.40x | Dense LRC profile |

`S4_EC_PROFILE=custom` supports custom RS and LRC profiles. LRC v1 requires
`S4_EC_K` to be divisible by `S4_EC_LRC_L`.

## Environment Variables

### Activation

| Variable | Default | Description |
|----------|---------|-------------|
| `S4_LICENSE_KEY` | unset | Enterprise license key string |
| `S4_LICENSE_FILE` | unset | Path to an Enterprise license file |
| `S4_MODE` | `single` | Must be `cluster` for EC pools |
| `S4_POOL_TYPE` | `standard` | Set to `ec` to activate an EC pool |
| `S4_POOL_NAME` | required in cluster mode | Pool name for this node |
| `S4_POOL_NODES` | required in cluster mode | Fixed pool members: `name:host:port,...` |
| `S4_SEEDS` | required in cluster/gateway mode | Seed gRPC addresses |
| `S4_NODE_ID` | auto | Human-readable node name; should match one `S4_POOL_NODES` name |
| `S4_NODE_GRPC_ADDR` | required in cluster mode | gRPC address for this node |
| `S4_NODE_HTTP_ADDR` | required in cluster mode | HTTP address advertised to other nodes |

### Profile

| Variable | Default | Description |
|----------|---------|-------------|
| `S4_EC_PROFILE` | `ec-rs-standard` | Built-in profile or `custom` |
| `S4_EC_CODEC` | `rs` | Custom codec: `rs` or `lrc` |
| `S4_EC_K` | required for `custom` | Data shard count |
| `S4_EC_RS_M` | required for custom RS | Reed-Solomon parity shard count |
| `S4_EC_LRC_L` | required for custom LRC | LRC local parity count |
| `S4_EC_LRC_G` | required for custom LRC | LRC global parity count |
| `S4_EC_MIN_OBJECT_SIZE` | `1048576` | Objects below this byte size remain replicated-small |
| `S4_EC_CHUNK_SIZE` | `6291456` | Logical chunk size before encoding |

### Codec engine

The profile decides how data is encoded; the engine decides only how fast the
arithmetic runs. Both engines produce byte-identical shards, so nodes with
different engines can serve the same data, and switching engines needs a
restart but no migration.

| Variable | Default | Description |
|----------|---------|-------------|
| `S4_EC_CODEC_ENGINE` | `auto` | `auto`, `isal` or `pure` |

- `auto` uses the SIMD engine when the binary was built with `--features isal`,
  the node is x86_64, and the CPU supports AVX2. Otherwise it falls back to the
  portable Rust engine without complaint. It never refuses to start.
- `isal` requires acceleration. If the feature was not built or the CPU cannot
  provide it, the EC runtime refuses to start rather than running slower than
  the operator expects.
- `pure` always uses the portable engine, on any build.

The engine chosen at startup is reported in the log and in
`GET /api/admin/ec/pools/{pool}/health` as `config.codec_engine`.

LRC profiles always compute their local parity on the portable engine: it is
plain XOR, which already runs at memory speed.

#### Building with acceleration

```bash
# Enterprise build with the accelerated codec. Requires nasm 2.14 or newer,
# which assembles the vendored ISA-L sources.
cargo build --release --features isal
```

`isal` implies `enterprise`. On x86_64 the official EE Docker image is built
this way; the aarch64 image is not, because ISA-L has no aarch64 backend.

#### What it changes, and what it does not

Nothing about the stored data. Shards, chunk hashes, CRCs and the failure matrix
are identical on both engines, which is why a cluster can mix them, an operator
can switch engines with a restart, and no migration or re-encoding is ever
needed.

Measured on an AMD Ryzen 9 7940HS, 6 MiB chunks, one core:

| Operation | Portable | ISA-L | Faster by |
|-----------|----------|-------|-----------|
| Encode, `ec-rs-standard` | 39 MiB/s | 318 MiB/s | 8.2x |
| Degraded read, `ec-rs-standard` | 44 MiB/s | 475 MiB/s | 10.9x |
| Shard repair, `ec-rs-standard` | 119 MiB/s | 928 MiB/s | 7.8x |

The other RS profiles land in the same range: 5.9x to 9.5x on encode, 7.7x to
14.9x on degraded reads, 7.8x to 8.5x on repair.

AVX-512 is worth very little here beyond AVX2. After acceleration the field
arithmetic is under 1% of an encode, so a node with AVX2 and no AVX-512 performs
within a percent of one that has it. Do not plan hardware around AVX-512 for
EC's sake.

### Topology

| Variable | Default | Description |
|----------|---------|-------------|
| `S4_EC_TOPOLOGY_POLICY` | `strict` | `strict` fails startup on partial labels; `warn` starts degraded |
| `S4_EC_NODE_TOPOLOGY` | unset | Optional labels by pool-node name. Decide the layout of a set only until the layout is recorded, see below |
| `S4_EC_LRC_GROUP_DOMAIN` | `off` | Failure-domain level the members of an LRC local group are to be spread over: `off`, `rack`, `zone` or `host`. See below |

Topology label format:

```bash
S4_EC_NODE_TOPOLOGY='node-1:zone=z1,rack=r1,host=h1,disk_group=d1;node-2:zone=z2,rack=r2,host=h2,disk_group=d2'
```

If no node has labels, startup succeeds with `TopologyUnknown`. If only some
nodes are labeled, `strict` fails startup and `warn` reports
`TopologyDegraded`.

A failure domain is named by every label above it: rack `r1` of zone `a` and
rack `r1` of zone `b` are two different racks. `disk_group` is not a failure
domain.

#### Slot layouts are recorded; labels do not move them

Which node holds which shard slot of an erasure set is chosen once and then
recorded in the cluster, with a copy on every node. A set that existed before
this release gets its record the first time its nodes run this release and the
cluster answers: the node records the layout it is already using, so nothing
moves. A node logs `EC erasure set layout recorded` when it does.

From then on the labels no longer move any slot. A node started with other
labels keeps the recorded layout and logs a warning:

```text
EC erasure set keeps its recorded layout, which differs from the one this node's topology labels give
```

Nothing is moved and nothing needs repairing. The labels still decide the
topology health and risks reported for a set, and the layout of sets created
later.

Only a node of a set ever writes that set's record. A node added to a grown
pool may find no record at first — growing the pool moves each record to new
owners in the cluster, which do not have it yet — and then waits: the set's own
nodes publish their copy again within one refresh (`S4_EC_SET_REFRESH_SECS`).
Until a node has read a set's record it serves the set's reads but encodes
nothing into it: an object written through that node while no other set can
take it is queued for the held set and stays on its RF=3 replicas until the
record is read, then it is encoded by the recorded layout. The same happens to
a node that cannot reach enough peers in its first seconds after start.
`layout_pending` in the set listing and in pool health shows it.

Where the shards of an object are is recorded in the object's own manifest,
and reads trust it: a shard is accepted from any node of the object's erasure
set, and refused from a node outside that set. An object written under an
earlier layout — by a release that recomputed layouts from labels, or by a node
that had not yet taken the record — therefore stays readable after a layout or
label change.

Two cautions remain:

- **Do not change labels while the pool still has nodes of an older release.**
  Those nodes still recompute layouts from labels. Upgrade every node first.
- **Rolling back to a release without layout records** makes every set laid out
  from labels again. If the labels changed after a layout was recorded, restore
  them before rolling back.

Keep `S4_EC_NODE_TOPOLOGY` identical on every node of the pool: each node judges
topology health by its own labels, and a node that records a layout records the
one its labels give.

#### LRC group level

`S4_EC_LRC_GROUP_DOMAIN` selects what counts as a placement violation for the
local groups of an LRC profile; `S4_EC_TOPOLOGY_POLICY` still decides what to do
about one. The level applies only to LRC profiles and only to sets created
after it is set. Every set that already exists, including the pool's first set,
keeps its recorded layout and is judged by the checks it was created under, so
switching the level on never moves data and never stops a node from starting.
An unknown value stops startup and names the variable.

With a level set, a set added to an LRC pool
(`POST /api/admin/ec/pools/{pool}/sets`) is laid out so that **every member of
a local group — its data shards and its local parity — is in a different
failure domain of that level**. Losing one domain then takes at most one shard
from each group, and every such loss is repaired by XOR inside the group,
without reading `k` shards (see [Repair](#repair)). Above that guarantee the
layout, in this order,
survives as many whole-zone losses as the decoder allows, spreads the global
parity shards over domains, and keeps the members of a group in one zone where
that costs nothing, so a local repair stays inside the zone.

| Level | Members of a group are in distinct… |
|-------|-------------------------------------|
| `rack` | racks (recommended; the level EC v1 already warns about) |
| `zone` | zones — every group then survives the loss of a zone, but every local repair crosses zones |
| `host` | hosts; under `strict` this is already true of every set, so it only matters under `warn` |

What the set's nodes need, for the level chosen:

| Profile | Members of a group | Domains needed at least |
|---------|--------------------|-------------------------|
| `ec-lrc-small` | 3 | 3 |
| `ec-lrc-standard` | 4 | 4 |
| `ec-lrc-dense` | 6 | 6 |

Enough domains is necessary but not sufficient: a domain can give at most one
node to each of the `l` local groups (2 for every standard profile), and the
rest of its nodes can only take the `g` global parity slots. The exact
condition is that the nodes above `l` per domain, summed over domains, do not
exceed `g`. For `ec-lrc-standard` in racks of 5, 2, 2 and 1 nodes, the rack of
five leaves three nodes for two global slots, so no layout keeps every group
apart.

When the nodes cannot meet it:

- `strict` refuses the set with `400` and says what is needed and what the set
  has, for example: `profile 'ec-lrc-standard' spreads every local group over 4
  distinct racks, but the 10 nodes of this set are in 3 rack(s) (z1/a=4,
  z1/b=3, z1/c=3): build the set from nodes in at least 4 racks`. Nothing is
  written. A pool without labels is refused too: the rule has nothing to spread
  by.
- `warn` creates the set as close to the rule as its nodes allow, with
  `TopologyDegraded` and the same explanation among its risks. The add
  response carries `layout_violation` with the reason. With incomplete labels
  it keeps the plain layout and says the rule was not applied.

Every such case is counted by `s4_ec_set_layout_unsatisfied_total{reason,
policy}` (see [Metrics](#metrics)). Each set is judged on its own nodes only.

A set's risks under the rule also name every zone whose complete loss the
profile does not survive. With one or two zones that is expected: a zone
holding half of a set's shards or more is never survivable, whatever the
layout. Surviving the loss of any zone takes three zones for `ec-lrc-small` and
`ec-lrc-standard`, and four for `ec-lrc-dense`.

The layout is chosen once, by the node that takes the request, and recorded
before the set is published. Every other node takes it from the record, so its
own labels never change the set. Adding a set by the rule is refused while any
node of the pool does not yet read layout records, and the refusal names those
nodes.

Switching the level back to `off` makes new sets use the plain layout again.
Sets laid out by the rule keep their layout until an operator applies a rule to
them explicitly (below). Rolling back to a release without the rule leaves the
objects of such sets unreadable through the old release, which would lay those
sets out by labels; returning to this release restores them.

#### When a set stops meeting its rule

Moving a node between racks moves no shard, but it can leave a set no longer
keeping the guarantee it was laid out with. That is detected and reported, never
corrected on its own.

Every anti-entropy pass compares the pool's sets against the labels the node
holds now, each set by its own rule. It is an in-memory comparison that reads no
manifest and no shard, so it costs nothing next to the pass it runs inside. What
it finds appears in three places:

- the node logs `EC erasure set no longer meets the layout rule it was laid out
  by`;
- pool health carries `topology.layout_drift` beside the topology health and
  risks that were always there;
- `s4_ec_set_layout_violations` counts the violators (see [Metrics](#metrics)).

A set laid out by the EC v1 rule is never a violator — that rule makes no
promise about placement — so switching the level on does not turn a pool's
existing sets into violators. A set created in breach under `warn` is reported
as `creation` rather than `drift`: it has been a violator since birth, and
looking for a label change that never happened would waste an operator's time.
Incomplete labels leave the rule unjudgeable, which is reported and not counted:
an unknown answer is not a known breach.

Each node judges by its own labels. Keep `S4_EC_NODE_TOPOLOGY` identical across
the pool; where it differs, the `placement` block in the set snapshot shows
which labels an answer came from — every slot with its zone, rack and host, and
the set's slots grouped by the domain level of its own rule.

```bash
curl -s http://localhost:9000/api/admin/ec/pools/my-pool/health \
  -H "Authorization: Bearer $TOKEN" | jq '.topology.layout_drift, .erasure_set.placement'
```

#### Applying the rule to a pool that already exists

Three ways out of a violation: restore the labels, accept the risk, or lay the
sets out again by the rule.

```bash
# What would change, writing nothing.
curl -s -X POST http://localhost:9000/api/admin/ec/pools/my-pool/topology \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"dry_run":true}'

# Lay every set of the pool out by the rule this node is configured with.
curl -s -X POST http://localhost:9000/api/admin/ec/pools/my-pool/topology \
  -H "Authorization: Bearer $TOKEN"

# One set only.
curl -s -X POST http://localhost:9000/api/admin/ec/pools/my-pool/topology \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"set_id":1}'
```

**Objects already stored do not move.** They keep their shards where they are,
stay readable, and keep the durability they had. Only objects written from then
on follow the new layout. Moving the existing ones would be a rewrite, and this
command does not do one. `moved_slots` in the answer says how far each set's
layout moved; zero means the rule already had it where it wants it.

The rule comes from the node taking the request and never from the body: a set
has to come out the same on every node. The command is refused before anything
is written when

- the node lays sets out by the EC v1 rule (`S4_EC_LRC_GROUP_DOMAIN` is `off`),
  or the profile is Reed-Solomon and has no local groups to spread;
- any node of the pool does not yet read layout records — the same gate that
  adding a set by the rule passes;
- a set is still waiting for its layout record on this node (`layout_pending`):
  its current layout is not known here, so it must not be replaced.

Apply the rule while the pool is healthy. A node that is unreachable for the
whole command never learns the new record, and if the cluster later cannot
produce that record — as right after a pool grows — such a node can publish its
older copy over the new one. The drift check above is what catches it: the set
reappears as a violator, and the command is simply run again.

#### Spreading a pool over zones

Keeping a local group in distinct racks survives a rack. Surviving a whole zone
takes zones to spread over, and enough of them that no zone holds half of a
set's shards:

| Profile | Members of a group | Racks at least | Zones to survive losing one |
|---------|--------------------|----------------|-----------------------------|
| `ec-lrc-small` | 3 | 3 | 3 |
| `ec-lrc-standard` | 4 | 4 | 3 |
| `ec-lrc-dense` | 6 | 6 | 4 |

With one or two zones, no layout survives the loss of a zone, and none can: two
zones leave one of them holding half the shards or more — 4, 5 and 7 — while the
decoder survives at most 3, 4 and 4 losses. That is a property of the profile,
not of the placement, so a two-zone pool is a pool that survives racks, not
zones.

Ten nodes of `ec-lrc-standard` over three zones with two racks in each, four
nodes in the first zone and three in the others:

```bash
export S4_EC_LRC_GROUP_DOMAIN=rack
export S4_EC_NODE_TOPOLOGY='node-1:zone=z1,rack=ra,host=h1;node-2:zone=z1,rack=ra,host=h2;node-3:zone=z1,rack=rb,host=h3;node-4:zone=z1,rack=rb,host=h4;node-5:zone=z2,rack=ra,host=h5;node-6:zone=z2,rack=ra,host=h6;node-7:zone=z2,rack=rb,host=h7;node-8:zone=z3,rack=ra,host=h8;node-9:zone=z3,rack=ra,host=h9;node-10:zone=z3,rack=rb,host=h10'
```

The level stays `rack`: the guarantee is about racks, and zones are what the
layout optimises for above it. No rack holds more than two of the set's nodes,
so the rule is satisfiable, and with three zones the layout that comes out
survives the loss of any one of them. Check it rather than assume it — the
answer is in the set's own risks, which name every zone whose loss the profile
does not survive:

```bash
curl -s http://localhost:9000/api/admin/ec/pools/my-pool/sets \
  -H "Authorization: Bearer $TOKEN" \
  | jq '.sets[] | {set_id, topology_risks, layout_rule}'
```

#### Taking a zone out, and bringing it back

A zone the layout survives can be taken out without the objects becoming
unreadable: every read decodes around it, and `s4_ec_read_degraded_total` is
what shows it is happening.

1. **Before.** The set must name no zone whose loss it does not survive (above),
   and the pool must report no drift and no backlog:
   `curl -s .../health | jq '.topology.layout_drift, .repair'`.
2. **While the zone is down.** Reads carry on. Repairs of shards owned by the
   stopped nodes cannot finish — there is nowhere to write them — so they wait
   with backoff rather than fail; only a loss whose sources all answered becomes
   `failed_permanent`. Writes carry on through the nodes that are up.
3. **Do not relabel the nodes that are left.** Labels move no shard: the layout
   is recorded, and relabeling only changes what the drift check reports. A set
   is laid out again only by `POST /api/admin/ec/pools/{pool}/topology`, and even
   then only new objects follow the new layout.
4. **When the zone returns.** Its shards are where it left them. The first
   anti-entropy pass after a restart re-registers shards that were written to a
   node remotely, so some repair traffic right after a return is expected. A
   shard that really was lost — a node that came back empty — is rebuilt by its
   local group: 2, 3 and 5 source shards for the three LRC profiles, against the
   `k` a Reed-Solomon set needs.
5. **If the zone is gone for good.** Laying the set out again over what is left
   needs enough racks for the rule; the command refuses with the numbers when
   there are not. Objects already stored keep their shards wherever the set goes
   next.

#### Before switching the level on

Two limits, both about releases rather than about placement, and both worth
knowing before the first set is laid out by the rule:

- **Do not change `S4_EC_NODE_TOPOLOGY` while the pool still has nodes of a
  release older than layout records.** Those nodes recompute layouts from labels
  and would serve a different one. Upgrade every node first; adding a set by the
  rule is refused until they all do.
- **Rolling back to a release without the rule leaves the objects of sets laid
  out by it unreadable through that release**, which would lay those sets out
  from labels instead. Nothing is lost — returning to this release restores
  them — but the rollback is not transparent, unlike a rollback on a pool whose
  sets are all EC v1 with labels unchanged.

### Transcode and Write Backpressure

| Variable | Default | Description |
|----------|---------|-------------|
| `S4_EC_TRANSCODE_ENABLED` | `true` | Allows staged transcode jobs to be queued after hot RF=3 commits |
| `S4_EC_INLINE_WRITE_ENABLED` | `false` | Future opt-in inline EC writes; staged hot RF=3 writes remain the v1 default |
| `S4_EC_MAX_CONCURRENT_TRANSCODES` | `2` | Max staged transcodes per node |
| `S4_EC_MAX_CONCURRENT_INLINE_WRITES` | `0` | Max inline EC writes per node |
| `S4_EC_TRANSCODE_LEASE_SECS` | `300` | Active transcode lease TTL |
| `S4_EC_FULL_VERIFY_MAX_BYTES` | `1073741824` | Full reconstruction verification limit |
| `S4_EC_RECONSTRUCTION_PROBE_CHUNKS` | `3` | Deterministic reconstruction probes for larger objects |
| `S4_EC_HOT_REPLICA_GC_GRACE_SECS` | `86400` | Grace before hot RF replicas may be purged |
| `S4_EC_HOT_PURGE_MODE` | `auto` | `auto` or `manual` |
| `S4_EC_TRANSCODE_BANDWIDTH_LIMIT` | unset | Optional transcode bandwidth cap in bytes/sec |
| `S4_EC_MAX_RETRY_ATTEMPTS` | `10` | Retry budget for staged transcode |
| `S4_EC_INITIAL_BACKOFF_SECS` | `30` | Initial staged transcode retry backoff |
| `S4_EC_MAX_BACKOFF_SECS` | `3600` | Maximum staged transcode retry backoff |

### Codec CPU Limits

| Variable | Default | Description |
|----------|---------|-------------|
| `S4_EC_CPU_WORKERS` | derived from CPU budget | Hard cap for concurrent codec jobs |
| `S4_EC_CPU_BUDGET_PERCENT` | `60` | Target CPU budget for EC codec work |
| `S4_EC_MAX_INFLIGHT_ENCODE_BYTES` | `268435456` | Max logical bytes currently being encoded |

`S4_EC_CPU_WORKERS` defaults to `ceil(cores * S4_EC_CPU_BUDGET_PERCENT / 100)`.
Each worker runs one codec job at a time on a blocking thread.

The two limits behave differently when they are reached, and it matters:

- **Worker limit:** the job waits for a free worker. Throughput drops, nothing
  fails.
- **Byte limit:** the job is rejected with `EC CPU backpressure`. Staged
  transcode retries it with the backoff configured above, so an object is
  delayed rather than lost, but a node that hits this constantly will build a
  transcode queue. This limit exists to bound memory, not to pace CPU.

#### Sizing with and without ISA-L

One worker sustains roughly the throughput measured for a single core:
39 MiB/s on the portable engine, 318 MiB/s with ISA-L (`ec-rs-standard`).
Encoding is the write path, so plan against the incoming write rate:

| Sustained writes | Workers, portable | Workers, ISA-L |
|------------------|-------------------|----------------|
| 1 Gbit/s (119 MiB/s) | ~3 | ~1 |
| 10 Gbit/s (1.2 GiB/s) | ~31 | ~4 |

On an accelerated node the default 60% budget is therefore generous, and
lowering `S4_EC_CPU_BUDGET_PERCENT` frees cores for the request path without
slowing EC down. Do not raise it above 60% to chase throughput: with ISA-L the
codec is no longer limited by arithmetic but by memory traffic — splitting
chunks into shards and hashing them is about 99% of an encode — and extra
workers past that point contend for the same memory bandwidth.

`S4_EC_MAX_INFLIGHT_ENCODE_BYTES` needs no change when acceleration is enabled.
It caps bytes, not time, and faster jobs release their reservation sooner, so
the same 256 MiB admits more work. Raise it only if backpressure rejections
appear while the CPU is idle, which means the cap, not the worker count, is
what is holding the queue back.

### Repair

| Variable | Default | Description |
|----------|---------|-------------|
| `S4_EC_REPAIR_MAX_GLOBAL_CONCURRENCY` | `4` | Max repairs running across this node |
| `S4_EC_REPAIR_MAX_NODE_CONCURRENCY` | `1` | Max repairs targeting one shard owner |
| `S4_EC_REPAIR_MAX_SET_CONCURRENCY` | `2` | Max repairs in one erasure set |
| `S4_EC_REPAIR_MAX_FAILURE_DOMAIN_CONCURRENCY` | `1` | Max repairs moving bytes across one zone's boundary at a time |
| `S4_EC_REPAIR_MAX_QUEUE_DEPTH` | `10000` | Durable repair queue depth |
| `S4_EC_REPAIR_GLOBAL_BANDWIDTH_BYTES_PER_SEC` | unset | Optional cap on every byte repair reads and writes on this node |
| `S4_EC_REPAIR_FAILURE_DOMAIN_BANDWIDTH_BYTES_PER_SEC` | unset | Optional cap on the repair bytes crossing one zone's boundary, per node |
| `S4_EC_REPAIR_THROTTLE_RETRY_SECS` | `5` | Retry delay after throttling |
| `S4_EC_REPAIR_INITIAL_BACKOFF_SECS` | `30` | Initial repair retry backoff |
| `S4_EC_REPAIR_MAX_BACKOFF_SECS` | `3600` | Maximum repair retry backoff |
| `S4_MAX_REJOIN_DOWNTIME_DAYS` | `3` | Downtime after which a returning node must rebuild before it serves EC reads |
| `S4_EC_LONG_OFFLINE_REJOIN_POLICY` | `needs_bootstrap` | What a node past that limit may do: `needs_bootstrap` or `require_admin_approval` |
| `S4_EC_REJOIN_GATE_ENABLED` | `true` | Whether a held-back node actually stops serving EC reads, or only reports that it should |
| `S4_EC_REPAIR_SCAN_LIMIT` | `1024` | Max stale running repair tasks recovered in one worker pass |
| `S4_EC_REPAIR_PRIORITY_AGE_BOOST_SECS` | `600` | How long a ready repair task waits before it starts taking turns of its own, ahead of more damaged chunks; `0` leaves the order to damage alone |
| `S4_EC_REPAIR_PRIORITY_AGE_BOOST_EVERY` | `8` | One turn in this many goes to the erasure set's longest-waiting ready task |
| `S4_EC_REPAIR_RUNNING_TIMEOUT_SECS` | `900` | Recover stale running repair tasks after this timeout |
| `S4_EC_REPAIR_WORKER_ENABLED` | `true` | Start the supervised durable repair worker |
| `S4_EC_REPAIR_WORKER_POLL_MS` | `1000` | Repair worker poll interval |
| `S4_EC_REPAIR_WORKER_BATCH_SIZE` | `64` | Max repair tasks attempted per worker poll |

A repair runs on the node that queued it and decides what to read before it
reads anything. One lost member of an LRC local group — a data shard or the
group's local parity — is rebuilt by XOR from the rest of its group: 2, 3 and 5
shards for `ec-lrc-small`, `ec-lrc-standard` and `ec-lrc-dense`. Anything else
— a global parity shard, a group missing a second member, every Reed-Solomon
shard — is decoded from `k` shards, the nearest first: this node, its rack, its
zone, then the rest. When a source turns out to be missing too, the repair reads
one source more rather than the whole chunk. Degraded reads ask for shards in
the same order but still read every shard of a chunk: that is how a read
notices a missing or corrupt shard and queues its repair.

Every repair task carries the zone of the shard's owner as its failure domain,
and the limits count the bytes that really move:

- `S4_EC_REPAIR_GLOBAL_BANDWIDTH_BYTES_PER_SEC` counts every byte a repair reads
  and writes.
- `S4_EC_REPAIR_FAILURE_DOMAIN_BANDWIDTH_BYTES_PER_SEC` and
  `S4_EC_REPAIR_MAX_FAILURE_DOMAIN_CONCURRENCY` apply to a repair that moves
  bytes across its zone's boundary, on either leg, and the bandwidth limit
  counts only those bytes. A pool of one zone is never held back by them. A
  node without a zone label counts as outside the zone.
- A repair larger than a whole second's budget runs when nothing is
  outstanding, and what it overspent is carried into the following seconds, so
  the limit holds on average. The limits are per node: the traffic through one
  zone can reach the limit times the number of nodes repairing.

A shard that cannot be rebuilt from what is left is reported with the lost
slots and any local group that lost every member, and the repair stops reading
as soon as it knows. If a source node did not answer, the task waits and
retries with backoff — the node may come back. If every lost source answered,
the task becomes `failed_permanent`, and only
`POST /api/admin/ec/pools/{pool}/repair` queues it again. A shard repaired once
is repaired again when it is lost again. A finished task's `reason` in
`repair-status` names how the shard was rebuilt, for example `repair completed
by local group 0 from 3 source shard(s)`. A shard that no EC v1 node could have
rebuilt says so: `repair completed by global from 4 source shard(s), through
the unified LRC solver`.

### What a repair writes

A repair always puts bytes on the disk of the shard's owner. It is never
answered by the write path saying "I already have this content" and never by
the replica saying "I have already applied this write":

- the write carries the healing marker the storage engine reads
  (`_s4_force_physical_write`), so the rebuilt shard is materialised in a volume
  instead of being pointed at the blob that already holds that content — which,
  after bit rot, is the damage itself;
- it carries an operation id of its own, one per write. A replica skips a write
  whose operation id it has already applied, which is right for a coordinator
  resending the same write after a timeout and wrong for a shard that was
  rebuilt again: the bytes there are gone or rotten now.

That is what makes a shard which rots twice come back twice.

### Which repair runs next

The queue is durable, it belongs to the node that queued the tasks, and it is
served by damage rather than by age.

**Damage is counted per chunk.** A chunk is what the codec rebuilds, so the
urgency of a task is how many shards of *its own chunk* the pool is missing —
the number of tasks of that chunk that are queued, running or waiting to retry.
Two damaged chunks of one object do not add up, and a shard already rebuilt
stops counting the moment its task finishes. The order is then: the chunk
missing most shards first, equally damaged chunks oldest first, and the task id
only to keep the order the same on every pass. For Reed-Solomon that is exactly
the redundancy each chunk has left; for LRC it can only understate the damage,
never overstate it, because where two losses fell inside a local group is not
part of the count.

This matters when the queue is long, which is when something has happened: an
object one shard away from being unrecoverable no longer waits behind thousands
of objects that lost one shard and were queued earlier.

**Turns between sets come first.** A set with a heavy incident takes its turn
like any other, so damage decides the order *inside* a set and never across
them. Without that, one set's backlog would take the whole node's repair
throughput.

**The age band stops a task from being passed over forever.** Ordering by damage
alone has no upper bound on how long a task can wait while more damaged ones
keep arriving, so one turn of a set in
`S4_EC_REPAIR_PRIORITY_AGE_BOOST_EVERY` goes to the task that has waited
longest, as soon as it has waited longer than
`S4_EC_REPAIR_PRIORITY_AGE_BOOST_SECS`:

| Variable | Default | What it does |
|---|---|---|
| `S4_EC_REPAIR_PRIORITY_AGE_BOOST_SECS` | `600` | The age at which a ready task starts taking turns of its own. `0` switches the band off and leaves the order to damage alone |
| `S4_EC_REPAIR_PRIORITY_AGE_BOOST_EVERY` | `8` | One turn in this many is the band's. `1` gives the band every turn, which is oldest-first again |

Because no task queued later can enter the band ahead of an earlier one, a task
that has entered it waits no longer than `(o + 1) × N` turns of its set, where
`o` is how many ready tasks of that set were already older than it and `N` is
the second variable. In a healthy pool the band is empty and costs nothing: a
turn nobody has waited long enough for goes to the most damaged chunk as usual.

A threshold alone would not do: after an hour of a dead node's backlog the whole
queue is older than any threshold, and "older than the threshold wins" is plain
oldest-first — the order this exists to replace, switched off exactly when it is
needed. That is why the band is a share of the turns and not a rule that
outranks damage.

**What the order does not consider.** Losses nobody has found yet — the count is
of what the pool knows, which is what the scrub and anti-entropy passes feed it.
The size of the shard or the zone it has to cross: how much a repair costs is
the business of the bandwidth and concurrency limits above, and mixing cost into
a number about risk would make both unreadable. And who queued the task: a
repair an operator asked for by hand counts as a loss like any other.

### LRC recovery paths

An LRC chunk comes back by one of three routes. They are not alternatives an
operator chooses between: the decoder takes the cheapest one that reaches the
loss in front of it.

| Route | What it reaches | What it reads |
|---|---|---|
| Local XOR | One data shard missing from a local group whose local parity survives, or a local parity shard whose group data survives | `k / local` shards of the one group: 2, 3 and 5 for `ec-lrc-small`, `ec-lrc-standard` and `ec-lrc-dense`. No cross-zone traffic when the group sits in one zone |
| Global Reed-Solomon | No more missing data shards than surviving global parity shards, local parity taking no part | `k` shards |
| Unified solver | Everything the other two reach, and the patterns that fall between them: a group that lost more data shards than global parity alone carries, while the group's local parity is alive | `k` shards |

The unified solver is the EC v2 addition. It puts every surviving equation into
one system — the identity row of each data slot, the XOR row of each local
parity slot, the Reed-Solomon row of each global parity slot — and solves it
over GF(2^8). The data comes back exactly when the rows of the surviving shards
have rank `k`, which is the most any decoder could promise, and short of that
rank it refuses rather than returning something plausible.

What that changes for a pool, as a number an operator can plan around — the
losses a profile survives whatever falls where:

| Profile | Guaranteed before EC v2 | Guaranteed now | Best case |
|---|---|---|---|
| `ec-lrc-small` (4,2,1) | 1 | 2 | 3 |
| `ec-lrc-standard` (6,2,2) | 2 | 3 | 4 |
| `ec-lrc-dense` (10,2,2) | 2 | 3 | 4 |

The boundary moved; it did not disappear. Losing a whole local group — its data
shards and its local parity — is still unrecoverable, and so is any pattern
that leaves fewer than `k` independent equations.

**What it costs.** Measured on a 6 MiB chunk, one core, portable engine
(LRC always uses the portable engine, because its local parity is XOR):

| Route | `ec-lrc-small` | `ec-lrc-standard` | `ec-lrc-dense` |
|---|---|---|---|
| Healthy read | 6.9 ms | 6.8 ms | 5.1 ms |
| Local XOR | 6.0 ms | 5.8 ms | 5.0 ms |
| Global Reed-Solomon | 50.7 ms | 50.8 ms | 48.9 ms |
| Unified solver | 93.1 ms | 134.9 ms | 134.9 ms |

A healthy read does not pay for any of this: the solver is reached only where
both fast paths have run out, and a read that finds every data shard in place
never enters the codec's recovery at all. The first two rows are almost
entirely the CRC32 and SHA-256 check of the shards, which every read does
anyway — that is why local XOR, which has one shard fewer to verify, comes out
below a healthy read.

**Seeing it.** A recovery that only the solver reached is written down rather
than passed over:

- the read path logs `EC chunk rebuilt by the unified LRC solver` with the
  bucket, key and chunk;
- a repair says `through the unified LRC solver` in the `reason` of the
  finished task;
- `s4_ec_unified_solver_recoveries_total` counts both, by `operation`.

A pool where that counter moves is a pool leaning on the full recovery path.
Nothing is wrong with the data — but the redundancy that is left is thinner
than the fast paths alone would carry.

**Emulating a pre-solver node.** `S4_EC_TEST_LRC_FAST_PATH_ONLY=true`, together
with `S4_EC_TEST_HOOKS_ENABLED=true`, holds one node's LRC recovery to the two
fast paths: it decodes, repairs and publishes a failure matrix exactly as an
EC v1 node did, and refuses the patterns only the solver reaches. The flag is
read once at startup and changes recovery only — encoding is untouched, so a
node carrying it writes the same shards as any other. It is test scaffolding:
in production it would only remove a recovery path the cluster already has.

### Reading only what is needed

| Variable | Default | Description |
|----------|---------|-------------|
| `S4_EC_MINIMAL_READ_ENABLED` | `true` | Whether a read that serves bytes pulls only the `k` data shards it assembles from |

Both codecs are systematic: the `k` data shards of a chunk are that chunk cut
into consecutive blocks, so assembling them is a concatenation and parity is
needed only where one of them is missing or fails its manifest hash. A read
therefore asks for what its own answer needs and nothing beyond it:

| Read | Shards it pulls, per chunk | What checks the bytes |
|------|---------------------------|-----------------------|
| `GET` of a whole object | the `k` data shards | `chunk_sha256` of each chunk |
| Range covering a chunk whole | the `k` data shards | `chunk_sha256` of that chunk |
| Range covering a chunk in part | only the data shards the range falls in, each whole | the `sha256` of each shard read |
| Any of them, short a data shard | all `n`, nearest first, then decode | `chunk_sha256` |
| `HEAD` | none | — |

The line runs along how much of a chunk is wanted rather than along the kind of
request. A 100 MiB range inside a 1 GiB object covers its middle chunks whole,
and those are read and checked exactly as a full `GET` reads them; only the two
chunks at the ends are partial.

On `ec-rs-standard` a healthy `GET` moves `6/9` of the bytes and makes `6/9` of
the round trips the EC v1 read made, and the share of both that crosses a zone
falls in the same proportion. A short range costs one shard instead of all nine
of its chunk. Watch it with
[`s4_ec_read_shard_bytes_total`](#metrics).

Measured on a live `ec-rs-small` (4+2) pool of six nodes, each in a zone of its
own, holding a 5 MiB object in five 1 MiB chunks — a 256 KiB shard — read
through one node:

| Request | Fetches | Bytes | Of which crossed a zone |
|---------|---------|-------|-------------------------|
| `GET` of the whole object | 30 → 20 | 7.5 MiB → 5 MiB | 6.25 MiB → 3.75 MiB |
| 1 KiB inside one shard | 6 → 1 | 1.5 MiB → 256 KiB | 1.25 MiB → 256 KiB |
| 4 bytes across a shard boundary | 6 → 2 | 1.5 MiB → 512 KiB | 1.25 MiB → 256 KiB |
| 4 bytes across a chunk boundary | 12 → 2 | 3 MiB → 512 KiB | 2.5 MiB → 256 KiB |

The whole-object figures are exactly `k/n`; the range figures are not a ratio of
`k` to `n` at all, because a short range used to cost every shard of every chunk
it touched and now costs the shards it falls in. The fetch column is worth as
much as the byte column: shards are fetched one after another, so a fetch that
is no longer made is a network round trip out of the latency chain.

**What this hands to the scrubber.** The old read verified every parity shard on
every read, and so found corruption in parity it did not otherwise need. It no
longer does. Finding that is the local scrub pass, which checks every shard a
node owns — parity included — against the same manifest hash, on
`S4_EC_SCRUB_WORKER_INTERVAL_SECS` (60 seconds by default; lower it to shorten
the window between a parity shard rotting and the pool noticing). Corruption in
a data shard is still found by the read, counted in
`s4_ec_shard_hash_mismatch_total` and queued for repair exactly as before.

A shard whose bytes the owner reads and refuses — they no longer match the hash
recorded for them — is reported as corruption and not as absence, and the node is
made to stop claiming it: the scrub quarantines the local record, counts the
shard under `corrupt_shards` and its `physical_read_failures` column, and queues
a corrupt-shard repair. A shard that was never written to that node stays a
missing shard, which is a placement problem and reads differently.

**A medium that does not answer is a third thing, and it condemns nothing.** A
detached volume, an exhausted descriptor table or an index that stopped
responding says nothing about the bytes it was asked for, so the scrub leaves the
shard with its owner: no quarantine, no corruption counter, no repair. Such
reads are counted under `media_unavailable` in the pass report, and once they
reach both four failures and `S4_EC_SCRUB_MEDIA_FAILURE_RATIO` of the shards the
pass has looked at, the pass stops and reports `media_suspect: true` — the
finding is the disk, and walking the rest of the inventory would only repeat it.
The next pass asks again; a shard that really is lost is found as corruption or
as absence as soon as the medium answers at all.

```bash
# What a pass found, including whether it stopped on the medium.
curl -s -X POST http://localhost:9000/api/admin/ec/test/scrub/run \
  -H "Authorization: Bearer $TOKEN" \
  | jq '{corrupt_shards, physical_read_failures, media_unavailable, media_suspect}'
```

The scrub pass has no counter of its own, so what it finds is watched through
what it does about it. A pass that finds a shard its owner can no longer produce
queues an ordinary repair, and that repair is visible everywhere repairs are:

```bash
# The task, named by object, chunk and slot, on the node that queued it.
curl -s "http://localhost:9000/api/admin/ec/pools/my-pool/repair-status" \
  -H "Authorization: Bearer $TOKEN" | jq '.tasks'
```

```promql
# And the same finding as a rate, next to the queue it lands in.
rate(s4_ec_repair_jobs_total[5m])
s4_ec_repair_queue_depth
```

A pool where parity rots and nothing is repaired is a pool whose scrub worker is
not running: check `S4_EC_SCRUB_WORKER_ENABLED` on the owners before reading the
quiet as good news.

Two reads deliberately ignore this setting and keep pulling all `n`: the
scrubber's reconstruction sample and the `full_data_verify` of
`POST /api/admin/ec/objects/{bucket}/{key}/verify`. Both answer a question about
every shard of the object rather than about the bytes somebody asked for, and a
check that stopped looking at parity would no longer mean what it says.

Setting it to `false` restores the EC v1 read on that node alone. Nothing on
disk depends on it, so a pool may run with it set either way on any of its
nodes, and `config.minimal_read_enabled` in pool health says which each node is
doing:

```bash
curl -s http://localhost:9000/api/admin/ec/pools/my-pool/health \
  -H "Authorization: Bearer $TOKEN" | jq '.config.minimal_read_enabled'
```

### Anti-Entropy and Scrub

| Variable | Default | Description |
|----------|---------|-------------|
| `S4_EC_AE_WORKER_ENABLED` | `true` | Start scheduled EC manifest inventory audits |
| `S4_EC_AE_WORKER_INTERVAL_SECS` | `60` | EC anti-entropy worker interval |
| `S4_EC_SCRUB_WORKER_ENABLED` | `true` | Start scheduled local shard scrub passes |
| `S4_EC_SCRUB_WORKER_INTERVAL_SECS` | `60` | EC scrub worker interval |
| `S4_EC_AE_MERKLE_DEPTH` | `15` | EC manifest Merkle tree depth; max accepted value is `20` |
| `S4_EC_AE_MAX_MANIFEST_SCAN_ENTRIES` | `10000` | Max manifests scanned in one anti-entropy pass |
| `S4_EC_AE_MAX_SHARD_SCAN_ENTRIES` | `10000` | Max local shard records scanned in one scrub pass |
| `S4_EC_SCRUB_RECONSTRUCTION_SAMPLE_CHUNKS` | `3` | Max reconstructed chunk samples per committed object |
| `S4_EC_SCRUB_MEDIA_FAILURE_RATIO` | `0.25` | Share of a scrub pass that may fail on the medium before the pass stops and calls the disk suspect instead of judging shards |

### Release Attestation

| Variable | Default | Description |
|----------|---------|-------------|
| `S4_EC_V1_CI_ACCEPTANCE_PASSED` | unset | Build-time flag that marks an EE build as accepted for production. Without it, health may report `runtime_ready=true` but keeps `production_ready=false`. |

### GC, Orphans, and Tiering

| Variable | Default | Description |
|----------|---------|-------------|
| `S4_EC_GC_GRACE_SECS` | `S4_GC_GRACE_DAYS * 86400` | EC tombstone and shard GC grace |
| `S4_EC_ORPHAN_LEASE_GRACE_SECS` | `3600` | Extra safety grace after transcode lease expiry |
| `S4_EC_ORPHAN_MIN_AGE_SECS` | `86400` | Minimum orphan shard age before physical GC |
| `S4_EC_GC_SCAN_LIMIT` | `1024` | GC metadata records scanned per page |
| `S4_EC_GC_MAX_SHARDS_PER_OBJECT` | `100000` | Max local shards removed for one tombstone cycle |
| `S4_EC_TOMBSTONE_GC_WORKER_ENABLED` | `true` | Whether this node releases deleted objects' shards on a schedule |
| `S4_EC_TOMBSTONE_GC_WORKER_INTERVAL_SECS` | `300` | Interval between tombstone sweeps |
| `S4_EC_TOMBSTONE_RETIRE_GRACE_SECS` | `3600` | Extra grace before a tombstone is retired pool-wide |
| `S4_EC_TOMBSTONE_GC_SCAN_BUDGET` | `10000` | Manifests one sweep walks before leaving the rest to the next cycle |
| `S4_EC_TIERING_ENABLED` | `false` | Optional auto-tiering gate |
| `S4_EC_TIERING_COLD_AFTER_SECS` | `2592000` | Idle time before an object is considered cold |
| `S4_EC_TIERING_SCAN_LIMIT` | `4096` | Objects examined by one tiering cycle |
| `S4_EC_ADMIN_SCAN_LIMIT` | `4096` | Max EC metadata records scanned by one admin request |

#### What collects a deleted object

A delete writes a tombstone manifest through the metadata quorum and returns.
Two things are still on disk at that point: the object's shards, held node by
node, and the tombstone itself. They are collected separately, because they
belong to different owners.

**The shards belong to the node that holds them.** Every node runs the same
sweep on `S4_EC_TOMBSTONE_GC_WORKER_INTERVAL_SECS`, and releases its own shards
once three things hold: the tombstone is older than `S4_EC_GC_GRACE_SECS`, the
pool's quorum read still returns this exact tombstone, and every owner of the
object's erasure set has been shown to hold it. An owner that was down when the
delete was committed never received it and nothing else would ever hand it
over, so the sweep writes the tombstone there before it judges anything — a
delete that reached a quorum but not a particular owner is exactly the zombie
this path exists to prevent.

**The tombstone belongs to the pool**, and is removed from every metadata node
in one step, `S4_EC_TOMBSTONE_RETIRE_GRACE_SECS` after the shard grace. Removed
from some nodes and not others it would be a manifest no quorum read can decide
either way, and a deleted object would answer "unavailable" instead of "gone".
The second grace is what gives every owner a sweep of its own first; an owner
that had not swept by then keeps shards no manifest names, which the orphan
sweep collects instead.

A node that comes back after the tombstone was retired finds the pool answering
"no such object" for a delete it still holds. That is not a reason to keep the
shards — it is the object finishing being collected — so the sweep releases them
and drops its own copy of the tombstone.

**What a sweep costs.** It looks for deletes among every manifest the node
holds, so the cost is the walk rather than the findings. Each cycle walks
`S4_EC_TOMBSTONE_GC_SCAN_BUDGET` manifests from where the last one stopped and
leaves the rest to the next; the position is durable, so a restart continues the
round instead of starting it again. Owners are asked about every delete of the
cycle in one request each — `O(owners)` round trips rather than one per delete
per owner — and only a peer explicitly answering `Unimplemented` is asked the
old way, one delete at a time. Probes and corrective handoffs share a ten-second
network deadline per owner-poll invocation; this does not bound the entire GC
cycle, which also scans metadata and releases shards. A transport failure is
unknown, not a negative answer, and does not trigger a sequence of handoffs to
the unreachable owner. Unconfirmed tombstones remain retained. The report of a
sweep adds up: `tombstones_scanned` is what the
cycle examined, and `tombstones_retained`, `tombstones_shards_released` and
`tombstones_unclaimed_by_any_set` are the three things that can happen to it.

`POST /api/admin/ec/tombstones/gc` runs one sweep on the node it is
addressed to, for an acceptance test that needs the result at a chosen moment
rather than on a schedule. Like the shards it releases, it is node-local:
sweeping a pool means asking each of its nodes. The request accepts
`{"action":"dry_run|run_once","max_entries":512,"cursor":"..."}`; omit `cursor`
for the first page and pass the opaque `next_cursor` into the next request until
`round_completed` is true. Every page reports `manifests_examined`, and both
dry-run and mutation responses expose the same completion and counter fields.
Manual cursors do not move the durable cursor used by the background worker.

### Auto-Tiering (EC v2, staged rollout)

Defaults are chosen so that enabling auto-tiering does not disturb user traffic;
raise them deliberately and watch the queue depth while you do. The reverse
direction — bringing a hot object back to RF=3 — is implemented and off by
default: `S4_EC_TIERING_PROMOTE_AFTER_READS=0` means no object comes back on its
own, while the manual rollback below stays available at all times.

**Deferred writes are what make the feature mean anything.** By default an EC
pool encodes every object above `S4_EC_MIN_OBJECT_SIZE` on its way in, which
leaves the migration worker nothing to find. Setting
`S4_EC_TIERING_DEFER_NEW_WRITES=true` keeps new writes on their RF=3 replicas
and lets tiering decide later, which is the "set and forget" behaviour: recent
data is replicated and fast, cold data pays 1.5x instead of 3x. Read the
reversibility note below before turning it on.

**Objects assembled from multipart parts are not migrated by the tiering
worker.** Such an object is stored as a manifest of its segments and has no
hash of the whole, so the worker cannot give the transcode a source check it
could pass, and it leaves the object on its RF=3 replicas. A multipart object
written into an EC pool the ordinary way is still encoded on its way in: there
the completion computes the hash itself.

| Variable | Default | Description |
|----------|---------|-------------|
| `S4_EC_TIERING_DEFER_NEW_WRITES` | `false` | Keep new client writes on RF=3 replicas and let the tiering worker encode them once they go cold. Requires `S4_EC_TIERING_ENABLED=true`; the node refuses to start otherwise, because nothing would ever encode |
| `S4_EC_TIERING_HEAT_ENABLED` | follows `S4_EC_TIERING_ENABLED` | Record object access marks. Enable it alone to collect access data before the first migration |
| `S4_EC_TIERING_HEAT_RESOLUTION_SECS` | `3600` | How much newer an access mark must be before it is written durably |
| `S4_EC_TIERING_HEAT_FLUSH_SECS` | `60` | Interval between flushes of collapsed access signals |
| `S4_EC_TIERING_HEAT_QUEUE_CAPACITY` | `4096` | Bounded access-signal channel; a full channel drops signals instead of blocking the response |
| `S4_EC_TIERING_SCAN_INTERVAL_SECS` | `3600` | Pause between tiering scan cycles |
| `S4_EC_TIERING_ENQUEUE_PER_CYCLE` | `64`, lowered to fit | Objects one cycle may queue for transcode. Set explicitly, it must not exceed the queue depth or the scan limit; left unset, it adapts to them |
| `S4_EC_TIERING_MAX_QUEUE_DEPTH` | `1024` | Ceiling of outstanding tiering-originated transcode jobs. Jobs from user writes are not counted |
| `S4_EC_TIERING_JITTER_PERCENT` | `10` | Deterministic spread of the cold-after threshold, so one ingest burst does not cool down at once |
| `S4_EC_TIERING_PROMOTE_AFTER_READS` | `0` | Reads within the promotion window that return an object to RF=3. `0` disables automatic reverse tiering |
| `S4_EC_TIERING_PROMOTE_WINDOW_SECS` | `86400` | Width of the promotion counting window |
| `S4_EC_TIERING_MIN_MIGRATION_INTERVAL_SECS` | `604800` | Base pause between two migrations of the same object; it grows with each migration |
| `S4_EC_TIERING_PROMOTE_PER_CYCLE` | `16` | Objects one scan cycle may send back to RF=3. Lower than the forward limit because a promotion decodes a whole object and then writes three copies of it |
| `S4_EC_TIERING_MAX_PROMOTION_QUEUE_DEPTH` | `256` | Ceiling of outstanding reverse migrations. Counted apart from the forward queue, so a rollback stays possible while the queue of migrations being undone is full |
| `S4_EC_TIERING_PROMOTE_POLL_SECS` | `60` | Pause between two passes over the reverse migration queue |
| `S4_EC_TIERING_PROMOTE_BATCH_SIZE` | `8` | Reverse migrations advanced by one pass |
| `S4_EC_TIERING_PROMOTE_MAX_ATTEMPTS` | `10` | Attempts one reverse migration gets before it is given up on |
| `S4_EC_TIERING_PROMOTE_RETRY_BACKOFF_SECS` | `60` | Base pause before a failed reverse migration step is retried; it grows linearly, to a ceiling of ten times this |
| `S4_EC_PROMOTED_GC_GRACE_SECS` | `3600` | How long the shards of a promoted object are kept after readers move to the RF=3 copy. Must be shorter than `S4_EC_TIERING_MIN_MIGRATION_INTERVAL_SECS`, and the node refuses to start otherwise — see below |

Auto-tiering never changes what an S3 client sees. The object body, `ETag`,
`Content-Length`, `Content-Type`, `Last-Modified` and `x-amz-storage-class` are
identical before and after a migration in either direction.

**Reversibility.** Turning `S4_EC_TIERING_ENABLED` off stops new migrations and
drops queued jobs, but objects already migrated stay erasure coded. Bringing
them back is a separate, explicit act:

```bash
# What would come back, without moving anything
curl -s -X POST http://localhost:9000/api/admin/ec/tiering/mybucket/promote \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"prefix": "archive/2024/", "dry_run": true}'

# Bring it back
curl -s -X POST http://localhost:9000/api/admin/ec/tiering/mybucket/promote \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"prefix": "archive/2024/"}'
```

The command works whether or not automatic reverse tiering is switched on, and
whether or not `S4_EC_TIERING_ENABLED` is set — an operator who turned the
feature off during an incident must not lose the way to undo what it did. It
also ignores the pause between two migrations of one object: that pause guards
against a heuristic changing its mind, not against a person who has decided.

#### How a migration is kept safe

An object is queued only after every one of these holds:

1. The bucket policy and the node configuration both allow it, and the object is
   the current version, is not a delete marker, and is within the size bounds.
2. It has been idle for longer than the cold-after threshold — measured against
   the newest access mark **any node of the set holds**, not just the local one.
   Marks are recorded by whichever node served the read, so the deciding node
   asks its peers before committing to a migration.
3. Admission has room: at most `S4_EC_TIERING_ENQUEUE_PER_CYCLE` objects per
   cycle, and at most `S4_EC_TIERING_MAX_QUEUE_DEPTH` tiering jobs in flight.

The migration itself runs through the same staged transcode pipeline as a client
write, which means the same guarantees: shards are written and verified, the
manifest is committed through quorum, and the object is read back from EC before
anything is switched. Immediately before the layout switch — and again straight
after it — the object's generation, size, ETag and content hash are compared
against what was encoded. Any difference means a client wrote during the
migration, and the client wins: the switch is refused or rolled back, and the
shards become orphan candidates for GC. Hot RF=3 replicas are released only
after the switch has held and `S4_EC_HOT_REPLICA_GC_GRACE_SECS` has passed.

#### How a reverse migration is kept safe

Bringing an object back is the more expensive direction and the one with more to
lose, so it runs as five durable steps, and the order is the guarantee:

1. The object is decoded from its shards and checked against the manifest — size
   and SHA-256 both.
2. The manifest is committed through quorum as `promoting`, and only then are
   those bytes written back to the RF=3 replicas, guarded by the object's
   generation. A client PUT that landed while the shards were being decoded is
   newer than these bytes, and its write wins: the promotion is dropped.
3. The RF=3 copy is read back through quorum and confirmed to be the object.
4. In the same pass, a promoted manifest is committed through quorum. This is
   the step that switches readers to the RF=3 copy, across the whole pool.
5. Only after `S4_EC_PROMOTED_GC_GRACE_SECS` are the shards and the manifest
   removed.

Steps 3 and 5 are never merged. Until the new copy has been read back and given
time to settle, the shards are the only copy known to work, and a promotion that
cannot confirm its own result leaves them exactly where they are. A copy that
does not read back is written again, and each such round counts against
`S4_EC_TIERING_PROMOTE_MAX_ATTEMPTS`.

The `promoting` state in step 2 is what keeps the restored copy in place. EC
anti-entropy reconciles each object's namespace entry with its manifest on
every node, and releases the hot copies an unfinished purge left behind on some
replica. A copy restored by a promotion has the generation and layout of such a
leftover, and the pass tells the two apart only by asking the metadata quorum:
it releases hot copies only while the quorum still says `hot_replicas_purged`.
Because `promoting` is at quorum before the first byte comes back, no node can
see the restored copy without also seeing it. Readers still use erasure coding
while the manifest is `promoting`, and repair and scrubbing still maintain its
shards.

A promotion that gives up after the mark leaves the manifest `promoting`: the
object keeps reading from erasure coding, but the copies it restored stay where
they are. Running the rollback command again for the object, through a node
that has read it or carried out the promotion, resumes it.

The manifest published in step 4 is a marker, not a resting state: step 5 also
removes it from the metadata quorum. It has to go, because a promotion preserves
the object's generation, and a later migration of that same generation produces
a manifest with the same clock value — which the marker outranks, deliberately,
since that is what makes a promotion switch readers at all. A marker left behind
would therefore win against the copy that replaced it. The window between
publishing the marker and collecting it is covered by the pause between two
migrations of one object, which is why the node refuses to start when
`S4_EC_PROMOTED_GC_GRACE_SECS` is not shorter than
`S4_EC_TIERING_MIN_MIGRATION_INTERVAL_SECS`.

The object is readable throughout. Before step 4 every node still resolves it
through erasure coding, and all its shards are there; from step 4 on every node
resolves it through the hot path, and all its replicas are there. There is no
moment when neither answers, and nothing a client can see changes — including
`Last-Modified`, which an ordinary rewrite would have moved.

**One generation of an object is encoded at most once.** An object brought back
to RF=3 stays there until a client writes it again — it is not re-encoded when
it goes quiet a second time. Manifests of a generation are ordered by that
generation's clock value, and a promotion preserves the generation, so a second
encoding of it could not be ordered against the first. The cost is space on
objects that came back and stayed idle; nothing about their availability or
integrity changes.

An object cannot oscillate between the two tiers. The thresholds measure
different things (idle time going out, read frequency coming back) and cannot be
ordered against each other, so what separates them is time: after each migration
an object must wait `S4_EC_TIERING_MIN_MIGRATION_INTERVAL_SECS`, doubling with
each move it has already made, up to 32 times the base.

#### Watching it work

| Metric | What it tells you |
|--------|-------------------|
| `s4_ec_tiering_scanned_total` | Objects examined. Flat means the worker is not running or every bucket has tiering off |
| `s4_ec_tiering_queued_total` | Objects admitted for migration |
| `s4_ec_tiering_queued_bytes_total` | Bytes admitted; the useful one for estimating how long a backlog will take |
| `s4_ec_tiering_skipped_total` | Objects examined and left alone. Close to `scanned` is the healthy steady state |
| `s4_ec_tiering_queue_depth` | Tiering jobs in flight. Sitting at `S4_EC_TIERING_MAX_QUEUE_DEPTH` means admission, not encoding, is the bottleneck |
| `s4_ec_tiering_cancelled_total` | Migrations abandoned because the object changed under them |
| `s4_ec_tiering_scan_cycles_total` | Completed cycles |
| `s4_ec_tiering_promotion_restored_total` | Objects whose RF=3 copy has been written back. The gap against `promotion_committed_total` is how many are currently sitting on two copies of themselves |
| `s4_ec_tiering_promotion_restored_bytes_total` | Bytes written back to RF=3 |
| `s4_ec_tiering_promotion_committed_total` | Objects whose readers have been switched to the RF=3 copy |
| `s4_ec_tiering_promotion_purged_total` | Promoted objects whose shards and manifest are gone |
| `s4_ec_tiering_promotion_superseded_total` | Reverse migrations dropped because a client wrote the object |
| `s4_ec_tiering_promotion_failed_total` | Reverse migrations that exhausted their retries. Every one of these needs an operator: the object is intact and readable, but it is holding an unused RF=3 copy alongside its shards |
| `s4_ec_tiering_promotion_queue_depth` | Reverse migrations in flight |

`GET /api/admin/ec/test/promotion/job/{bucket}/{key}` shows this node's queue
entry for one object, which is the difference between "nothing was queued" and
"it is queued and waiting". It needs `S4_EC_TEST_HOOKS_ENABLED=true`.

Before turning anything on, `POST /api/admin/ec/tiering/dry-run` reports what
the worker would do, broken down by reason, and changes nothing. If it reports
that everything is still warm, the thresholds are the thing to look at; if it
reports everything below the size floor, the profile is.

#### Mixed-Cluster Gating During Rolling Upgrades

A migration into erasure coding changes how an object is stored for the whole
pool, so it must not start while any member still runs software that cannot
follow it. Before it looks at a single object, every tiering cycle asks cluster
membership whether all the nodes holding a slot of this pool advertise protocol
version 2 or higher. If they do not, the cycle stops with
`stop: "mixed_cluster"` and queues nothing.

The gate is fail-closed, and "confirmed" is the operative word. Three different
situations all answer "no":

- a node still runs older software and advertises protocol version 1;
- a node has joined membership but its metadata has not arrived yet, so it is
  still carrying the placeholder an operator never sees;
- a node in the pool has not joined membership at all.

The second is ordinary and short-lived — it lasts until the joining node's first
metadata broadcast, which every existing member triggers by rebroadcasting its
own as soon as it sees the join. The third is not: a pool member that never
appears means the node is down, misconfigured, or unreachable, and that is worth
investigating for its own sake. A node that has joined and then died keeps its
published metadata, so rebooting one node does not pause tiering for the pool.

The verdict is read fresh on every cycle, never cached at startup. A node can be
restarted into older software at any point during a rolling upgrade, and a
verdict taken at boot would not notice. It also means finishing the upgrade is
all it takes: migrations resume by themselves, with no restart and nothing to
switch back on.

Only forward migration is gated. Reverse tiering keeps running, because it moves
objects back to RF=3 — a layout every version understands — and because it
carries the manual rollback, which has to work precisely when the cluster is in
a state nobody planned.

Read the gate from the pool health endpoint:

```bash
curl -s http://localhost:9000/api/admin/ec/pools/my-pool/health \
  -H "Authorization: Bearer $TOKEN" | jq '.config.tiering_cluster_supported'
```

A node that was never given a membership handle at all reports `false` here and
refuses to start its tiering worker, logging why. That is a wiring fault rather
than a mixed cluster, and the two are distinguished in the log rather than left
for the operator to guess.

#### Crash Resilience and Node Failures

Hard node termination — `kill -9`, power loss, a partition — is safe at every
stage of auto-tiering, and each stage is safe for a different reason:

- **Scanning and candidate selection.** The worker keeps its sweep position in
  the `ec_tiering_cursor` keyspace. A crash costs at most the objects examined
  since the last saved cursor, and the restarted worker resumes from it. Nothing
  has been written yet, so there is nothing to undo.
- **Transcode and staging.** Shards are written to the slots they will
  permanently occupy — there is no temporary staging area — but they are
  invisible until the manifest naming them is committed to quorum. A crash
  therefore leaves shards that no manifest references, which is exactly what
  orphan GC collects. The transcode job itself is keyed by an operation id
  derived from the source object's generation, so a retry after the crash
  re-encodes the same bytes rather than racing its own leftovers, and the RF=3
  replicas stay intact and authoritative throughout.

  The retry is not immediate, and that is deliberate. A job caught in an
  in-progress state holds a lease, and no worker will pick it up again until
  that lease expires — otherwise a node that is merely slow, or briefly
  unreachable, would be raced by a second encoder writing the same slots. So a
  migration interrupted by a hard crash resumes about
  `S4_EC_TRANSCODE_LEASE_SECS` (default 300) after the crash, not seconds after
  the node comes back. Until then the object is served from its RF=3 replicas
  and nothing is lost; if migrations appear stalled shortly after a node
  restart, this is the first thing to rule out.
- **Layout cutover.** The switch is guarded by `EcCutoverGuard`, which carries
  the generation HLC, size, ETag and SHA-256 the shards were produced from. If a
  client overwrote the object while it was being encoded, the guard no longer
  matches and the cutover is abandoned: the client's newer data wins, and the
  shards become orphans for GC to reclaim.
- **Hot purge.** The RF=3 replicas are removed only after the EC copy has been
  read back and verified, and only after `S4_EC_HOT_REPLICA_GC_GRACE_SECS` has
  elapsed. A crash before that leaves the object with both copies, which costs
  space and nothing else.
- **Reverse tiering.** The RF=3 copy is re-attached through the
  generation-guarded `attach_object_data_if_hlc` primitive. Until the promoted
  manifest is committed to quorum, readers keep being served from the shards; a
  crash before the commit leaves them there. Shards are scheduled for collection
  only after the verified quorum read and the commit, and then only once
  `S4_EC_PROMOTED_GC_GRACE_SECS` has passed.

In every case the invariant is the same: the object never has fewer than one
complete, readable copy, and whatever the crash left behind is either ignored or
reclaimed rather than served.

#### Diagnosing Stuck or Inactive Migrations

If cold objects do not seem to be migrating, work down this list.

**1. Ask the dry run why.** `POST /api/admin/ec/tiering/dry-run` reports what a
cycle would do without doing it, broken down by reason. The counts appear under
`skipped`, and every reason is one of:

| Reason | Meaning |
|--------|---------|
| `disabled` | Tiering is off for this node or this bucket |
| `not_current_version` | Not the current version of the object |
| `delete_marker` | A delete marker has no bytes to move |
| `already_erasure_coded` | Already migrated, and not read often enough to come back |
| `too_small` | Below `S4_EC_MIN_OBJECT_SIZE`, or the bucket's own floor if it set a higher one |
| `too_large` | Above what the transcode worker reads in one piece |
| `still_warm` | Used more recently than `S4_EC_TIERING_COLD_AFTER_SECS`, counting reads reported by every node in the pool |
| `excluded` | Held in place by a bucket prefix or tag exclusion |
| `recently_migrated` | Inside the hysteresis pause that follows a migration, which starts at `S4_EC_TIERING_MIN_MIGRATION_INTERVAL_SECS` and grows with each one |

"Everything is `still_warm`" and "everything is `excluded`" lead to opposite
next steps, which is why the breakdown is worth reading before changing
anything.

**2. Check the mixed-cluster gate.** `config.tiering_cluster_supported` in
`/api/admin/ec/pools/{pool}/health` must be `true`; see the section above for
what a `false` means and which of its causes resolve on their own.

**3. Check the queues.** The same numbers are in the health response under
`counts` and on `/metrics`:

- `s4_ec_tiering_queue_depth` — tiering-originated transcodes in flight. It
  saturates at `S4_EC_TIERING_MAX_QUEUE_DEPTH` (reported as
  `counts.tiering_queue_ceiling`), and sitting at the ceiling means the pool has
  no admission headroom rather than that it has exactly that many jobs. When it
  stays there, the transcode side is the constraint: look at
  `S4_EC_CPU_WORKERS`, `S4_EC_CPU_BUDGET_PERCENT` and
  `S4_EC_TRANSCODE_BANDWIDTH_LIMIT`.
- `s4_ec_tiering_promotion_queue_depth` — reverse migrations in flight.
- `s4_ec_tiering_cancelled_total` — migrations abandoned because the object was
  overwritten or deleted under them. A rising count means clients are actively
  writing what the worker is trying to move, which is a workload observation,
  not a fault.
- `s4_ec_tiering_scanned_total` against `s4_ec_tiering_queued_total` — how much
  of the catalog is being walked for how little work.

Every `s4_ec_tiering_*` metric is registered at zero when the node starts, so a
series that reads zero means zero, not "the exporter has not seen one yet".

**4. Look at one object.** Two routes show a single object's place in the
queues, and between them they separate "nothing was queued" from "it is queued
and waiting":

- `GET /api/admin/ec/test/transcode/job/{bucket}/{key}` — the forward migration.
  This is the only way to see a migration that has started but not finished: a
  staged transcode publishes no manifest until it commits, so until then
  `GET /api/admin/ec/objects/{bucket}/{key}/manifest` answers 404 for an object
  that is being encoded right now. `in_flight` says whether work is outstanding,
  and `job.state` says how far it has got.
- `GET /api/admin/ec/test/promotion/job/{bucket}/{key}` — the reverse migration.

Both are node-local, because both queues are: a migration is carried out by the
node that admitted it, so a pool is diagnosed by asking each node rather than
whichever one the balancer picked. The manifest route
(`GET /api/admin/ec/objects/{bucket}/{key}/manifest`) is the opposite: it reads
through quorum, so every node answers the same thing about an object's state
even when its own copy is behind. Both need `S4_EC_TEST_HOOKS_ENABLED=true`, so
they are diagnostics to switch on deliberately, not something to leave enabled.

**5. Stop a transcode at a named state.** The staged states are too short-lived
to look at from outside: an object passes through `ec_encoding` and
`ec_shards_durable` in the time it takes to write its shards, and by the time a
question about one of them is answered the object is somewhere else. The
crash-safety scenarios need the object to sit still, so
`POST /api/admin/ec/test/transcode/pause` holds it:

```json
{"action": "arm", "bucket": "b", "key": "k", "state": "ec_shards_durable", "mode": "pause_once"}
```

`action` is `arm`, `status` or `release`; `state` is one of
`ec_transcode_queued`, `ec_encoding`, `ec_shards_durable`,
`ec_manifest_committed`, `ec_read_verified`. A `status` call answers `armed` —
whether a pause is set for that object and state — and `paused` — whether a
transcode is being held there right now — along with the operation holding it;
`release` lets it go on. Both answers come from the pause itself, so `status` on
an object nobody armed says `armed: false` rather than echoing the request. The pause is node-local, like the queues themselves, and is dropped when the
held transcode continues — so a node that is restarted comes back with nothing
armed.

Like every other route in this section it needs `S4_EC_TEST_HOOKS_ENABLED=true`,
and a node without it pays one atomic read per state transition and nothing
else. A pause that is never released is not permanent either: after ten minutes
the transcode goes on and says so in the log, so a forgotten hook costs a delay
rather than a stuck worker.

**6. Look for shards nothing references.** A transcode that fails before it
commits its manifest leaves shards on disk that no manifest points at. They are
orphan candidates: they cost space and protect nothing, and GC removes them once
they are old enough and no transcode lease is still alive.
`POST /api/admin/ec/test/orphans/scan` runs that sweep now and records what it
finds, so `GET /api/admin/ec/orphans` can be read back for the answer. The
background scrub looks for the same thing on its own schedule.

The sweep only records; it never deletes. Collecting is
`POST /api/admin/ec/orphans/gc`, an ordinary operator route, and it still
refuses a candidate that is too young or whose transcode lease was seen
recently. Also needs `S4_EC_TEST_HOOKS_ENABLED=true`.

## Run Examples

Every node in an EC pool uses the same `S4_CLUSTER_NAME`, `S4_SEEDS`,
`S4_POOL_NAME`, `S4_POOL_NODES`, `S4_POOL_TYPE`, `S4_EC_PROFILE`, topology
labels, and license. Each node must use a unique `S4_NODE_ID`,
`S4_NODE_GRPC_ADDR`, `S4_NODE_HTTP_ADDR`, and `S4_DATA_DIR`.

### 6-Node RS Small Validation Pool

Use this for a compact validation environment. It requires exactly 6 nodes.

```bash
export S4_LICENSE_KEY=your-license-key
export S4_MODE=cluster
export S4_CLUSTER_NAME=s4-ec-dev
export S4_POOL_TYPE=ec
export S4_POOL_NAME=ec-rs-small-pool
export S4_EC_PROFILE=ec-rs-small
export S4_EC_TOPOLOGY_POLICY=warn
export S4_SEEDS=10.10.0.11:9100,10.10.0.12:9100,10.10.0.13:9100
export S4_POOL_NODES=node-1:10.10.0.11:9100,node-2:10.10.0.12:9100,node-3:10.10.0.13:9100,node-4:10.10.0.14:9100,node-5:10.10.0.15:9100,node-6:10.10.0.16:9100

# Node 1
S4_NODE_ID=node-1 \
S4_NODE_GRPC_ADDR=10.10.0.11:9100 \
S4_NODE_HTTP_ADDR=10.10.0.11:9000 \
S4_DATA_DIR=/var/lib/s4/node-1 \
./target/release/s4-server
```

Start nodes 2-6 with the same shared variables and their own node ID, addresses,
and data directory.

### 9-Node RS Standard Pool

This is the default EC profile shape. It requires exactly 9 nodes.

```bash
export S4_LICENSE_KEY=your-license-key
export S4_MODE=cluster
export S4_CLUSTER_NAME=s4-ec-prod
export S4_POOL_TYPE=ec
export S4_POOL_NAME=ec-rs-standard-pool
export S4_EC_PROFILE=ec-rs-standard
export S4_EC_TOPOLOGY_POLICY=strict
export S4_SEEDS=10.20.0.11:9100,10.20.0.12:9100,10.20.0.13:9100
export S4_POOL_NODES=node-1:10.20.0.11:9100,node-2:10.20.0.12:9100,node-3:10.20.0.13:9100,node-4:10.20.0.14:9100,node-5:10.20.0.15:9100,node-6:10.20.0.16:9100,node-7:10.20.0.17:9100,node-8:10.20.0.18:9100,node-9:10.20.0.19:9100
export S4_EC_NODE_TOPOLOGY='node-1:zone=z1,rack=r1,host=h1,disk_group=d1;node-2:zone=z2,rack=r2,host=h2,disk_group=d2;node-3:zone=z3,rack=r3,host=h3,disk_group=d3;node-4:zone=z1,rack=r4,host=h4,disk_group=d4;node-5:zone=z2,rack=r5,host=h5,disk_group=d5;node-6:zone=z3,rack=r6,host=h6,disk_group=d6;node-7:zone=z1,rack=r7,host=h7,disk_group=d7;node-8:zone=z2,rack=r8,host=h8,disk_group=d8;node-9:zone=z3,rack=r9,host=h9,disk_group=d9'

# Node 1
S4_NODE_ID=node-1 \
S4_NODE_GRPC_ADDR=10.20.0.11:9100 \
S4_NODE_HTTP_ADDR=10.20.0.11:9000 \
S4_DATA_DIR=/var/lib/s4/node-1 \
./target/release/s4-server
```

### 10-Node LRC Standard Pool

Use LRC when local repair locality is more important than pure RS simplicity.
It requires exactly 10 nodes.

```bash
export S4_LICENSE_KEY=your-license-key
export S4_MODE=cluster
export S4_CLUSTER_NAME=s4-ec-lrc
export S4_POOL_TYPE=ec
export S4_POOL_NAME=ec-lrc-standard-pool
export S4_EC_PROFILE=ec-lrc-standard
export S4_EC_TOPOLOGY_POLICY=strict
export S4_SEEDS=10.30.0.11:9100,10.30.0.12:9100,10.30.0.13:9100
export S4_POOL_NODES=node-1:10.30.0.11:9100,node-2:10.30.0.12:9100,node-3:10.30.0.13:9100,node-4:10.30.0.14:9100,node-5:10.30.0.15:9100,node-6:10.30.0.16:9100,node-7:10.30.0.17:9100,node-8:10.30.0.18:9100,node-9:10.30.0.19:9100,node-10:10.30.0.20:9100

# Node 1
S4_NODE_ID=node-1 \
S4_NODE_GRPC_ADDR=10.30.0.11:9100 \
S4_NODE_HTTP_ADDR=10.30.0.11:9000 \
S4_DATA_DIR=/var/lib/s4/node-1 \
./target/release/s4-server
```

### Custom RS(6,3)

This custom profile is equivalent in shape to `ec-rs-standard`, but shows the
custom profile syntax.

```bash
export S4_POOL_TYPE=ec
export S4_EC_PROFILE=custom
export S4_EC_CODEC=rs
export S4_EC_K=6
export S4_EC_RS_M=3
export S4_EC_MIN_OBJECT_SIZE=1048576
export S4_EC_CHUNK_SIZE=6291456
```

The pool must still contain exactly `S4_EC_K + S4_EC_RS_M` nodes.

### Custom LRC(8,2,2)

```bash
export S4_POOL_TYPE=ec
export S4_EC_PROFILE=custom
export S4_EC_CODEC=lrc
export S4_EC_K=8
export S4_EC_LRC_L=2
export S4_EC_LRC_G=2
```

The pool must contain exactly `8 + 2 + 2 = 12` nodes.

## Docker

Docker uses the same environment variables. The image must be the EE image and
must receive a license key or license file.

### Docker Compose Dev Cluster

For local development, the repository includes a ready-to-run Enterprise EC
cluster in `ee/docker-compose-dev.yml`. It starts one erasure-coded pool
(`S4_POOL_TYPE=ec`) with the `ec-rs-small` profile — RS(4,2), one shard per
node, so 6 nodes — HAProxy on port `9000`, and HAProxy stats on port `8404`.
Objects of at least 1 MiB are written at RF=3 and transcoded into EC shards in
the background; smaller ones stay at RF=3.

Two things are needed before it starts:

- **A license.** Erasure coding needs an Enterprise license with the
  `erasure_coding` capability. The license file is not part of the repository:
  it is issued by the S4Core sales team — contact
  [support@s4core.com](mailto:support@s4core.com). Save it as
  `ee/license-key.txt`, or point `S4_LICENSE_HOST_FILE` at it. Without the file,
  compose refuses to start the nodes.
- **An EE image with erasure coding.** The default is `s4core/s4core:ee`; an EE
  image from a release before erasure coding ignores every `S4_EC_*` variable
  and starts an ordinary replicated pool. Until a release with erasure coding is
  published, build the image from the repository and select it with
  `S4_EE_IMAGE`.

Run from the repository root:

```bash
# License from ee/license-key.txt, image s4core/s4core:ee
docker compose -f ee/docker-compose-dev.yml up -d

curl http://127.0.0.1:9000/health
curl http://127.0.0.1:8404/
```

Use a local or freshly built EE image by overriding `S4_EE_IMAGE`:

```bash
docker build --build-arg EDITION=ee -t s4core-ee:local .
S4_EE_IMAGE=s4core-ee:local \
  docker compose -f ee/docker-compose-dev.yml up -d
```

Use a license file stored elsewhere by overriding `S4_LICENSE_HOST_FILE`:

```bash
S4_LICENSE_HOST_FILE=/absolute/path/to/license.key \
  docker compose -f ee/docker-compose-dev.yml up -d
```

The dev compose disables S3 auth by default (`S4_DISABLE_AUTH=1`) so basic smoke
checks can use plain `curl`:

```bash
truncate -s 2097152 /tmp/s4-ec-smoke.bin
curl -fsS -X PUT http://127.0.0.1:9000/ec-smoke
curl -fsS -X PUT http://127.0.0.1:9000/ec-smoke/large.bin \
  --data-binary @/tmp/s4-ec-smoke.bin
curl -fsS http://127.0.0.1:9000/ec-smoke/large.bin \
  -o /tmp/s4-ec-smoke.out
sha256sum /tmp/s4-ec-smoke.bin /tmp/s4-ec-smoke.out
```

Reset the dev cluster and remove its named volumes:

```bash
docker compose -f ee/docker-compose-dev.yml down -v
```

An Enterprise cluster without erasure coding — six nodes in one replicated pool,
RF=3 — is in `ee/docker-compose-dev-no-ec.yml`. It takes the same license file
and the same `S4_EE_IMAGE` and `S4_LICENSE_HOST_FILE` overrides, and keeps S3
authentication on (`my-access-key-one` / `my-secret-key-one` by default).

### Single Docker Node Example

```bash
docker run -d \
  --name s4-ec-node-1 \
  -p 9000:9000 \
  -p 9100:9100 \
  -v s4-ec-node-1:/data \
  -e S4_BIND=0.0.0.0:9000 \
  -e S4_LICENSE_KEY=your-license-key \
  -e S4_MODE=cluster \
  -e S4_POOL_TYPE=ec \
  -e S4_EC_PROFILE=ec-rs-small \
  -e S4_NODE_ID=node-1 \
  -e S4_NODE_GRPC_ADDR=10.10.0.11:9100 \
  -e S4_NODE_HTTP_ADDR=10.10.0.11:9000 \
  -e S4_SEEDS=10.10.0.11:9100,10.10.0.12:9100,10.10.0.13:9100 \
  -e S4_POOL_NAME=ec-rs-small-pool \
  -e S4_POOL_NODES=node-1:10.10.0.11:9100,node-2:10.10.0.12:9100,node-3:10.10.0.13:9100,node-4:10.10.0.14:9100,node-5:10.10.0.15:9100,node-6:10.10.0.16:9100 \
  s4core/s4core:ee
```

Repeat for the remaining nodes with their own node ID and advertised addresses.

## Admin API

EC admin routes are available under `/api/admin/ec/*` and require a SuperUser
JWT token. CE builds and unlicensed EE builds return an Enterprise-required
error.

```bash
TOKEN=$(curl -s -X POST http://localhost:9000/api/admin/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"root","password":"password12345"}' | jq -r '.token')

curl -s http://localhost:9000/api/admin/ec/pools \
  -H "Authorization: Bearer $TOKEN"

curl -s http://localhost:9000/api/admin/ec/pools/ec-rs-small-pool/health \
  -H "Authorization: Bearer $TOKEN"

curl -s http://localhost:9000/api/admin/ec/pools/ec-rs-small-pool/repair-status \
  -H "Authorization: Bearer $TOKEN"

# The same route, asked about one slot owner, also answers whether that node's
# shards are all back. See "Rebuilding a node's shards".
curl -s "http://localhost:9000/api/admin/ec/pools/ec-rs-small-pool/repair-status?node-id=0d3bb4d8-876d-4b6a-a27f-49a69d11d3ae" \
  -H "Authorization: Bearer $TOKEN"

curl -s http://localhost:9000/api/admin/ec/orphans \
  -H "Authorization: Bearer $TOKEN"

curl -s -X POST http://localhost:9000/api/admin/ec/orphans/gc \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"dry_run":true,"max_entries":128}'
```

Node replacement adds one route, answered by the node it names:

```bash
curl -s -X POST http://localhost:9000/api/admin/ec/pools/ec-rs-small-pool/rejoin/approve \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"node_id":"0d3bb4d8-876d-4b6a-a27f-49a69d11d3ae"}'
```

See [Coming back to the pool](#coming-back-to-the-pool) for what it accepts and
what it deliberately does not.

Object manifest and verify endpoints use the object key followed by the
operation suffix:

```bash
curl -s "http://localhost:9000/api/admin/ec/objects/mybucket/path/to/object.bin/manifest" \
  -H "Authorization: Bearer $TOKEN"

curl -s -X POST "http://localhost:9000/api/admin/ec/objects/mybucket/path/to/object.bin/verify" \
  -H "Authorization: Bearer $TOKEN"
```

Both answer from the manifest the pool's quorum holds, not from the copy the
node answering happens to have cached, so every node gives the same answer —
also right after the object was overwritten through another node.
`full_data_verify` compares the bytes it read with the manifest they were
decoded by.

### Duplicate content report

```bash
curl -s http://localhost:9000/api/admin/ec/dedup/report \
  -H "Authorization: Bearer $TOKEN"
```

Reports how much erasure-coded content the pool stores more than once. It reads
manifests and writes nothing, so it is safe to run against a live pool.

Query parameters:

| Parameter | Default | Meaning |
|---|---|---|
| `max-entries` | 1024 | Manifests one node may walk. Each node clamps it to its own `S4_EC_ADMIN_SCAN_LIMIT` |
| `local-only` | `false` | Measure this node's manifests only, without asking the pool |

The answer has four parts:

- `scope` — how much of the pool was actually read: `nodes_answered` of
  `nodes_total`, `nodes_unread` naming every node that did not answer and why,
  `nodes_truncated` counting the nodes whose walk stopped at the scan limit, and
  `complete`, which is true only when every node answered in full.
- `totals` — the population the shares are taken over: distinct erasure-coded
  objects, their bytes, distinct content hashes, and the objects left out
  because they live inside a packed segment.
- `within_set` — what sharing shard sets could actually save. Only repeats
  inside one erasure set count: shards live on the nodes of a specific set, so a
  manifest of another set could not reference them. `ignoring_set_boundary` and
  `lost_to_set_boundary_bytes` show what that boundary costs.
- `decision` — the `verdict` (`go`, `conditional`, `no_go` or `inconclusive`),
  the `reason` for it, and the thresholds it was read against.

**A report that did not cover the pool decides nothing.** A node that was not
read is not a node without duplicates, and a walk that stopped at the scan limit
covered a prefix of the store rather than the store. In either case `verdict` is
`inconclusive` and `reason` says which condition failed — raise
`S4_EC_ADMIN_SCAN_LIMIT` above the pool's manifest count, or wait for every node
to be reachable, and measure again.

Why the walk asks every node: a node holds the manifests it transcoded plus the
ones its read path resolved, so on its own it sees a fraction of the pool, and a
repeat whose two objects were transcoded on different nodes is invisible on
both. `local-only=true` answers the narrower question and marks itself
incomplete. A node too old to know the inter-node call is counted as unread, not
as empty.

**What the numbers mean, and what they do not.** A repeat here is a repeat among
*manifests*. It is usually not a second copy on disk: shards are written through
the ordinary object path, which reuses a blob whose content hash the node
already holds, and an erasure set has a fixed slot-to-node assignment, so
identical objects encode to identical shards on identical nodes, which is why
the erasure-coding tier has no deduplication of its own. Use the report to
observe content repetition, not to estimate reclaimable space.

### Failure matrix

```bash
curl -s http://localhost:9000/api/admin/ec/pools/ec-lrc-small-pool/failure-matrix \
  -H "Authorization: Bearer $TOKEN"
```

The document lists the failure patterns the pool's profile is judged by. Every
entry names the slots it loses, whether the profile recovers it, and
`recovery_path` — `direct`, `local_xor`, `global_rs`, `unified_solver` or
`none` — which is what a degraded read of that pattern will cost. The path is
computed from the slots, never read off the entry's name: in `ec-lrc-small` the
entry named after the global Reed-Solomon budget is a single lost data shard,
and local XOR repairs it from two reads.

The document carries a `version`:

- `version: 1` — what the fast paths alone reach, the EC v1 guarantees;
- `version: 2` — the same table with the patterns only the unified solver
  reaches appended.

Version 1 is version 2 with every `unified_solver` entry dropped: the earlier
entries keep their labels, slots, verdicts and order, so the two differ by an
addition rather than by a disagreement. The document is otherwise independent of
the node that produced it, so comparing it between nodes byte for byte is a
valid check — within one version. During a rolling upgrade the two versions
coexist, and that difference is the upgrade, not a fault.

### Erasure sets of a pool

A pool holds one erasure set or several. The set an object was written into is
recorded in its manifest and never changes; adding a set adds capacity for new
objects and moves nothing that is already stored. Rebalancing existing data
between sets is deliberately out of scope and deferred to EC v3.

Growing a pool is two steps, and only the first needs a restart:

1. **Add the nodes to the pool's configuration.** The pool's node count must be a
   whole multiple of the profile width — nine nodes for `rs_standard`, so
   eighteen for a pool of two sets. Nodes are **appended**, never inserted: the
   pool's inherited set is the first `width` nodes in configured order, and that
   order is what every manifest written so far names. A node that starts with a
   reordered list refuses to adopt the pool's set list and says so, rather than
   serving objects from the wrong nodes.
2. **Form the new nodes into a set**, on a running cluster, through the admin
   API. Every other node picks the new set up within `S4_EC_SET_REFRESH_SECS`.

A set is built on nodes no other set of the pool holds. A node already carrying
slots elsewhere is refused: the pool would gain no capacity, and its free space
is the sum of its sets' — a node counted twice would promise room that is not
there.

Set management requires `S4_EC_MULTI_SET_ENABLED=true`; with it off the routes
refuse and name the switch. Listing always works — it is a read, and refusing it
would take away the only way to see what the pool holds.

```bash
# What the pool holds, and which of its nodes are still free.
curl -s http://localhost:9000/api/admin/ec/pools/my-pool/sets \
  -H "Authorization: Bearer $TOKEN"

# Form the free nodes into a set. The count must equal profile_width exactly.
FREE=$(curl -s http://localhost:9000/api/admin/ec/pools/my-pool/sets \
  -H "Authorization: Bearer $TOKEN" | jq -c '.unassigned_nodes')

curl -s -X POST http://localhost:9000/api/admin/ec/pools/my-pool/sets \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d "$(jq -nc --argjson nodes "$FREE" '{node_ids: $nodes}')"

# Stop a set taking new objects. Reads and repair carry on unchanged.
curl -s -X POST http://localhost:9000/api/admin/ec/pools/my-pool/sets/1/deactivate \
  -H "Authorization: Bearer $TOKEN"
```

The listing carries free space per set and for the pool. The pool figure is the
sum of the per-set figures and comes from the same computation, so the two
cannot disagree. A set whose nodes have not reported usably is `measured: false`
— unknown, which is not the same as full, and an operator acts on the two
differently.

Deactivating the last set that still takes writes is refused: a pool with
nowhere to write fails every new object, and finding that out on the next PUT is
worse than being told now.

#### Growing a pool while the cluster is being upgraded

A node running a binary that predates multi-set pools does not know the pool's
set list and cannot resolve a manifest naming a set it has never heard of. So a
pool refuses to grow until every one of its nodes has confirmed EC v2 support
over gossip, and while it has not, new objects go to the inherited set — the one
set every version of the binary knows. The listing answers the question:

```bash
curl -s http://localhost:9000/api/admin/ec/pools/my-pool/sets \
  -H "Authorization: Bearer $TOKEN" | jq '.all_nodes_speak_v2'
```

A node that has not answered counts as not confirmed — an unknown node reads the
same as an old one, because a guard that opens on missing input is not a guard.
Deactivation is deliberately not gated: taking a set out of the write rotation
only reduces exposure, and it has to stay available in the middle of an upgrade.

#### Planning capacity

A pool grows by whole sets: `profile_width` nodes at a time, on nodes no other
set of the pool holds. Half a set is not capacity — it is nodes waiting for the
other half, and the listing shows them as `unassigned_nodes`.

New objects are spread across the sets that take writes. `round_robin` walks them
evenly; `free_space` weighs each set by what its nodes report free, so a set
twice as large takes roughly two objects of every three rather than the first two
hundred of three hundred. A set whose nodes have not reported usable numbers is
unmeasured, and `free_space` then places exactly as `round_robin` does: guessing
that "unknown" means "empty" would be a measurement invented out of silence.

A set that has crossed `S4_EC_SET_FULL_THRESHOLD` stops taking new objects and
starts again on its own once space is freed. The ratio is measured against the
filesystem holding each node's data directory — the disk, not the pool's logical
usage — so a node that shares its filesystem with something else is judged on
what that something else has taken too. A pool whose sets are all full or all
deactivated costs space, never availability: the client's write is already
durable on its RF=3 replicas, so the object stays replicated and is encoded once
room appears. The set listing says so directly (`full: true` with the measured
`used_ratio`), and the node logs it once per object it did not encode.

This rule arrives with `S4_EC_MULTI_SET_ENABLED=true` and did not exist before
it: with the switch off, a pool hands every new object to its single set no
matter how full the disks are. Turning multi-set on for the first time on nodes
whose filesystems are already close to the threshold therefore stops erasure
coding — nothing is lost, but objects stay on their replicas, which is the
opposite of what an operator turning on a capacity feature expects. Check the
listing's `capacity` block before and after the switch.

Existing data is never moved between sets — see below.

#### What a rollback costs

Three different things are called "rolling back multi-set pools", and they cost
different amounts:

1. **Turning `S4_EC_MULTI_SET_ENABLED` off.** New sets can no longer be created
   and placement returns to the inherited set. Sets that already exist keep their
   objects and keep serving them. Fully reversible.
2. **Going back to a binary without multi-set support.** That binary cannot
   resolve a manifest naming an added set, and it refuses a pool configured wider
   than one set — so the pool's node list has to be rolled back with it. The
   objects of the added sets are then unreadable until the binary is rolled
   forward again. Nothing is lost, but the window is real, and it is the reason
   the pool refuses to grow while any node is still on the old binary.
3. **Removing a set.** There is no such operation. A set can be deactivated, so
   that it stops taking new objects while it keeps serving the ones it has.

Rebalancing existing objects between sets is deferred to EC v3. An added set
therefore fills with new objects only, and a pool expanded long after it filled
up stays lopsided until its data ages out. Plan the expansion before the pool is
full, not after.

### Replacing a node

A node's identity is not its hostname, its address or `S4_NODE_ID`. It is a UUID
in `<S4_DATA_DIR>/volumes/node_id`, generated once and never changed, and it is
the only name the pool has for the shards that node holds. A repaired shard is
written to the identity the manifest names and to no other, so **redundancy comes
back through a machine that takes over the dead node's identity** — with an empty
disk, which ordinary repair then fills.

#### The procedure, end to end

None of this is automatic, and none of it has to happen in the next five
minutes: the pool reads around a lost node for as long as `k` of `n` shards
survive. What does not wait is the next failure, which will have one less shard
to spare — so a replacement is measured in days rather than in weeks.

**Before anything fails.** Save the identities of the pool's nodes beside
whatever else you keep about them — see
[Which node holds which slots](#which-node-holds-which-slots-and-which-of-them-the-pool-can-see).
You will not need the saved copy for a pool that still has a node running, and
you will be glad of it for one that does not.

1. **Find the identity of the node that died.** Ask any surviving node for the
   pool's sets: the dead machine is the one whose `membership` is `dead` or
   `not_a_member`, and the `node_id` beside it is what the replacement has to
   become. Pool health reports the same nodes under `slot_owners.not_seen_alive`.
2. **Prepare a machine with an empty data directory.** Same pool configuration
   as the node it replaces — `S4_POOL_NAME`, `S4_POOL_NODES`, `S4_EC_PROFILE`
   and the rest — and its own `S4_EC_NODE_TOPOLOGY` labels.
3. **Start it with the dead node's identity**:
   `S4_EXPECTED_NODE_IDENTITY=<node_id>`. An empty directory adopts that
   identity; a directory holding a different one stops the start and names both.
   See [Bringing the replacement up](#bringing-the-replacement-up).
4. **Check that the pool sees it as the owner of the dead node's slots.** The
   set listing shows the identity `alive` again, with the same `slots` it always
   had. Until its shards are back the node does not serve erasure-coded reads
   and says so — see [Coming back to the pool](#coming-back-to-the-pool).
5. **Queue the rebuild** with one repair request filtered by that `node_id`, and
   repeat it while `remaining` is above zero. See
   [Rebuilding a node's shards](#rebuilding-a-nodes-shards).
6. **Wait for the pool to stop missing its shards.** `repair-status` with the
   same `node-id` answers `complete: true` when nothing of that identity is
   missing — and `null`, never `true`, when it could not tell. The queue is not
   the answer: an empty queue is equally what a rebuild nobody started looks
   like.
7. **Tell the node its return is acceptable.** `rejoin/approve`, sent to the
   node itself, puts it back in the read path. Redundancy is restored at step 6;
   this step is about that node serving reads again.

Steps 5 and 6 are the only ones that cost anything, and what they cost is
repair traffic, paced by the same limits every other repair is paced by.

Two things are not part of this procedure and must not be improvised into it:
bringing the replacement up with a **fresh** identity, and running two machines
under **one** identity. A fresh identity leaves the dead node's shards assigned
to an identity nobody holds — repair has nowhere to write, redundancy never
comes back, and nothing reports it, because the objects go on reading through
the surviving shards. Two machines under one identity write two histories of the
same slots. Both are explained below, and the server refuses the second on its
own.

#### Which node holds which slots, and which of them the pool can see

The set listing answers both, for every node of the pool:

```bash
curl -s http://localhost:9000/api/admin/ec/pools/my-pool/sets \
  -H "Authorization: Bearer $TOKEN" | jq '.membership_available, .nodes'
```

```json
[
  { "node_id": "0d3bb4d8-…", "set_id": 0, "slots": [3], "membership": "dead" },
  { "node_id": "1535d018-…", "set_id": 0, "slots": [0], "membership": "alive" },
  { "node_id": "7c1f0a44-…", "set_id": null, "slots": [], "membership": "alive" }
]
```

`membership` is what the node answering the request currently sees:

| Value | Meaning |
|-------|---------|
| `alive` | confirmed up |
| `suspect` | missed a probe, not yet confirmed either way |
| `dead` | declared unreachable |
| `left` | shut down gracefully |
| `not_a_member` | the cluster has no entry for this identity at all — nothing answers for it, and this is what a node replaced by a machine with a *fresh* identity looks like |
| `unknown` | the node answering has no membership view, so it has no opinion — check `membership_available` |

`membership_available: false` means the answer is about the node you asked, not
about the pool. An empty list of dead nodes then proves nothing, and the same
distinction is why pool health reports it too:

```bash
curl -s http://localhost:9000/api/admin/ec/pools/my-pool/health \
  -H "Authorization: Bearer $TOKEN" | jq '.slot_owners'
```

```json
{
  "membership_available": true,
  "total": 6,
  "seen_alive": 5,
  "not_seen_alive": [
    { "node_id": "0d3bb4d8-…", "set_id": 0, "slots": [3], "membership": "dead" }
  ]
}
```

`slot_owners` is reported beside `topology`, never inside it. "This node is
dead" and "this set sits in too few racks" are both risks to durability and
nothing else about them is alike: one is answered by replacing a machine, the
other by moving a set.

#### Bringing the replacement up

Write the identities down before you need them — but the pool remembers them
anyway, in the listing above and in every object manifest
(`GET /api/admin/ec/objects/<bucket>/<key>/manifest`, field
`chunks[].shards[].node_id`). Either source answers while one node of the pool is
alive.

Give the replacement an **empty** data directory and the identity of the node it
replaces:

```bash
S4_EXPECTED_NODE_IDENTITY=0d3bb4d8-876d-4b6a-a27f-49a69d11d3ae \
S4_DATA_DIR=/var/lib/s4 \
  s4-server
```

`S4_EXPECTED_NODE_IDENTITY` is a replacement tool, not an everyday setting. It
does two things and nothing else:

- an **empty** data directory adopts that identity instead of generating a fresh
  one, so the machine takes over the dead node's slots;
- a data directory that already holds a **different** identity stops the start,
  with both identities in the message.

The refusal is the point. Two identities are in play during every replacement —
the one you mean and the one on disk — and a silent disagreement means the
replacement came up for the wrong node. Nothing downstream notices: the pool
keeps assigning the dead node's slots to an identity nobody holds, the objects
go on reading through the surviving shards, and the only symptom is a rebuild
that never finishes.

Leaving the variable unset keeps the old behaviour exactly: an existing
`node_id` is honoured, an empty directory generates a new identity. Once the
replacement has started, the identity is on its disk and the variable can be
dropped.

**What you must not do** is bring the replacement up with a *fresh* identity. It
joins the cluster, serves requests and looks healthy, but the pool goes on
assigning the dead node's slots to an identity that no longer exists: the new
node owns nothing, repair has nowhere to write, and redundancy never comes back.
Reassigning a slot to a different identity is not supported and is deferred to
EC v3 together with moving objects between sets.

#### Starting while the pool is short a node

A node in cluster mode starts when it can **name** every entry of
`S4_POOL_NODES` — not when it can see every one of them. Everything under the
configuration is addressed by node identity (the placement ring, quorums,
repair, erasure-set slots), and a pool named only in part would compute a
different ring from the one the stored objects were written into.

Naming an entry takes two steps, and a neighbour that is down blocks both: its
address has to resolve (`S4_POOL_NODES` accepts DNS names, and an orchestrator
takes the name of a stopped container away), and the identity behind it has to
be learned. Neither changes while the peer is away, so neither is looked up
twice. Each start records what it learned in `pool_peers.json`, beside the
node's own identity file, keyed by the address exactly as configured; the next
start takes from it any entry that no longer resolves and any node gossip does
not show. What answers now wins over the record, because the machine behind an
address may have been replaced.

What this means in practice:

| Situation | What happens |
|---|---|
| Whole pool up | Named from gossip at once, as before |
| One or two nodes down, this node has run before | Starts immediately: a majority is visible and the rest are named from the record; a `warn` names them |
| Most of the pool down | Starts after the 30s wait, for the same reason — a live answer is preferred while there is time to get one |
| An entry that neither resolves nor was ever recorded | Start is refused, naming that entry: an unknown slot owner cannot be placed |

A node that came up next to dead neighbours serves what quorum allows and
nothing more: reads and writes below quorum answer `503`, and EC reads fail
closed. Slots whose owner is not visible are listed in `slot_owners.not_seen_alive`
of `GET /api/admin/ec/pools/:pool/health`.

There is no switch to require full visibility again.

**Two machines under one identity is the other thing you must not do**, and this
one the server stops for you. Before joining gossip, a starting node asks every
address in `S4_POOL_NODES` who it is. If one of them answers with *this node's*
identity, the start is refused and the message names that address:

```
node identity 0d3bb4d8-… is already held by a live member of this pool:
node-4 at 10.0.1.4:9100 answers for it.
```

Two nodes under one identity write two histories of the same slots — each
repairs shards the other does not have, each accepts writes the other never sees
— and afterwards nothing in the pool can tell them apart, because a manifest
names a slot's owner by identity and not by address. An address that does not
answer proves nothing and never refuses a start, so a pool where the peers are
down comes up exactly as it did before. `S4_NODE_IDENTITY_CONFLICT_CHECK=false`
switches the check off.

#### Coming back to the pool

A node that owes the pool shards it has not rebuilt does not serve
erasure-coded reads. It joins the cluster, accepts writes, and repairs — it just
does not answer `GET` and `HEAD` for erasure-coded objects until its shards are
back. Requests to it return `503` with the reason in the body; another node of
the pool serves them meanwhile.

The verdict is reached once per start, from two things the node knows about
itself:

- **its own presence record**, kept in its EC metadata and refreshed while it
  runs, which says in wall-clock terms how long it was away. A node back within
  `S4_MAX_REJOIN_DOWNTIME_DAYS` serves immediately; one that was away longer is
  held, and `S4_EC_LONG_OFFLINE_REJOIN_POLICY` decides how — `needs_bootstrap`
  waits for the rebuild, `require_admin_approval` waits for you.
- **where its identity came from.** A machine that adopted an identity through
  `S4_EXPECTED_NODE_IDENTITY` into an empty data directory has no shards and no
  downtime to measure: it is held until its shards are rebuilt. Restarting it
  does not clear that — the obligation is carried in the presence record.

A node whose identity was generated in this start is never held: no pool can
have shards under a name invented a moment ago. Neither is a node the pool gives
no slots.

Pool health, asked **on the node in question**, says where it stands:

```bash
curl -s http://localhost:9000/api/admin/ec/pools/my-pool/health \
  -H "Authorization: Bearer $TOKEN" | jq '.health, .rejoin'
```

```json
"degraded"
{
  "node_id": "0d3bb4d8-…",
  "decision": "needs_bootstrap",
  "reason": "storage_adopted",
  "serving_ec_reads": false,
  "enforced": true,
  "admin_approved": false,
  "admin_override": false,
  "slots": [3],
  "local_shards_held": 0,
  "local_shards_truncated": false,
  "downtime_secs": null
}
```

`slots` and `local_shards_held` side by side are the point: this node owns slot
3 and holds nothing. That is the state `slot_owners` cannot show, because from
its side the node is simply `alive` again — and it is why the pool reports
`degraded` through this node rather than healthy. `local_shards_truncated` says
whether the count stopped at `S4_EC_ADMIN_SCAN_LIMIT`; a count that hit the limit
counted the limit, not the shards.

The same verdict is on the node's `/metrics`, as
`s4_ec_node_rejoin_serving` and `s4_ec_node_rejoin_decision{decision="…"}`.

Rebuild the node's shards with the ordinary repair command (below), then tell the
node its return is acceptable:

```bash
curl -s -X POST http://localhost:9000/api/admin/ec/pools/my-pool/rejoin/approve \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"node_id":"0d3bb4d8-876d-4b6a-a27f-49a69d11d3ae"}'
```

Send it to the node that is held back: the verdict was reached from that node's
own storage, so no other node can lift it, and an approval naming a different
identity is refused rather than applied to whichever node the request reached.

The approval covers **this return only**. It lives in the node's memory, so the
next start weighs the evidence again — it never becomes a setting. Under
`require_admin_approval` it is the policy's own way out and the decision becomes
`incremental_repair_allowed`. Under `needs_bootstrap` the decision does not
change its mind: the node serves again, and the answer shows
`"decision": "needs_bootstrap"` together with `"admin_override": true`, because
what happened is that you said the node was whole, not that it proved it.

`S4_EC_REJOIN_GATE_ENABLED=false` leaves every verdict computed and published
and stops it holding anything back.

#### Rebuilding a node's shards

One request queues the whole of a node, and it is meant to be repeated until
nothing is left. Send it to a node that is up — the repair runs on the node that
queued it and writes the rebuilt shard to the owner the manifest names.

```bash
curl -s -X POST http://localhost:9000/api/admin/ec/pools/my-pool/repair \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"node_id":"0d3bb4d8-876d-4b6a-a27f-49a69d11d3ae","max_tasks":1024}'
```

```json
{
  "pool": "my-pool",
  "target_node_id": "0d3bb4d8-…",
  "queued": 1024,
  "newly_queued": 1024,
  "already_queued": 0,
  "remaining": 3120,
  "manifests_scanned": 4096,
  "stopped_at": "max_tasks",
  "scan_truncated": true,
  "tasks": []
}
```

`remaining` is the field to act on: it says how many more shards of that node
still need repair, so a request that filled its own `max_tasks` does not read
like one that finished the pool. `scan_truncated` says whether the walk reached
the end of the pool's manifests or stopped at `S4_EC_ADMIN_SCAN_LIMIT` — when it
stopped, `remaining` is a floor rather than a total. `stopped_at` is `end`,
`max_tasks`, `scan_limit` or `queue_full`.

Repeating the request does not double the queue: one repair task lives per
shard, so a second call finds the tasks it already made and reports them as
`already_queued`. A full durable queue stops the call queueing more and is
reported rather than raised as an error — the tasks already made are real work.

Whether the rebuild has finished is a different question, and it is not answered
by the queue: an empty queue is equally what a finished rebuild and a rebuild
nobody started look like. Ask the pool what it still misses:

```bash
curl -s "http://localhost:9000/api/admin/ec/pools/my-pool/repair-status?node-id=0d3bb4d8-876d-4b6a-a27f-49a69d11d3ae" \
  -H "Authorization: Bearer $TOKEN" | jq '.node_rebuild'
```

```json
{
  "node_id": "0d3bb4d8-…",
  "slots": [3],
  "manifests_scanned": 512,
  "scan_truncated": false,
  "shards_expected": 512,
  "shards_present": 512,
  "shards_missing": 0,
  "shards_unknown": 0,
  "unknown_reason": null,
  "complete": true,
  "verdict_reason": "all_shards_present",
  "tasks": { "queued": 0, "running": 0, "succeeded": 512, "failed_retryable": 0, "failed_permanent": 0 }
}
```

The count comes from walking the manifests and asking the owner for each shard
it should hold, so `complete: true` means the pool can no longer find a shard of
that node missing. It is `null`, never `true`, when the answer is not known:
when the walk stopped at `S4_EC_ADMIN_SCAN_LIMIT` (`verdict_reason`
`scan_truncated`), or when a shard's owner could not be asked (`shards_unknown`
above zero, `verdict_reason` `shards_unanswered`, with the first failure in
`unknown_reason`). One shard known to be missing settles it the other way —
`complete: false` — however much of the pool went unread. Without a `node-id`
the route answers exactly as it always has, with the pool's repair queue alone.

A whole-node rebuild is the heaviest repair load a pool ever carries, and it is
paced by the same limits as any other repair: the number of queued tasks buys it
nothing. `s4_ec_repair_throttled_total{limit="…"}` counts the repairs each limit
held back, which is what tells a rebuild running inside its budget apart from
one that is stuck.

The same two questions have answers on `/metrics`, so a rebuild can be watched
without polling the admin API:

| Metric | Answers |
|--------|---------|
| `s4_ec_node_shards_missing{node_id="…"}` | How many shards of that owner the pool still misses. Zero is the end of the rebuild |
| `s4_ec_node_rebuild_verdict{node_id="…",verdict="…"}` | Whether that zero can be trusted: `complete`, `incomplete` or `unknown` |
| `s4_ec_repair_bytes_total{leg="write"}` | Bytes the rebuild has written. A rebuild that is slow but moving is not a rebuild that is stuck |
| `s4_ec_repair_throttled_total{limit="…"}` | Which limit is pacing it, if any |

The first two are written when `repair-status` is asked about that node, not at
scrape time: the count behind them is a walk over the pool's manifests, which is
not work a scrape can do. So they appear for a node once something has asked
about it, and a node nobody has asked about publishes nothing rather than zero —
"the pool owes this owner nothing" is not a claim to make on a question that was
never put. Scrape them by keeping the `repair-status` call above in the loop
that drives the rebuild.

### Auto-Tiering Policies

A bucket policy is a delta over the node settings: a field left out is
inherited, not zeroed. Reading a policy returns both halves — what is stored for
the bucket and what is actually in force — because otherwise there is no way to
tell an inherited value from a bucket's own when explaining why an object did
not migrate.

The policy is cluster state. It is written through whichever node you happen to
reach and is in force on all of them.

```bash
# What is in force for a bucket
curl -s http://localhost:9000/api/admin/ec/tiering/mybucket \
  -H "Authorization: Bearer $TOKEN"

# Override the cold-after threshold and keep one prefix hot forever
curl -s -X PUT http://localhost:9000/api/admin/ec/tiering/mybucket \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"cold_after_secs":604800,"exclude_prefixes":["keep-hot/"]}'

# Back to inheriting everything
curl -s -X PUT http://localhost:9000/api/admin/ec/tiering/mybucket \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{}'
```

A `cold_after_secs` below one day is refused: a shorter threshold turns
auto-tiering into a migration generator. The node-wide
`S4_EC_TIERING_COLD_AFTER_SECS` keeps a looser rule so that deployments which
already set a small value still start.

The dry run answers what tiering would do right now. It writes nothing, is
bounded by `S4_EC_ADMIN_SCAN_LIMIT`, and counts skips per reason — "nothing
would migrate" is not actionable, "everything is still warm" and "everything is
excluded by a prefix" lead to opposite next steps.

The answer covers the whole pool, not the objects the node you asked happens to
hold, so any node gives the same report.

```bash
curl -s -X POST http://localhost:9000/api/admin/ec/tiering/dry-run \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"bucket":"mybucket","max_objects":1000,"max_samples":32}'
```

## Packed Segments (small objects)

An object below `S4_EC_MIN_OBJECT_SIZE` is not erasure coded on its own — coding
a 16 KiB object into six shards costs more in metadata and seeks than it saves —
so without packing it stays at three full copies. Packed segments gather such
objects into one large container, seal it, and erasure code the container as a
whole. The objects keep their own identity to every S3 client; what changes is
that their bytes now live inside somebody else's shards.

Off by default. Switching it on affects new small objects only.

```bash
S4_EC_PACKED_SEGMENTS_ENABLED=true
```

### The life of a segment

| State | What it means | What ends it |
|---|---|---|
| `open` | Taking objects. Their bytes are ordinary RF=3 copies, so durability is never lower than without packing | The segment reaches `S4_EC_PACKED_SEGMENT_TARGET_BYTES`, `S4_EC_PACKED_SEGMENT_MAX_OBJECTS`, or `S4_EC_PACKED_SEGMENT_MAX_AGE_SECS` |
| `sealed` | Contents frozen, offsets final | The transcode worker encodes it |
| `encoded` | Shards exist and are quorum-committed; objects still read from their RF=3 copies | Every object is read back out of the shards and its location committed to the metadata quorum |
| `purged` | The RF=3 copies are gone. This is where the space saving is realised | A repack, or the deletion of everything in it |
| `dead` | Nothing points at it any more | `S4_EC_PACKED_SEGMENT_RETIRE_GRACE_SECS`, after which its shards are tombstoned and collected like any other |

Nothing here needs an operator. A segment that is stuck is a segment whose state
has stopped changing, and `s4_ec_packed_segments` is what shows that.

### Reclaiming space: repacking

Deleting an object inside an encoded segment does **not** free its bytes. They
sit inside a container that was coded as a whole, and a piece cannot be cut out
of it. The space comes back through repacking: a background pass moves a
fragmented segment's surviving objects into a fresh segment and retires the old
one.

| Variable | Default | What it decides |
|---|---|---|
| `S4_EC_PACKED_SEGMENT_REPACK_DEAD_RATIO` | `0.5` | The fraction of a segment that must be dead before moving what is left is worth the work |
| `S4_EC_PACKED_SEGMENT_REPACK_MIN_DEAD_BYTES` | `8388608` | The absolute floor, so a small badly fragmented segment does not cost an encode to return a few megabytes |
| `S4_EC_PACKED_SEGMENT_RETIRE_GRACE_SECS` | `3600` | How long a repacked segment is kept intact so reads that started before the switch can finish on it |

A repack is three steps spread over cycles, and each is picked up from durable
state, so a node that restarts between two of them resumes rather than starts
again:

1. **Stage.** A replacement segment is described from the source's surviving
   objects. Nothing is copied and nothing points at it yet.
2. **Encode.** The ordinary transcode pipeline reads those objects out of the
   source's shards and codes them into the replacement's own.
3. **Cutover.** Every object is read back out of the replacement and checked,
   its location is committed to the metadata quorum, and only then do all the
   objects switch — in one operation, so a client listing versions never sees
   half of them moved.

Both thresholds must hold before a repack starts, and a segment holding an
object under Object Lock is left alone until the hold expires. That wait is
counted by `s4_ec_packed_repack_blocked_total` rather than reported as an error:
it is a compliant store behaving correctly, not a fault. Long retention on a
fragmented segment therefore pins its dead space for the length of the hold.

### Rolling back: unpacking

Switching `S4_EC_PACKED_SEGMENTS_ENABLED` off stops new segments. It does
nothing for the ones that already exist, so leaving the feature behind takes one
more step:

```bash
# What would be unpacked, without changing anything
curl -sS -X POST "$ADMIN/ec/segments/unpack" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"dry_run": true}' | jq

# Unpack everything this node holds
curl -sS -X POST "$ADMIN/ec/segments/unpack" \
  -H "Authorization: Bearer $TOKEN" -d '{}' | jq

# Or one segment
curl -sS -X POST "$ADMIN/ec/segments/unpack" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"segment_id": "0f2c6d1c-5680-43db-a024-bafe3b123e1d"}' | jq
```

Unpacking puts each object back on three copies of its own, withdraws the
cluster-wide record that described it as packed, and retires the segment. An
object that was never released still has its copies, so unpacking it is a
metadata change and nothing more. Segments are node-local records, so the call
acts on the segments of the node that receives it.

An **open** segment is refused: it is a live pool's write cursor. Wait out
`S4_EC_PACKED_SEGMENT_MAX_AGE_SECS` and it seals itself.

### Diagnosing a segment that is not moving

| Symptom | What to look at | Usual cause |
|---|---|---|
| `s4_ec_packed_segments{state="sealed"}` stays above zero | Node logs for `EC transcode could not read its source` | An object the segment was built from was deleted; a staged replacement heals itself on the next pass, a client segment needs the object restored |
| `s4_ec_packed_segments{state="encoded"}` stays above zero | Node logs for `packed segment is not ready to release its hot copies yet` | Shards are not readable from this node yet — usually a node that is down |
| Dead space does not fall | `s4_ec_packed_segment_dead_ratio` against the threshold, and `s4_ec_packed_repack_blocked_total` | Either the fraction has not been reached, or Object Lock is holding the segment |
| `s4_ec_packed_repack_queue_depth` stays above zero | Node logs for `packed segment queued for repacking` and what follows | Repacking is running but slower than deletes arrive |

**A limitation worth knowing.** A packed object's deletion is recorded against
its segment on the node that handles the `DELETE`, and segment records are node
local. A cluster that spreads deletes across nodes therefore reclaims space more
slowly than one that sends them to the node that wrote the objects. The data is
correct either way — only the timeliness of the reclaim is affected.

## Metrics

Every metric in the first list below carries an `erasure_set` label naming the
erasure set the work belonged to. A pool holds one set or several, and most EC
work belongs to exactly one of them: a transcode encodes into a set, a repair
rebuilds a slot of a set, a degraded read decodes a set's shards. The label is
what lets one set falling behind be seen instead of averaged away.

The pool-wide figure is the sum over the label:

```promql
sum without (erasure_set) (s4_ec_repair_jobs_total)
```

There is deliberately no second, unlabelled series of the same name — it would
double every total a dashboard adds up. The three gauges
(`s4_ec_transcode_queue_depth`, `s4_ec_repair_queue_depth`,
`s4_ec_orphan_shards_total`) are published for every set the pool has, including
the sets with nothing to report, which appear as zero: a series that stops being
written keeps its last value, so a drained queue would otherwise look busy
forever.

Tiering metrics stay unlabelled. Tiering is decided per bucket against the pool,
and the set only enters when a migration is admitted — which the transcode
counters already record.

The EC metrics surface includes:

- `s4_ec_transcode_jobs_total`
- `s4_ec_transcode_failures_total`
- `s4_ec_transcode_queue_depth`
- `s4_ec_inline_writes_total`
- `s4_ec_inline_writes_rejected_total`
- `s4_ec_read_degraded_total`
- `s4_ec_shard_hash_mismatch_total`
- `s4_ec_repair_jobs_total`
- `s4_ec_repair_failures_total`
- `s4_ec_repair_queue_depth`
- `s4_ec_repair_queue_depth_by_losses`
- `s4_ec_repair_succeeded_total`
- `s4_ec_repair_throttled_total`
- `s4_ec_orphan_shards_total`
- `s4_ec_manifest_quorum_failures_total`
- `s4_ec_hot_replicas_gc_eligible_total`
- `s4_ec_hot_replicas_purged_total`
- `s4_ec_node_rejoin_serving`
- `s4_ec_node_rejoin_decision`

The repair queue is published twice: as its depth, and split by how damaged the
chunks in it are. `s4_ec_repair_queue_depth_by_losses` counts the same tasks
under a second `losses` label — `1`, `2`, `3` and `4+` shards missing from the
task's own chunk, the measure the queue is served by — so the buckets of a set
add up to that set's `s4_ec_repair_queue_depth`:

```promql
# What the queue of one set is made of.
s4_ec_repair_queue_depth_by_losses{erasure_set="1"}

# Chunks with no redundancy left to lose, across the pool.
sum without (erasure_set) (s4_ec_repair_queue_depth_by_losses{losses!="1"})
```

The split is what separates two pools that report the same depth: forty thousand
tasks spread over the `1` bucket is a long tail of single losses, and the same
forty thousand in `2` and `3` is a pool a failure away from losing data. Read a
bucket against the profile of its own set — `2` has used up everything
`ec-rs-small` can spare and is half of what `ec-rs-dense` carries — which is why
the number is published per set and never summed across them.

Both gauges are written while pool health is composed, because the walk behind
them is over the node's durable queue rather than something a scrape can do:
`GET /api/admin/ec/pools/{pool}/health` is what refreshes them. They are also
per node, like the queue itself — a repair is carried out by the node that
queued it — so the pool's queue is the sum over the nodes scraped.

Packed segments add a census of the pool's containers and the counters of the
work that moves them along. `s4_ec_packed_segments` carries a second `state`
label, so one query shows where every segment of a set is in its life; a state
whose count never falls is a state something is stuck in. The ratio of degraded
to direct reads is the number packing is judged on in a healthy cluster: the
fallback rebuilds whole chunks, which is exactly the read amplification packing
exists to avoid.

- `s4_ec_packed_segments` (labels: `erasure_set`, `state`)
- `s4_ec_packed_segment_used_bytes`
- `s4_ec_packed_segment_dead_bytes`
- `s4_ec_packed_segment_dead_ratio`
- `s4_ec_packed_direct_reads_total`
- `s4_ec_packed_degraded_reads_total`
- `s4_ec_packed_repack_queue_depth`
- `s4_ec_packed_repack_segments_total`
- `s4_ec_packed_repack_objects_total`
- `s4_ec_packed_repack_reclaimed_bytes_total`
- `s4_ec_packed_repack_blocked_total`
- `s4_ec_packed_segments_retired_total`
- `s4_ec_packed_segments_unpacked_total`

Recoveries that no EC v1 path would have reached are counted separately.
`s4_ec_unified_solver_recoveries_total` carries the `erasure_set` label and an
`operation` label — `read` for a degraded read, `repair` for a shard rebuilt
back onto its node. The two mean different things to whoever is watching: a
read is a request already in flight, a repair is the pool healing itself. Either
way a moving counter says the pool is leaning on the full recovery path, which
is a statement about how much redundancy is left rather than about the codec. A
pool with no LRC profile never touches it.

- `s4_ec_unified_solver_recoveries_total` (labels: `erasure_set`, `operation`)

Laying out new sets by the LRC group rule adds one counter, without the
`erasure_set` label: a refused set never gets an id.
`s4_ec_set_layout_unsatisfied_total` counts the sets whose nodes could not meet
the rule, by `reason` (`too_few_domains`, `domain_overloaded`,
`incomplete_labels`) and `policy` (`strict` — the set was refused, `warn` — it
was created with degraded topology). All six series are exported from startup at
zero.

- `s4_ec_set_layout_unsatisfied_total` (labels: `reason`, `policy`)

Watching whether the sets still meet their rules adds one gauge, per set and per
origin. `s4_ec_set_layout_violations` is `1` when a set breaches the rule it was
laid out by and `0` when it does not, so a pool's violators are
`sum without (erasure_set) (s4_ec_set_layout_violations)`. A set laid out by the
EC v1 rule promises nothing about placement and reads `0` always. `origin`
separates `drift` — the labels changed under a set that was laid out correctly —
from `creation`, a set created in breach under `warn`. The two call for
different actions, so they are not summed together. Every set of the pool is
published on both origins, so a set that has been put right reads zero rather
than keeping its last value.

- `s4_ec_set_layout_violations` (labels: `erasure_set`, `origin`)

Reading adds two counters without the `erasure_set` label, for the same reason
repair's do: a link between zones is one physical thing whichever set's shards
cross it. `s4_ec_read_shard_bytes_total` counts the bytes EC reads pulled out of
shards and `s4_ec_read_shard_fetches_total` the fetches they made, both by
`scope` (`local`, `same_zone`, `cross_zone`, `unknown_zone`). A shard that came
back counts with the bytes it carried, whether or not it then passed its
manifest hash; a fetch that failed — no such shard, or bytes its own owner
refused because they no longer match its recorded hash — counts as a fetch of
no bytes, because the round trip was made but nothing arrived to measure. All
eight series are exported from startup at zero.

The two are counted apart because they are saved apart. Shards are fetched one
after another, so a fetch a read no longer makes is a network round trip out of
the latency chain and not only bytes off the link:

```promql
# What reads on this node are pulling, and how much of it crosses a zone.
rate(s4_ec_read_shard_bytes_total[5m])
rate(s4_ec_read_shard_bytes_total{scope="cross_zone"}[5m])

# Bytes per fetch: on a healthy pool this is the shard size, and it collapses
# when short ranges start dominating the traffic.
rate(s4_ec_read_shard_bytes_total[5m]) / rate(s4_ec_read_shard_fetches_total[5m])
```

A node whose `scope` is entirely `unknown_zone` has no `S4_EC_NODE_TOPOLOGY`: it
cannot place itself in the pool, so its traffic is reported as unplaced rather
than guessed at. The totals stay right either way.

- `s4_ec_read_shard_bytes_total` (label: `scope`)
- `s4_ec_read_shard_fetches_total` (label: `scope`)

Repair adds a byte counter, also without the `erasure_set` label: a link
between zones does not belong to a set. `s4_ec_repair_bytes_total` counts the
bytes repair actually moved, by `leg` (`source` — shards read, `write` — the
rebuilt shard) and `scope` (`local`, `same_zone`, `cross_zone`,
`unknown_zone`). A source that answered counts with the bytes it sent, verified
or not. `cross_zone` is what the links between zones carried for repair; for a
local repair of a group lying in one zone it stays at zero. All eight series
are exported from startup at zero.

- `s4_ec_repair_bytes_total` (labels: `leg`, `scope`)

Repair also counts what its limits held back, again without the `erasure_set`
label: a bandwidth budget is a property of this node and its links.
`s4_ec_repair_throttled_total` counts each repair that was refused admission, by
the `limit` that refused it — `global_concurrency`, `node_concurrency`,
`set_concurrency`, `failure_domain_concurrency`, `global_bandwidth` or
`failure_domain_bandwidth`. A refused repair is not a failure: it is put back
with a short delay and tried again. The counter matters most during a whole-node
rebuild, which is the heaviest repair load a pool ever carries: it is what tells
a rebuild running slower than the hardware could go because the budget is
working apart from one that is stuck. All six series are exported from startup
at zero.

- `s4_ec_repair_throttled_total` (label: `limit`)

A node's return to its pool adds two gauges, both about the node rather than
about any of its sets, so neither carries `erasure_set`.
`s4_ec_node_rejoin_serving` is `1` while the node answers erasure-coded reads
and `0` while it is held back until its shards are rebuilt — which is the one
thing membership cannot tell you, because a node that came back with none of its
shards is `alive` like any other. `s4_ec_node_rejoin_decision` is `1` on the
verdict in force and `0` on the other two, so a verdict that no longer holds
falls to zero instead of keeping its last value. Both are exported from startup,
so a node that never evaluated a return reads "serving" rather than "no data".

- `s4_ec_node_rejoin_serving`
- `s4_ec_node_rejoin_decision` (label: `decision` — `incremental_repair_allowed`,
  `needs_bootstrap`, `admin_approval_required`)

Rebuilding a replaced node adds two more, labelled by the owner they are about
rather than by an erasure set: a machine holds the slots of one set of a pool,
and the question is about the machine. `s4_ec_node_shards_missing` is how many
shards of that owner the pool still misses, and zero is what the end of a
rebuild looks like. `s4_ec_node_rebuild_verdict` says whether that zero can be
trusted — a walk stopped at `S4_EC_ADMIN_SCAN_LIMIT`, or one that could not
reach the owner, counted neither the pool nor the shards and reports zero
missing all the same. It is `1` on the verdict in force and `0` on the other
two, so a verdict that stops holding falls rather than keeping its last value.

Unlike every other gauge here, these two are not registered at startup and are
not computed at scrape time. The count behind them is a walk over the pool's
manifests, so they are written when `GET /ec/pools/:pool/repair-status?node-id=…`
is asked — which is the request that drives a rebuild anyway. A node nobody has
asked about therefore has no series at all, rather than a zero: the label is an
identity, and publishing zeroes for invented identities would claim the pool
owes nothing to nodes it has never heard of.

Bytes are not counted again for rebuilds. A rebuild is repair, and what it has
moved is the `write` leg of `s4_ec_repair_bytes_total` above.

- `s4_ec_node_shards_missing` (label: `node_id`)
- `s4_ec_node_rebuild_verdict` (labels: `node_id`, `verdict` — `complete`,
  `incomplete`, `unknown`)

Auto-tiering adds the activity tracker counters. A healthy tracker keeps
`records_written` far below `signals_total`: that ratio is the whole point of
collapsing, and a value close to one means every read is reaching the disk. A
rising `signals_dropped_total` costs accuracy only — reads outran the flusher,
so some objects look colder than they are.

- `s4_ec_tiering_heat_signals_total`
- `s4_ec_tiering_heat_signals_dropped_total`
- `s4_ec_tiering_heat_flushes_total`
- `s4_ec_tiering_heat_records_written_total`
- `s4_ec_tiering_heat_pending_depth`

## Troubleshooting

| Error or symptom | Meaning | Fix |
|------------------|---------|-----|
| `S4_POOL_TYPE=ec requires S4_MODE=cluster` | EC pools cannot run in single or gateway mode | Set `S4_MODE=cluster` |
| `S4_POOL_TYPE=ec requires an Enterprise Edition build` | Binary was built without `--features enterprise` | Build or run the EE binary |
| Enterprise-required or license-limit error | License missing, invalid, expired, or lacks `erasure_coding` | Provide a valid EE license |
| `fixed-width EC v1 requires exactly N node(s)` | `S4_POOL_NODES` count does not match profile total shards | Use the exact profile node count |
| Partial topology labels fail startup | `S4_EC_TOPOLOGY_POLICY=strict` requires complete labels when any label is present | Complete all labels or use `warn`. On a pool whose nodes all run a release with layout records this does not move data (see [Slot layouts are recorded](#slot-layouts-are-recorded-labels-do-not-move-them)) |
| A node logs `keeps its recorded layout, which differs from the one this node's topology labels give` | The labels changed after the set's layout was recorded, or differ between nodes | Nothing moved and nothing needs repair. Make `S4_EC_NODE_TOPOLOGY` identical on every node |
| `GET` of older EC objects returns `503` after labels changed | A node of an older release, which recomputes layouts from labels, is serving the pool | Restore the previous `S4_EC_NODE_TOPOLOGY` on the older nodes and upgrade them before changing labels |
| EC health reports `runtime_ready=false` | One or more local data-path workers or runtime ports are not active | Check `.runtime.production_ready_blockers`, worker env vars, and cluster bootstrap logs |
| EC health reports `runtime_ready=true` and `production_ready=false` | Runtime is wired, but this build is not marked as accepted for production | Use a release EE build; a build from source reports this unless built with `S4_EC_V1_CI_ACCEPTANCE_PASSED=true` |
| Object is missing from `ListObjectsV2` but `GET` and `HEAD` still return it | Its namespace entry was lost by a cutover from before S4 kept it | Run the anti-entropy pass, or wait for it: it rebuilds the entry from the manifest |

### Erasure coding and the bucket listing

Moving an object to erasure coding changes where its bytes live, and nothing
else. The object keeps its entry in the bucket, so `ListObjectsV2` returns it
with the same size and ETag as before, lifecycle rules still see it, and bucket
statistics still count it. Purging the hot replicas frees the replicated copies
without touching that entry.

Anti-entropy audits this the same way it audits shards: for every manifest it
checks that the object is still in the namespace, and puts the entry back if it
is not. That repairs both objects converted by older builds, which deleted the
entry, and a replica that was offline while its peers converted theirs.

The entry itself owns no bytes. A local read of one fails with a clear error
rather than reading whatever happens to sit at the recorded offset, because the
data is in the pool and only the EC read path can assemble it.
