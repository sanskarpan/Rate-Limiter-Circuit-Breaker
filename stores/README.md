# `stores/` — additional distributed backends

This is a **separate nested Go module**
(`github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/stores`) that provides extra
implementations of the core [`store.Store`](../ratelimit/store/store.go)
interface. It lives outside the root module — exactly like
[`contrib/`](../contrib) does for framework middleware — so the **core library
stays zero-dependency**: the heavy backend SDKs (`bradfitz/gomemcache`,
`aws-sdk-go-v2`) are required only here, never by the root `go.mod`.

The module uses `replace github.com/sanskarpan/Rate-Limiter-Circuit-Breaker => ../`
so it builds against the local core.

```
stores/
├── go.mod                      # replace directive → ../ ; memcached + AWS SDK deps
├── memcached/                  # Memcached backend (CAS-based)
│   ├── memcached.go            #   Get/Set/SetNX/GetSet/IncrBy/Del/Ping/Close
│   ├── eval.go                 #   client-side "scripts" via Gets/CompareAndSwap
│   ├── zset.go                 #   sorted-set emulation for sliding-window-log
│   └── *_test.go               #   unit tests (fake client) + integration (build tag)
├── dynamodb/                   # DynamoDB backend (conditional writes / OCC)
│   ├── dynamodb.go             #   Store methods; ADD counters, conditional PutItem
│   ├── eval.go                 #   client-side "scripts" via single-item OCC loop
│   ├── zset.go                 #   sorted-set emulation for sliding-window-log
│   └── *_test.go               #   unit tests (fake API) + integration (build tag)
└── README.md                   # this file
```

Build & test the module:

```bash
cd stores
go build ./... && go vet ./... && go test ./...          # unit (no live server)
go test -tags=integration ./memcached/...                # needs memcached
go test -tags=integration ./dynamodb/...                 # needs DynamoDB Local
```

---

## Why a second backend at all?

The core ships an in-memory store and a Redis store
([`ratelimit/store/redis.go`](../ratelimit/store/redis.go)). Redis runs every
rate-limiting decision as a **single atomic Lua script** — read state, compute,
write — on the single-threaded server, which is the gold standard for
correctness. Teams standardised on **Memcached** or **DynamoDB**, or who want a
managed/serverless data plane, could not previously do distributed limiting. The
`Store` interface already abstracts this, so these backends slot in without any
change to the limiters.

The catch: **neither Memcached nor DynamoDB can run arbitrary server-side
scripts.** Atomicity has to be re-expressed per backend, and not every algorithm
survives the translation. The tables below are the honest accounting.

---

## Backend comparison

| Capability | **Redis** (core) | **Memcached** (`stores/memcached`) | **DynamoDB** (`stores/dynamodb`) |
|---|---|---|---|
| Atomicity primitive | Server-side **Lua** (whole script atomic) | **CAS** loop (`Gets`/`CompareAndSwap`) + atomic `Incr`/`Add` | **Conditional writes** / atomic `ADD` counter + optimistic version (OCC) |
| Scope of atomicity | Multi-key (all keys named by the script) | **Single key only** | **Single item only** |
| `Get`/`Set`/`Del`/`Ping` | ✅ | ✅ | ✅ |
| `SetNX` | ✅ `SET NX` | ✅ `Add` (store-if-absent) | ✅ conditional `PutItem` (`attribute_not_exists`) |
| `GetSet` | ✅ `SET … GET` | ✅ CAS loop | ✅ OCC loop |
| `IncrBy` | ✅ atomic Lua incr+expire | ✅ native `Incr` (+ CAS for negative deltas) | ✅ atomic `UpdateItem ADD` |
| **Token bucket** | ✅ | ✅ single-key CAS | ✅ single-item OCC |
| **GCRA** | ✅ | ✅ single-key CAS | ✅ single-item OCC |
| **Leaky bucket** | ✅ | ✅ single-key CAS | ✅ single-item OCC |
| **Fixed window** | ✅ | ✅ single-key CAS | ✅ single-item OCC |
| **Sliding-window log** | ✅ | ✅ single-key CAS (ZSET serialized into one value) | ✅ single-item OCC |
| **Sliding-window counter** | ✅ (current+prev in one script) | ❌ `ErrScriptUnsupported` (2 keys) | ❌ `ErrScriptUnsupported` (2 items) |
| **Distributed circuit breaker** | ✅ (one hash) | ❌ `ErrScriptUnsupported` | ❌ `ErrScriptUnsupported` |
| Server clock / clock-skew mode | ✅ `TIME` in-script | ❌ (client time only) | ❌ (client time only) |
| Auto key GC | TTL (`PEXPIRE`) | TTL **+ silent LRU eviction** | TTL attribute (async, ≤48h) + logical exp on read |
| Extra per-op cost | 1 round-trip | 1 round-trip (more under contention) | 1–2 round-trips (read + conditional write) |

### Atomicity model — Memcached

Memcached has no scripting and no transactions. Each "script" is a
`Gets → compute → CompareAndSwap` retry loop over the script's **single** state
key. Each individual key mutation is linearizable: two racing callers cannot both
commit against the same version — the loser retries against the winner's value.
This is genuinely **weaker than Redis Lua**:

- **Single-key only.** The sliding-window-**counter** (reads current + previous
  window) and the distributed **circuit breaker** need atomic access to more than
  one key, which Memcached cannot provide. They return `ErrScriptUnsupported`.
- **Bounded retries, fail-closed.** Under heavy single-key contention the CAS
  loop retries up to `maxCASRetries` (32) and then **denies** (`ErrCASExhausted`)
  rather than committing a stale computation. Redis serializes on the server with
  no client retry, so Memcached can very rarely deny a request Redis would admit.
- **Silent LRU eviction.** Memcached may evict a counter under memory pressure
  independent of TTL, momentarily resetting a limiter to empty (fail-open for that
  key). Size your slabs for the key working set.

### Atomicity model — DynamoDB

- `IncrBy` → `UpdateItem` with an **`ADD`** action: a true server-side atomic
  counter, as strong as Redis `INCRBY`.
- `SetNX` → conditional `PutItem` with `attribute_not_exists`.
- `GetSet` and the client-side scripts → a **single-item OCC loop**: a
  strongly-consistent `GetItem`, compute, then a `PutItem` conditional on an
  unchanged version attribute (`ver`). A `ConditionalCheckFailedException` triggers
  a re-read + retry, bounded by `maxOCCRetries` (32), failing **closed** on
  exhaustion (`ErrOCCExhausted`).

Per-**item** OCC matches Redis for any single-item algorithm. It is weaker for
multi-item algorithms (`ErrScriptUnsupported` for the counter and breaker):
DynamoDB `TransactWriteItems` can atomically *write* multiple items but cannot
carry a read-compute-decide across items in one transaction without an external
read. DynamoDB TTL deletion is asynchronous (≤48h), so this store also enforces
expiry **logically on read** (an item whose `exp` is past is reported absent).

---

## Which backend should I choose?

- **Default to Redis.** If you can run Redis, use the core Redis store: full
  algorithm coverage, true multi-key atomicity, server-clock skew mitigation,
  lowest latency.
- **Memcached** — you already operate a Memcached fleet and only need the
  single-key limiters (token bucket / GCRA / leaky bucket / fixed window /
  sliding-window log). Cheapest and simplest, but tolerate occasional
  LRU-eviction resets and no counter/breaker support.
- **DynamoDB** — you want a **fully managed / serverless** control plane with no
  servers to run, pay-per-request billing, and native TTL GC, and again only need
  the single-item limiters. Highest per-op latency and cost; strongest of the two
  non-Redis atomicity models thanks to `ADD` counters and conditional writes.
- **Need the sliding-window counter or a distributed circuit breaker?** Use
  Redis — those are not portable to a single-key/item store.

All three non-core backends share the same **fail-open fallback** behaviour as
the Redis store: when the backend is unreachable they route to a configurable
`Fallback` store (default: a fresh per-process in-memory store). Read the
`Options.Fallback` godoc for the per-instance-divergence trade-off, and supply a
fail-closed store if strict enforcement during an outage matters.

---

## Gossip / in-cluster backend (design sketch — not yet implemented)

For deployments that want distributed-ish limiting with **zero external
dependency** — no Redis, Memcached, or DynamoDB — the plan is an **eventually
consistent, fleet-local** limiter built on a gossip membership protocol
(SWIM, via [`hashicorp/memberlist`](https://github.com/hashicorp/memberlist)) and
a **CRDT counter**. This is fundamentally **approximate**: there is no single
source of truth, so the global limit is enforced only *eventually* and can be
briefly overshot during propagation. It trades exactness for the operational
simplicity of having no data store at all — appropriate for coarse abuse
mitigation, not hard quotas.

### Approach

Each node keeps a **PN-Counter** (a state-based CRDT: a grow-only map of
`nodeID → count` per limiter key, one for increments and one for decrements). A
node increments only its **own** entry on a local admission; the effective count
for a key is the sum across all nodes' entries. Nodes periodically gossip their
per-key vectors; merge is the element-wise `max`, which is commutative,
associative, and idempotent — so the counter converges regardless of message
order, loss, or duplication. Window rollover is handled by tagging each vector
with a window epoch and discarding stale epochs.

Because every node can read the (last-gossiped) fleet-wide sum locally, an
admission decision needs **no network round-trip** — it is as fast as the
in-memory store. The cost is staleness: a node may admit slightly over the limit
until the next gossip round reconciles.

### Interface sketch

The backend would implement the same `store.Store` interface. Only the
single-key counter algorithms (fixed window, and an approximate token bucket)
port cleanly; time-ordered algorithms (sliding-window log, GCRA) do not, because
gossip gives no total order over events.

```go
// Package gossip (sketch): an eventually-consistent, dependency-light Store
// backed by a SWIM cluster and PN-Counter CRDTs. APPROXIMATE — see package doc.
package gossip

// Options configures the gossip cluster.
type Options struct {
    BindAddr  string        // memberlist bind address
    Seeds     []string      // existing members to join
    GossipInterval time.Duration
    KeyPrefix string
    Fallback  store.Store
}

// Gossip is an eventually-consistent Store. It implements store.Store, but its
// Eval only supports the counter-style scripts (FixedWindow, and an approximate
// token bucket); time-ordered scripts return ErrScriptUnsupported.
type Gossip struct { /* memberlist + per-key PN-Counter map */ }

func New(opts Options) (*Gossip, error)

// pnCounter is the per-key CRDT: increments/decrements keyed by node ID.
type pnCounter struct {
    epoch uint64            // window epoch; stale epochs are dropped on merge
    inc   map[string]int64  // node ID -> local increments
    dec   map[string]int64  // node ID -> local decrements
}
func (c *pnCounter) value() int64          // sum(inc) - sum(dec)
func (c *pnCounter) merge(other *pnCounter) // element-wise max per node ID
```

**References:** SWIM / `hashicorp/memberlist`, CRDT PN-Counters, DynamoDB Global
Tables, Cloudflare's multi-region "approximate then reconcile" limiting.
(ENHANCEMENTS.md §5.2, §5.6.)
