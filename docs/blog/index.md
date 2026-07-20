# Blog & talk material

Longer-form write-ups derived from the actual design of this library. Every
claim here is grounded in the code and the
[Architecture Decision Records](../adr/README.md); file references point at real
source.

- **[Building a zero-dependency Go resilience library](zero-dependency-resilience.md)**
  — why the core has no runtime dependencies, how that boundary is *enforced in
  CI* rather than merely documented, and how optional Redis / framework /
  observability integrations are isolated behind adapters and a separate module.

- **[GCRA vs token bucket: choosing a rate limiter](gcra-vs-token-bucket.md)**
  — the mechanics of the two most useful algorithms in this library, when each
  wins, and the float64/256-ns precision subtlety that only shows up once you push
  GCRA through a Lua script.

- **[Talk outline: Resilience patterns in Go, deterministically tested](talk-outline.md)**
  — a conference-length outline covering the pipeline design, distributed
  atomicity with Lua, and testing time-dependent code with a fake clock.
