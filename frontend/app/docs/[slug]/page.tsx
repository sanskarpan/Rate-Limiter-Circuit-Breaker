import { notFound } from 'next/navigation'
import Link from 'next/link'

const DOCS: Record<string, { title: string; content: string }> = {
  'token-bucket': {
    title: 'Token Bucket',
    content: `
## Overview

The **Token Bucket** algorithm is the most widely deployed rate limiting algorithm. It models a bucket that holds up to \`capacity\` tokens. Tokens are added at \`refillRate\` tokens per second. Each request consumes one or more tokens. If insufficient tokens are available, the request is denied.

## Key Properties

- **Burst allowed**: up to \`capacity\` requests can be served instantly if tokens accumulated
- **Constant average rate**: over any long interval, rate converges to \`refillRate\`
- **Memory**: O(keys) — one floating-point value per key
- **Distributed**: yes — Redis Lua script atomically reads+refills+decrements

## Algorithm

\`\`\`
on Allow(key, n):
  elapsed = now - lastRefill[key]
  tokens[key] += elapsed * refillRate
  tokens[key] = min(tokens[key], capacity)
  lastRefill[key] = now

  if tokens[key] >= n:
    tokens[key] -= n
    return ALLOWED
  else:
    retryAfter = (n - tokens[key]) / refillRate
    return DENIED, retryAfter
\`\`\`

## When to Use

Use Token Bucket when:
- You need to allow short bursts (e.g., initial page loads)
- You want a simple, well-understood algorithm
- You need distributed support via Redis

Avoid when:
- You need strictly constant output rate (use Leaky Bucket instead)
- Memory per key is a concern at massive scale

## Implementation Notes

This library implements lazy refill — tokens are computed on demand rather than by a background goroutine. This means no wasted CPU for inactive keys and exact token counts at any point in time.

The \`AllowN(n)\` method is fully atomic: either n tokens are consumed or zero are consumed. There is never partial consumption.
    `,
  },
  'leaky-bucket': {
    title: 'Leaky Bucket',
    content: `
## Overview

The **Leaky Bucket** algorithm enforces a strictly constant output rate regardless of input bursts. Requests enter a queue (the "bucket"). They are processed (leaked) at exactly \`leakRate\` requests per second. If the queue is full, new requests are dropped immediately.

## Key Properties

- **No bursting**: output is always at constant rate
- **Smoothing**: bursty input becomes smooth output
- **Memory**: O(keys + queue_depth) — queue per key
- **Distributed**: challenging — requires distributed queue coordination

## Algorithm

\`\`\`
on Allow(key):
  if len(queue[key]) >= capacity:
    return DENIED  // queue full

  queue[key].push(request)
  // background leaker goroutine:
  //   every 1/leakRate seconds: process one request from queue

on leak():
  for each key with non-empty queue:
    process(queue[key].pop_front())
    notify_caller(ALLOWED)
\`\`\`

## Key Difference from Token Bucket

| Property | Token Bucket | Leaky Bucket |
|----------|-------------|--------------|
| Burst | ✅ Allowed | ❌ Not allowed |
| Output rate | Variable | Constant |
| Latency added | None | Up to queueDepth/leakRate |

## When to Use

Use Leaky Bucket when:
- You need strictly constant output (e.g., outgoing API calls to a partner with strict SLA)
- You want to smooth bursty traffic before sending downstream
- You can accept queuing latency

Avoid when:
- Latency predictability is critical (queuing adds latency)
- You need burst allowance
    `,
  },
  'sliding-window': {
    title: 'Sliding Window',
    content: `
## Overview

The **Sliding Window** family eliminates the boundary burst problem of Fixed Window. Two variants:

1. **Log** — exact, O(n) memory
2. **Counter** — approximate, O(1) memory

## Log Variant

Stores a timestamp for every request in the window. Exact counting, but memory scales with request rate.

\`\`\`
on Allow(key, limit, window):
  prune timestamps[key] older than (now - window)
  if len(timestamps[key]) >= limit:
    retryAfter = oldest_timestamp + window - now
    return DENIED, retryAfter
  timestamps[key].append(now)
  return ALLOWED
\`\`\`

## Counter Variant

Uses two buckets (current + previous window) and a weighted formula:

\`\`\`
effectiveCount = previousCount * (1 - elapsed/window) + currentCount
\`\`\`

Maximum approximation error: \`limit * (1/window)\` at boundary.

## Boundary Burst Problem (Fixed Window)

With Fixed Window at limit=100/min, a burst of 200 requests is possible: 100 at the end of window N + 100 at the start of window N+1.

Sliding Window eliminates this: at any given second, the effective count accurately reflects the last \`window\` duration.

## When to Use

| Variant | Use When |
|---------|---------|
| Log | You need exact counting; memory is acceptable |
| Counter | High-volume keys; memory efficiency required; ~1% error acceptable |
    `,
  },
  'gcra': {
    title: 'GCRA — Generic Cell Rate Algorithm',
    content: `
## Overview

**GCRA** (Generic Cell Rate Algorithm) is used by Stripe, Shopify, and many high-performance APIs. It achieves the properties of a sliding window using only **one timestamp per key**.

## Core Formula

\`\`\`
emissionInterval = window / limit       // e.g. 100ms for 10 req/s
burstOffset      = emissionInterval * (burst - 1)

// Theoretical Arrival Time
TAT = max(lastTAT[key], now) + emissionInterval

allowed = (TAT - burstOffset) <= now
retryAfter = TAT - burstOffset - now   // when denied
remaining  = floor((now + burstOffset - TAT) / emissionInterval)
\`\`\`

## Why GCRA is Special

1. **Single timestamp per key** — minimal memory footprint
2. **Mathematically exact** — no approximation unlike sliding window counter
3. **Redis-friendly** — single \`GET\` + \`SET\` in a CAS loop, or one Lua script
4. **Burst control** — \`burst\` parameter cleanly controls how many requests can arrive simultaneously

## Integer Arithmetic

This implementation uses \`time.Duration\` (int64 nanoseconds) throughout — no floating point. This means no drift over millions of operations.

## References

- ATM Forum Traffic Management specification
- Brandur Leach: "Rate Limiting with Redis" — the best practical guide
- RFC 2697 (Single Rate Three Color Marker)
    `,
  },
  'circuit-breaker': {
    title: 'Circuit Breaker',
    content: `
## Overview

The **Circuit Breaker** pattern prevents cascade failures. When a downstream service starts failing, the circuit "opens" and requests fail fast without hitting the downstream — giving it time to recover.

## State Machine

\`\`\`
         failures >= threshold
CLOSED ──────────────────────→ OPEN
  ↑                              │
  │                              │ after openTimeout
  │       success count          ↓
  └────── >= successThreshold ─ HALF_OPEN
                                 │
                                 │ any failure
                                 └──────────────→ OPEN
\`\`\`

## States

| State | Behavior |
|-------|---------|
| **CLOSED** | All requests pass through |
| **OPEN** | All requests fail fast with \`ErrCircuitOpen\` |
| **HALF_OPEN** | Limited probe requests to test recovery |

## Window Types

- **Count-based**: track last N outcomes in ring buffer
- **Time-based**: track outcomes in rolling time window (e.g. last 60s)

## Half-Open Probes

When open, after \`openTimeout\`, the circuit transitions to HALF_OPEN. Up to \`halfOpenMaxRequests\` probes are allowed. If \`successThreshold\` consecutive successes occur, the circuit closes. Any single failure re-opens the circuit and resets the full timeout.

## Error Classification

Context cancellation (user cancelled) is **not** counted as a circuit failure. Only real downstream errors count. Configurable via \`IsFailure func(error) bool\`.
    `,
  },
  'comparison': {
    title: 'Algorithm Comparison Guide',
    content: `
## Choosing the Right Algorithm

### Decision Tree

\`\`\`
Need burst allowance?
├── YES → Token Bucket or GCRA
│         Need exact counting? → GCRA (also Redis-optimal)
│         Otherwise → Token Bucket (simpler, well-understood)
└── NO → Constant output rate needed?
         ├── YES → Leaky Bucket
         └── NO → Need exact counting?
                  ├── YES → Sliding Window Log
                  └── NO → Sliding Window Counter or Fixed Window
\`\`\`

## Performance Benchmarks

| Algorithm | ns/op | allocs/op | Notes |
|-----------|-------|-----------|-------|
| Token Bucket | 62 | 0 | Atomic fast path |
| GCRA | 67 | 0 | sync.Map + time.Duration |
| Fixed Window | 45 | 0 | Simplest implementation |
| Sliding Counter | 52 | 0 | Two-bucket formula |
| Sliding Log | 110 | 1 | O(n) slice ops |
| Leaky Bucket | 95 | 1 | Channel enqueue |
| Circuit Breaker | 82 | 0 | Atomic state check |

## Memory Trade-offs

| Algorithm | Memory Per Key | Memory Per Request |
|-----------|---------------|-------------------|
| Token Bucket | ~48 bytes | 0 |
| GCRA | ~24 bytes | 0 |
| Fixed Window | ~40 bytes | 0 |
| Sliding Counter | ~80 bytes | 0 |
| Sliding Log | ~40 bytes | **~24 bytes per request in window** |
| Leaky Bucket | ~40 bytes + queue | 0 |

## Distributed Comparison

| Algorithm | Redis Ops | Atomic | Notes |
|-----------|-----------|--------|-------|
| Token Bucket | 1 Lua | ✅ | Read-refill-decrement in one script |
| GCRA | 1 GET + 1 SET | ✅ via CAS | Or 1 Lua for true atomicity |
| Fixed Window | 1 INCR + EXPIRE | ✅ | Simplest distributed impl |
| Sliding Log | ZADD + ZCOUNT | ✅ via MULTI | Redis sorted set |
| Sliding Counter | 2 INCR | ✅ | Two window keys |
| Leaky Bucket | Hard | ❌ | Requires distributed queue |

## Production Recommendations

- **General API rate limiting**: Token Bucket (burst-friendly, battle-tested)
- **High-frequency microservices**: GCRA (minimal memory, exact, Redis-optimal)
- **Compliance/billing (exact counts)**: Sliding Window Log (exact at cost of memory)
- **Outbound rate to partner API**: Leaky Bucket (constant output rate)
- **Simple internal rate limits**: Fixed Window (easy to reason about)
    `,
  },
}

interface Props {
  params: { slug: string }
}

export function generateStaticParams() {
  return Object.keys(DOCS).map((slug) => ({ slug }))
}

export default function DocPage({ params }: Props) {
  const doc = DOCS[params.slug]
  if (!doc) notFound()

  return (
    <div className="space-y-6 max-w-3xl">
      <div className="flex items-center gap-2 text-sm text-gray-500">
        <Link href="/docs" className="hover:text-white transition-colors">Docs</Link>
        <span>/</span>
        <span className="text-white">{doc.title}</span>
      </div>

      <h1 className="text-3xl font-bold text-white">{doc.title}</h1>

      <div className="prose prose-invert prose-sm max-w-none space-y-4">
        {renderContent(doc.content)}
      </div>

      <div className="border-t border-white/10 pt-6 flex justify-between">
        <Link href="/docs" className="text-sm text-blue-400 hover:text-blue-300">
          ← Back to Docs
        </Link>
        <Link href="/algorithms/compare" className="text-sm text-blue-400 hover:text-blue-300">
          Try Algorithm Comparison →
        </Link>
      </div>
    </div>
  )
}

function renderContent(content: string) {
  const sections = content.trim().split(/(?=\n## )/)
  return sections.map((section, sectionIdx) => {
    const parts = section.split('\n')
    const elements: React.ReactNode[] = []
    let i = 0
    let codeBlock = false
    let codeLines: string[] = []

    while (i < parts.length) {
      const line = parts[i]

      if (line.startsWith('```')) {
        if (!codeBlock) {
          codeBlock = true
          codeLines = []
        } else {
          codeBlock = false
          elements.push(
            <pre key={`code-${sectionIdx}-${i}`} className="rounded-lg bg-gray-900 border border-white/10 p-4 overflow-x-auto">
              <code className="text-xs text-gray-300 font-mono">{codeLines.join('\n')}</code>
            </pre>
          )
        }
        i++
        continue
      }

      if (codeBlock) {
        codeLines.push(line)
        i++
        continue
      }

      if (line.startsWith('## ')) {
        elements.push(
          <h2 key={`h2-${i}`} className="text-xl font-bold text-white mt-6 mb-3">{line.slice(3)}</h2>
        )
      } else if (line.startsWith('| ')) {
        // Table
        const tableLines: string[] = []
        while (i < parts.length && parts[i].startsWith('|')) {
          tableLines.push(parts[i])
          i++
        }
        const rows = tableLines.filter((l) => !l.match(/^\|[-| ]+\|$/))
        const headers = rows[0]?.split('|').filter(Boolean).map((c) => c.trim()) ?? []
        const bodyRows = rows.slice(1)
        elements.push(
          <div key={`table-${i}`} className="overflow-x-auto rounded-lg border border-white/10">
            <table className="w-full text-sm">
              <thead className="bg-white/5">
                <tr>
                  {headers.map((h, hi) => (
                    <th key={hi} className="px-4 py-2 text-left font-medium text-gray-300">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-white/5">
                {bodyRows.map((row, ri) => (
                  <tr key={ri}>
                    {row.split('|').filter(Boolean).map((cell, ci) => (
                      <td key={ci} className="px-4 py-2 text-gray-400">{cell.trim()}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )
        continue
      } else if (line.startsWith('- **')) {
        elements.push(
          <li key={`li-${i}`} className="text-gray-300 ml-4">
            <strong className="text-white">{line.slice(3, line.indexOf('**', 3) + 2).replace(/\*\*/g, '')}</strong>
            {line.slice(line.indexOf('**', 3) + 2)}
          </li>
        )
      } else if (line.startsWith('- ')) {
        elements.push(
          <li key={`li-${i}`} className="text-gray-400 ml-4">{line.slice(2)}</li>
        )
      } else if (line.trim() !== '') {
        // Regular paragraph — handle inline bold
        elements.push(
          <p key={`p-${i}`} className="text-gray-400 leading-relaxed">
            {line.replace(/\*\*([^*]+)\*\*/g, '$1')}
          </p>
        )
      }
      i++
    }

    return <div key={sectionIdx}>{elements}</div>
  })
}
