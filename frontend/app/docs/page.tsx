import Link from 'next/link'

const DOCS = [
  {
    slug: 'token-bucket',
    title: 'Token Bucket',
    description:
      'The foundational rate limiting algorithm. Tokens accumulate at a constant rate up to a capacity ceiling, allowing controlled bursting.',
    tags: ['burst', 'O(1)', 'distributed'],
  },
  {
    slug: 'leaky-bucket',
    title: 'Leaky Bucket',
    description:
      'Enforces a strictly constant output rate regardless of input bursts. Requests queue up and are processed one-at-a-time at the leak rate.',
    tags: ['constant-rate', 'queue', 'smoothing'],
  },
  {
    slug: 'sliding-window',
    title: 'Sliding Window',
    description:
      'Two variants: Log (exact, O(n) memory) and Counter (approximate, O(1) memory). Eliminates the boundary burst problem of fixed windows.',
    tags: ['exact', 'approximate', 'no-boundary-burst'],
  },
  {
    slug: 'gcra',
    title: 'GCRA',
    description:
      'Generic Cell Rate Algorithm — the most memory-efficient algorithm. One timestamp per key, zero approximation, naturally distributed-friendly.',
    tags: ['efficient', 'exact', 'redis-friendly'],
  },
  {
    slug: 'circuit-breaker',
    title: 'Circuit Breaker',
    description:
      'Prevents cascade failures by opening a circuit after a failure threshold, allowing the system time to recover.',
    tags: ['resilience', 'FSM', 'half-open'],
  },
  {
    slug: 'comparison',
    title: 'Algorithm Comparison',
    description:
      'When to use which algorithm. Trade-offs between memory, accuracy, burst support, and distributed compatibility.',
    tags: ['guide', 'trade-offs'],
  },
]

export default function DocsPage() {
  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-3xl font-bold text-white">Documentation</h1>
        <p className="mt-1 text-sm text-gray-400">
          Algorithm deep-dives, mathematical foundations, and implementation guides
        </p>
      </div>

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {DOCS.map((doc) => (
          <Link
            key={doc.slug}
            href={`/docs/${doc.slug}`}
            className="group rounded-xl border border-white/10 bg-white/5 p-5 transition-all hover:border-blue-500/40 hover:bg-white/8"
          >
            <h2 className="mb-2 text-lg font-semibold text-white group-hover:text-blue-300 transition-colors">
              {doc.title}
            </h2>
            <p className="mb-3 text-sm text-gray-400 leading-relaxed">{doc.description}</p>
            <div className="flex flex-wrap gap-1.5">
              {doc.tags.map((tag) => (
                <span
                  key={tag}
                  className="rounded-full bg-blue-500/10 px-2 py-0.5 text-xs font-medium text-blue-400"
                >
                  {tag}
                </span>
              ))}
            </div>
          </Link>
        ))}
      </div>

      <div className="rounded-xl border border-white/10 bg-white/5 p-6">
        <h2 className="mb-3 text-lg font-semibold text-white">Quick Reference</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-white/10 text-left">
                <th className="pb-3 font-medium text-gray-400">Algorithm</th>
                <th className="pb-3 font-medium text-gray-400">Burst</th>
                <th className="pb-3 font-medium text-gray-400">Exact</th>
                <th className="pb-3 font-medium text-gray-400">Memory</th>
                <th className="pb-3 font-medium text-gray-400">Distributed</th>
                <th className="pb-3 font-medium text-gray-400">Best For</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {[
                ['Token Bucket', '✅', '✅', 'O(keys)', '✅', 'API rate limiting'],
                ['Leaky Bucket', '❌', '✅', 'O(keys)', '⚠️', 'Constant output rate'],
                ['Sliding Log', '❌', '✅', 'O(req)', '✅', 'Exact counting needed'],
                ['Sliding Counter', '❌', '~', 'O(keys)', '✅', 'Approximate, fast'],
                ['Fixed Window', '❌', '✅', 'O(keys)', '✅', 'Simple, fast'],
                ['GCRA', '✅', '✅', 'O(keys)', '✅', 'High-performance API'],
              ].map(([algo, burst, exact, memory, dist, when]) => (
                <tr key={algo}>
                  <td className="py-2.5 font-medium text-white">{algo}</td>
                  <td className="py-2.5 text-center">{burst}</td>
                  <td className="py-2.5 text-center">{exact}</td>
                  <td className="py-2.5 font-mono text-gray-400">{memory}</td>
                  <td className="py-2.5 text-center">{dist}</td>
                  <td className="py-2.5 text-gray-400">{when}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
