export default function AlgorithmLoading() {
  return (
    <div className="space-y-8 animate-pulse">
      <div className="h-10 w-64 rounded-lg bg-white/10" />
      <div className="grid gap-6 lg:grid-cols-2">
        <div className="h-72 rounded-xl bg-white/5" />
        <div className="h-72 rounded-xl bg-white/5" />
      </div>
      <div className="grid grid-cols-4 gap-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <div key={i} className="h-20 rounded-lg bg-white/5" />
        ))}
      </div>
    </div>
  )
}
