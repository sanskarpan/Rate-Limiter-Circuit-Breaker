export default function CircuitBreakerLoading() {
  return (
    <div className="space-y-8 animate-pulse">
      <div className="h-10 w-80 rounded-lg bg-white/10" />
      <div className="grid gap-6 lg:grid-cols-3">
        <div className="h-80 rounded-xl bg-white/5" />
        <div className="col-span-2 h-80 rounded-xl bg-white/5" />
      </div>
    </div>
  )
}
