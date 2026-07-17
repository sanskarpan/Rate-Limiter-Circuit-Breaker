import { create } from 'zustand'
import type { RateLimitResult, CBSnapshot, SimResult, ServerSimStats, CBState } from '@/lib/api/types'

interface AlgorithmEntry {
  lastResult: RateLimitResult | null
  history: SimResult[]
  // Aggregated stats streamed over the WebSocket "sim_stats" event.
  stats: ServerSimStats | null
}

interface AppState {
  // Algorithm states
  algorithms: Record<string, AlgorithmEntry>
  setAlgorithmResult: (algo: string, result: RateLimitResult) => void
  addSimResult: (result: SimResult) => void
  setSimStats: (algo: string, stats: ServerSimStats) => void

  // Circuit breaker states
  cbSnapshots: Record<string, CBSnapshot>
  setCBSnapshot: (snapshot: CBSnapshot) => void
  setCBSnapshots: (snapshots: CBSnapshot[]) => void

  // WS connection status
  wsStatus: 'connecting' | 'open' | 'closed' | 'error' | 'idle'
  setWsStatus: (status: 'connecting' | 'open' | 'closed' | 'error' | 'idle') => void

  // UI
  selectedAlgorithm: string
  setSelectedAlgorithm: (algo: string) => void
}

const ALGORITHMS = ['token_bucket', 'sliding_window', 'fixed_window', 'leaky_bucket']

function makeEntry(): AlgorithmEntry {
  return { lastResult: null, history: [], stats: null }
}

export const useAppStore = create<AppState>((set) => ({
  algorithms: Object.fromEntries(ALGORITHMS.map((a) => [a, makeEntry()])),
  cbSnapshots: {},
  wsStatus: 'idle',
  selectedAlgorithm: 'token_bucket',

  setAlgorithmResult: (algo, result) =>
    set((s) => ({
      algorithms: {
        ...s.algorithms,
        [algo]: {
          ...(s.algorithms[algo] ?? makeEntry()),
          lastResult: result,
        },
      },
    })),

  addSimResult: (result) =>
    set((s) => {
      const algo = result.algorithm
      const prev = s.algorithms[algo] ?? makeEntry()
      const history = [...prev.history, result].slice(-200) // keep last 200
      return {
        algorithms: {
          ...s.algorithms,
          [algo]: { ...prev, history },
        },
      }
    }),

  setSimStats: (algo, stats) =>
    set((s) => ({
      algorithms: {
        ...s.algorithms,
        [algo]: {
          ...(s.algorithms[algo] ?? makeEntry()),
          stats,
        },
      },
    })),

  setCBSnapshot: (snapshot) =>
    set((s) => ({
      cbSnapshots: { ...s.cbSnapshots, [snapshot.name]: snapshot },
    })),

  setCBSnapshots: (snapshots) =>
    set(() => ({
      cbSnapshots: Object.fromEntries(snapshots.map((s) => [s.name, s])),
    })),

  setWsStatus: (wsStatus) => set({ wsStatus }),

  setSelectedAlgorithm: (selectedAlgorithm) => set({ selectedAlgorithm }),
}))

export const ALGO_LABELS: Record<string, string> = {
  token_bucket: 'Token Bucket',
  sliding_window: 'Sliding Window',
  fixed_window: 'Fixed Window',
  leaky_bucket: 'Leaky Bucket',
}

export const CB_STATE_COLORS: Record<CBState, string> = {
  closed: '#22c55e',
  'half-open': '#f59e0b',
  open: '#ef4444',
}
