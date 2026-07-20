'use client'

import { useEffect, useRef, useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'

interface TokenBucketVizProps {
  tokens: number
  capacity: number
  lastAllowed?: boolean | null
}

interface Drop {
  id: number
  x: number
}

let dropId = 0

export function TokenBucketViz({ tokens, capacity, lastAllowed }: TokenBucketVizProps) {
  const fillRatio = capacity > 0 ? Math.min(tokens / capacity, 1) : 0
  const [drops, setDrops] = useState<Drop[]>([])
  const prevTokensRef = useRef(tokens)

  // Spawn a drop when tokens increase (refill)
  useEffect(() => {
    if (tokens > prevTokensRef.current) {
      const newDrops = Array.from({ length: Math.min(tokens - prevTokensRef.current, 3) }, () => ({
        id: ++dropId,
        x: 40 + Math.random() * 40,
      }))
      setDrops((d) => [...d, ...newDrops])
    }
    prevTokensRef.current = tokens
  }, [tokens])

  const removeDrops = (id: number) => {
    setDrops((d) => d.filter((drop) => drop.id !== id))
  }

  const bucketWidth = 120
  const bucketHeight = 160
  const wallThickness = 6
  const viewBoxW = 200
  const viewBoxH = 240

  // Water fill height from bottom of bucket interior
  const maxWaterH = bucketHeight - wallThickness
  const waterH = maxWaterH * fillRatio

  // Inner bucket coords
  const innerLeft = (viewBoxW - bucketWidth) / 2 + wallThickness
  const innerRight = (viewBoxW + bucketWidth) / 2 - wallThickness
  const innerBottom = 200 - wallThickness
  const innerTop = innerBottom - maxWaterH

  const waterY = innerBottom - waterH

  return (
    <div className="flex flex-col items-center gap-4">
      <svg
        viewBox={`0 0 ${viewBoxW} ${viewBoxH}`}
        className="w-48 select-none"
        role="img"
        aria-label={`Token bucket: ${tokens} of ${capacity} tokens available${
          lastAllowed === false ? ', last request denied' : ''
        }`}
      >
        <title>Token bucket visualization</title>
        <desc>
          {`The bucket currently holds ${tokens} of ${capacity} tokens (${Math.round(
            fillRatio * 100,
          )}% full).`}
        </desc>
        {/* Glow filter */}
        <defs>
          <filter id="glow">
            <feGaussianBlur stdDeviation="3" result="coloredBlur" />
            <feMerge>
              <feMergeNode in="coloredBlur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
          <clipPath id="bucket-clip">
            <rect
              x={innerLeft}
              y={innerTop}
              width={innerRight - innerLeft}
              height={maxWaterH}
            />
          </clipPath>
        </defs>

        {/* Bucket walls */}
        {/* Left wall */}
        <rect
          x={(viewBoxW - bucketWidth) / 2}
          y={innerTop}
          width={wallThickness}
          height={bucketHeight}
          fill="#374151"
          rx={2}
        />
        {/* Right wall */}
        <rect
          x={(viewBoxW + bucketWidth) / 2 - wallThickness}
          y={innerTop}
          width={wallThickness}
          height={bucketHeight}
          fill="#374151"
          rx={2}
        />
        {/* Bottom */}
        <rect
          x={(viewBoxW - bucketWidth) / 2}
          y={innerBottom}
          width={bucketWidth}
          height={wallThickness}
          fill="#374151"
          rx={2}
        />

        {/* Water fill - animated */}
        <motion.rect
          x={innerLeft}
          width={innerRight - innerLeft}
          height={waterH}
          y={waterY}
          fill={lastAllowed === false ? '#f97316' : '#3b82f6'}
          clipPath="url(#bucket-clip)"
          animate={{ y: waterY, height: waterH, fill: lastAllowed === false ? '#f97316' : '#3b82f6' }}
          transition={{ type: 'spring', stiffness: 120, damping: 20 }}
          filter="url(#glow)"
        />

        {/* Water shimmer line */}
        <motion.rect
          x={innerLeft}
          width={innerRight - innerLeft}
          height={3}
          fill="rgba(255,255,255,0.3)"
          animate={{ y: waterY }}
          transition={{ type: 'spring', stiffness: 120, damping: 20 }}
        />

        {/* Falling drops */}
        <AnimatePresence>
          {drops.map((drop) => (
            <motion.circle
              key={drop.id}
              cx={drop.x + (viewBoxW - bucketWidth) / 2 + wallThickness}
              r={4}
              fill="#60a5fa"
              initial={{ cy: innerTop - 30, opacity: 1 }}
              animate={{ cy: waterY - 4, opacity: 0.8 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.4, ease: 'easeIn' }}
              onAnimationComplete={() => removeDrops(drop.id)}
            />
          ))}
        </AnimatePresence>

        {/* Token count label */}
        <text
          x={viewBoxW / 2}
          y={waterH > 30 ? innerBottom - waterH / 2 + 5 : innerBottom - 10}
          textAnchor="middle"
          fill="white"
          fontSize={18}
          fontWeight="bold"
          className="tabular-nums"
        >
          {tokens}
        </text>

        {/* Capacity label at top */}
        <text
          x={viewBoxW / 2}
          y={innerTop - 8}
          textAnchor="middle"
          fill="#9ca3af"
          fontSize={11}
        >
          capacity: {capacity}
        </text>
      </svg>

      {/* Percentage bar */}
      <div className="w-full">
        <div className="mb-1 flex items-center justify-between text-xs text-gray-400">
          <span>Tokens</span>
          <span className="font-mono">
            {tokens} / {capacity}
          </span>
        </div>
        <div
          className="h-2 w-full overflow-hidden rounded-full bg-gray-800"
          role="progressbar"
          aria-label="Tokens available"
          aria-valuemin={0}
          aria-valuemax={capacity}
          aria-valuenow={tokens}
          aria-valuetext={`${tokens} of ${capacity} tokens`}
        >
          <motion.div
            className="h-full rounded-full bg-blue-500"
            style={{ width: `${fillRatio * 100}%` }}
            animate={{ width: `${fillRatio * 100}%` }}
            transition={{ type: 'spring', stiffness: 100, damping: 20 }}
          />
        </div>
      </div>
    </div>
  )
}
