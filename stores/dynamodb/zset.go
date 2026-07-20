package dynamodb

import "strconv"

// Minimal sorted-set ("ZSET") emulation for the client-side sliding-window-log
// script. A direct port of the emulation in the core store's scripts_memory.go
// so the serialized wire format and pruning/counting semantics match the Memory,
// memcached, and Redis backends exactly.

type zmember struct {
	score  int64
	member string
}

type zset struct {
	members []zmember // sorted by score ascending
}

func parseZSet(s string) *zset {
	z := &zset{}
	if s == "" {
		return z
	}
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == '\x1e' {
			rec := s[start:i]
			start = i + 1
			if rec == "" {
				continue
			}
			sep := -1
			for j := 0; j < len(rec); j++ {
				if rec[j] == '\x1f' {
					sep = j
					break
				}
			}
			if sep < 0 {
				continue
			}
			sc, err := strconv.ParseInt(rec[:sep], 10, 64)
			if err != nil {
				continue
			}
			z.members = append(z.members, zmember{score: sc, member: rec[sep+1:]})
		}
	}
	return z
}

func (z *zset) serialize() string {
	if len(z.members) == 0 {
		return ""
	}
	buf := make([]byte, 0, len(z.members)*24)
	for i, mem := range z.members {
		if i > 0 {
			buf = append(buf, '\x1e')
		}
		buf = strconv.AppendInt(buf, mem.score, 10)
		buf = append(buf, '\x1f')
		buf = append(buf, mem.member...)
	}
	return string(buf)
}

func (z *zset) add(score int64, member string) {
	for i := range z.members {
		if z.members[i].member == member {
			z.members[i].score = score
			z.resort()
			return
		}
	}
	z.members = append(z.members, zmember{score: score, member: member})
	z.resort()
}

func (z *zset) resort() {
	for i := 1; i < len(z.members); i++ {
		for j := i; j > 0 && z.members[j-1].score > z.members[j].score; j-- {
			z.members[j-1], z.members[j] = z.members[j], z.members[j-1]
		}
	}
}

func (z *zset) removeByScoreUpTo(cutoff int64) {
	idx := 0
	for idx < len(z.members) && z.members[idx].score <= cutoff {
		idx++
	}
	if idx > 0 {
		z.members = z.members[idx:]
	}
}
