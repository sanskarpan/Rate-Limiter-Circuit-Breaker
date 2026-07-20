package memcached

import "strconv"

// This file is a minimal sorted-set ("ZSET") emulation used only by the
// client-side sliding-window-log script. It is a direct port of the ZSET
// emulation in the core store's scripts_memory.go so that the serialized wire
// format and the pruning/counting semantics match the Memory and Redis backends
// exactly (a nanosecond score snapped through float64, ascending by score).

type zmember struct {
	score  int64
	member string
}

type zset struct {
	members []zmember // kept sorted by score ascending
}

// parseZSet deserializes the "score\x1fmember\x1e..." wire format.
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

// serialize renders the set to the wire format ("" when empty).
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

// add inserts or updates a member (ZADD semantics), keeping members sorted.
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

// removeByScoreUpTo removes members with score <= cutoff (ZREMRANGEBYSCORE).
func (z *zset) removeByScoreUpTo(cutoff int64) {
	idx := 0
	for idx < len(z.members) && z.members[idx].score <= cutoff {
		idx++
	}
	if idx > 0 {
		z.members = z.members[idx:]
	}
}
