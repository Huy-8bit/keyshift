package policy

import (
	"errors"
	"fmt"
	"strings"
)

// ValidatePattern checks a key glob. Supported syntax: '*' matches any
// sequence (including ':' separators), '?' matches one byte, and '\' escapes
// the next byte. Character classes are intentionally not supported.
func ValidatePattern(p string) error {
	if p == "" {
		return errors.New("empty pattern")
	}
	if len(p) > MaxPatternLen {
		return fmt.Errorf("pattern longer than %d bytes", MaxPatternLen)
	}
	if strings.HasSuffix(p, `\`) && !strings.HasSuffix(p, `\\`) {
		return fmt.Errorf("pattern %q ends with a dangling escape", p)
	}
	if strings.Count(p, "*") > 8 {
		return fmt.Errorf("pattern %q has more than 8 wildcards", p)
	}
	return nil
}

// Matcher matches keys against a set of glob patterns. It is immutable and
// safe for concurrent use.
type Matcher struct {
	exact    map[string]struct{}
	prefixes []string
	globs    []string
}

// NewMatcher compiles patterns. Patterns must already be validated.
func NewMatcher(patterns []string) *Matcher {
	m := &Matcher{}
	for _, p := range patterns {
		switch {
		case !strings.ContainsAny(p, `*?\`):
			if m.exact == nil {
				m.exact = map[string]struct{}{}
			}
			m.exact[p] = struct{}{}
		case strings.HasSuffix(p, "*") && !strings.ContainsAny(p[:len(p)-1], `*?\`):
			m.prefixes = append(m.prefixes, p[:len(p)-1])
		default:
			m.globs = append(m.globs, p)
		}
	}
	return m
}

// Empty reports whether the matcher has no patterns.
func (m *Matcher) Empty() bool {
	return m == nil || len(m.exact) == 0 && len(m.prefixes) == 0 && len(m.globs) == 0
}

// Match reports whether key matches any pattern.
func (m *Matcher) Match(key string) bool {
	if m == nil {
		return false
	}
	if _, ok := m.exact[key]; ok {
		return true
	}
	for _, p := range m.prefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	for _, g := range m.globs {
		if Glob(g, key) {
			return true
		}
	}
	return false
}

// Glob reports whether s matches pattern using iterative backtracking with
// O(len(pattern)*len(s)) worst case and no allocation.
func Glob(pattern, s string) bool {
	p, i := 0, 0
	starP, starI := -1, 0
	for i < len(s) {
		if p < len(pattern) {
			switch c := pattern[p]; c {
			case '*':
				starP, starI = p, i
				p++
				continue
			case '?':
				p++
				i++
				continue
			case '\\':
				if p+1 < len(pattern) && pattern[p+1] == s[i] {
					p += 2
					i++
					continue
				}
			default:
				if c == s[i] {
					p++
					i++
					continue
				}
			}
		}
		if starP >= 0 {
			p = starP + 1
			starI++
			i = starI
			continue
		}
		return false
	}
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}
