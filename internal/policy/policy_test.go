package policy

import (
	"strings"
	"testing"
)

func TestParseValid(t *testing.T) {
	p, err := Parse(strings.NewReader(`{
		"version": 3, "service": "product-service", "mode": "adaptive",
		"admission": {"min_read_rate": 10, "min_hit_ratio": 0.7, "exit_hit_ratio": 0.4},
		"budget": {"max_entries": 500},
		"pin_patterns": ["config:*"], "deny_patterns": ["balance:*", "lock:*"],
		"deny_fingerprints": ["0123456789abcdef0123456789abcdef"]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if p.Version != 3 || p.Admission.MinReadRate != 10 || len(p.DenyPatterns) != 2 {
		t.Fatalf("unexpected %+v", p)
	}
}

func TestParseRejects(t *testing.T) {
	cases := map[string]string{
		"unknown field":     `{"mode":"adaptive","bogus":1}`,
		"bad mode":          `{"mode":"always"}`,
		"trailing":          `{"mode":"off"} {}`,
		"ratio":             `{"mode":"adaptive","admission":{"min_hit_ratio":1.5}}`,
		"hysteresis":        `{"mode":"adaptive","admission":{"min_hit_ratio":0.5,"exit_hit_ratio":0.6}}`,
		"empty pattern":     `{"mode":"adaptive","deny_patterns":[""]}`,
		"bad fingerprint":   `{"mode":"adaptive","deny_fingerprints":["XYZ"]}`,
		"negative budget":   `{"mode":"adaptive","budget":{"max_entries":-1}}`,
		"bad service":       `{"mode":"adaptive","service":"a b"}`,
		"dangling escape":   `{"mode":"adaptive","pin_patterns":["abc\\"]}`,
		"too many wildcard": `{"mode":"adaptive","pin_patterns":["*a*a*a*a*a*a*a*a*a"]}`,
		"not json":          `mode: adaptive`,
	}
	for name, doc := range cases {
		if _, err := Parse(strings.NewReader(doc)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestParseOversized(t *testing.T) {
	doc := `{"mode":"adaptive","comment":"` + strings.Repeat("x", MaxPolicyBytes) + `"}`
	if _, err := Parse(strings.NewReader(doc)); err == nil {
		t.Fatal("expected size error")
	}
}

func TestGlob(t *testing.T) {
	cases := []struct {
		p, s string
		want bool
	}{
		{"user:*", "user:1", true},
		{"user:*", "users:1", false},
		{"*:profile", "user:1:profile", true},
		{"a?c", "abc", true},
		{"a?c", "ac", false},
		{"*", "", true},
		{"a*b*c", "axxbyyc", true},
		{"a*b*c", "axxbyy", false},
		{`a\*`, "a*", true},
		{`a\*`, "ab", false},
		{"", "", true},
		{"", "x", false},
	}
	for _, c := range cases {
		if got := Glob(c.p, c.s); got != c.want {
			t.Errorf("Glob(%q,%q)=%v want %v", c.p, c.s, got, c.want)
		}
	}
}

func TestMatcher(t *testing.T) {
	m := NewMatcher([]string{"exact:key", "prefix:*", "*:mid:*"})
	for k, want := range map[string]bool{
		"exact:key": true, "exact:key2": false, "prefix:anything": true,
		"x:mid:y": true, "nomatch": false,
	} {
		if m.Match(k) != want {
			t.Errorf("Match(%q) != %v", k, want)
		}
	}
	if !NewMatcher(nil).Empty() || (*Matcher)(nil).Match("x") {
		t.Fatal("empty matcher misbehaves")
	}
}

func TestResolveRemoteCannotRaiseLimits(t *testing.T) {
	local := &Policy{Mode: ModeAdaptive, Budget: Budget{MaxEntries: 100, MaxMemoryBytes: 1 << 20},
		Admission: Admission{MaxEntryBytes: 1024}, DenyPatterns: []string{"lock:*"}}
	remote := &Policy{Version: 9, Mode: ModeExplicit, Budget: Budget{MaxEntries: 5000, MaxMemoryBytes: 1 << 10},
		Admission: Admission{MaxEntryBytes: 1 << 20, MinReadRate: 50}, PinPatterns: []string{"cfg:*"},
		DenyFingerprints: []string{"0123456789abcdef0123456789abcdef"}}
	e := Resolve(local, remote)
	if e.Source != "remote" || e.Version != 9 || e.Mode != ModeExplicit {
		t.Fatalf("unexpected %+v", e)
	}
	if e.Budget.MaxEntries != 100 || e.Budget.MaxMemoryBytes != 1<<10 {
		t.Fatalf("budget not min-merged: %+v", e.Budget)
	}
	if e.Admission.MaxEntryBytes != 1024 || e.Admission.MinReadRate != 50 {
		t.Fatalf("admission merge wrong: %+v", e.Admission)
	}
	if !e.Deny.Match("lock:x") || !e.Pin.Match("cfg:1") || !e.HasDenyF {
		t.Fatal("pattern union missing")
	}
	off := Resolve(&Policy{Mode: ModeOff}, &Policy{Mode: ModeAdaptive})
	if off.Mode != ModeOff {
		t.Fatal("remote must not re-enable locally disabled caching")
	}
	def := Resolve(nil, nil)
	if def.Admission != DefaultAdmission || def.Budget != DefaultBudget || def.Source != "local" {
		t.Fatalf("defaults wrong: %+v", def)
	}
}

func TestEnvelopeSelectAndBuckets(t *testing.T) {
	cur := &Policy{Version: 2, Mode: ModeAdaptive}
	prev := &Policy{Version: 1, Mode: ModeOff}
	env := &Envelope{Current: cur, Previous: prev, RolloutPercent: 30}
	if err := env.Validate(); err != nil {
		t.Fatal(err)
	}
	inCur := 0
	for i := range 10000 {
		id := "pod-" + string(rune('a'+i%26)) + "-" + itoa(i)
		if env.Select(id) == cur {
			inCur++
		}
		first, second := env.Select(id), env.Select(id)
		if first != second {
			t.Fatal("selection not stable")
		}
	}
	if inCur < 2500 || inCur > 3500 {
		t.Fatalf("30%% rollout selected %d/10000", inCur)
	}
	env.RolloutPercent = 100
	if env.Select("x") != cur {
		t.Fatal("100% must select current")
	}
	env.RolloutPercent, env.Previous = 0, nil
	if env.Select("x") != nil {
		t.Fatal("0% without previous must select local")
	}
	if (&Envelope{Current: cur, RolloutPercent: 101}).Validate() == nil {
		t.Fatal("expected rollout validation error")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func FuzzParse(f *testing.F) {
	f.Add(`{"mode":"adaptive","pin_patterns":["a:*"]}`)
	f.Add(`{"version":1,"mode":"off","budget":{"max_entries":5}}`)
	f.Add(`{}`)
	f.Fuzz(func(t *testing.T, doc string) {
		p, err := Parse(strings.NewReader(doc))
		if err != nil {
			return
		}
		if err := p.Validate(); err != nil {
			t.Fatalf("parsed policy fails validation: %v", err)
		}
		e := Resolve(nil, p)
		if e.Admission.ExitHitRatio > e.Admission.MinHitRatio {
			t.Fatal("hysteresis invariant violated")
		}
		for _, pat := range p.PinPatterns {
			_ = e.Pin.Match(pat)
		}
	})
}

func FuzzGlob(f *testing.F) {
	f.Add("a*b?c", "axxbyc")
	f.Fuzz(func(t *testing.T, p, s string) {
		if len(p) > 64 || len(s) > 256 {
			return
		}
		got := Glob(p, s)
		if p == s && !containsMeta(p) && !got {
			t.Fatalf("literal pattern %q failed to match itself", p)
		}
	})
}

func containsMeta(s string) bool { return strings.ContainsAny(s, `*?\`) }
