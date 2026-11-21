package fingerprint

import (
	"sync"
	"testing"
)

func TestSharedSecretIsStable(t *testing.T) {
	secret := []byte("0123456789abcdef-shared")
	a, err := New(secret)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := New(secret)
	if a.Scope() != ScopeShared || a.KeyID() != b.KeyID() {
		t.Fatalf("scope=%s keyids %s %s", a.Scope(), a.KeyID(), b.KeyID())
	}
	if a.Fingerprint("user:1") != b.Fingerprint("user:1") {
		t.Fatal("fingerprints differ for same secret")
	}
	if a.Fingerprint("user:1") == a.Fingerprint("user:2") {
		t.Fatal("collision on trivial input")
	}
	if !Valid(a.Fingerprint("x")) {
		t.Fatal("invalid fingerprint format")
	}
	c, _ := New([]byte("another-secret-0123456789"))
	if c.Fingerprint("user:1") == a.Fingerprint("user:1") || c.KeyID() == a.KeyID() {
		t.Fatal("different secrets must produce different fingerprints")
	}
}

func TestInstanceScope(t *testing.T) {
	a, _ := New(nil)
	b, _ := New(nil)
	if a.Scope() != ScopeInstance {
		t.Fatal("expected instance scope")
	}
	if a.Fingerprint("k") == b.Fingerprint("k") {
		t.Fatal("random secrets should not collide")
	}
}

func TestShortSecretRejected(t *testing.T) {
	if _, err := New([]byte("short")); err == nil {
		t.Fatal("expected error")
	}
}

func TestConcurrentUse(t *testing.T) {
	h, _ := New([]byte("0123456789abcdef"))
	want := h.Fingerprint("k")
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				if h.Fingerprint("k") != want {
					t.Error("unstable fingerprint")
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestValid(t *testing.T) {
	for _, s := range []string{"", "abc", "0123456789ABCDEF0123456789abcdef", "0123456789abcdef0123456789abcdeg"} {
		if Valid(s) {
			t.Errorf("%q should be invalid", s)
		}
	}
}

func FuzzFingerprint(f *testing.F) {
	h, _ := New([]byte("0123456789abcdef"))
	f.Add("user:1")
	f.Add("")
	f.Add("\x00\xff")
	f.Fuzz(func(t *testing.T, key string) {
		fp := h.Fingerprint(key)
		if !Valid(fp) {
			t.Fatalf("invalid fingerprint %q", fp)
		}
		if fp != h.Fingerprint(key) {
			t.Fatal("non-deterministic")
		}
	})
}

func BenchmarkFingerprint(b *testing.B) {
	h, _ := New([]byte("0123456789abcdef"))
	b.ReportAllocs()
	for b.Loop() {
		_ = h.Fingerprint("product:123456")
	}
}
