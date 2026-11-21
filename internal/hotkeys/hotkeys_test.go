package hotkeys

import (
	"errors"
	"testing"
	"time"
)

func TestOptionsValidation(t *testing.T) {
	for _, o := range []Options{{}, {Duration: time.Hour}, {Duration: time.Second, Count: 5}, {Duration: time.Second, Metrics: []string{"mem"}}, {Duration: time.Second, Sample: -1}} {
		if err := o.Validate(); err == nil {
			t.Errorf("expected error for %+v", o)
		}
	}
}

func TestClassify(t *testing.T) {
	cases := map[string]error{
		"ERR unknown command 'HOTKEYS', with args beginning with: ":     ErrUnsupported,
		"NOPERM User x has no permissions to run the 'hotkeys' command": ErrNotPermitted,
		"ERR hotkey tracking session already in progress":               ErrSessionActive,
	}
	for msg, want := range cases {
		if got := classify(errors.New(msg)); !errors.Is(got, want) {
			t.Errorf("%q -> %v", msg, got)
		}
	}
	if classify(nil) != nil {
		t.Fatal("nil")
	}
	_ = time.Second
}
