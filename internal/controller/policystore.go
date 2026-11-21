package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/Huy-8bit/keyshift/internal/policy"
)

// DefaultScope is the service key of the default policy.
const DefaultScope = "_default"

// HistoryEntry records one policy change.
type HistoryEntry struct {
	At             time.Time      `json:"at"`
	Service        string         `json:"service"`
	Action         string         `json:"action"`
	Version        int64          `json:"version"`
	RolloutPercent int            `json:"rollout_percent"`
	Actor          string         `json:"actor,omitempty"`
	Policy         *policy.Policy `json:"policy,omitempty"`
}

type storeState struct {
	NextVersion int64                       `json:"next_version"`
	Envelopes   map[string]*policy.Envelope `json:"envelopes"`
	History     []HistoryEntry              `json:"history"`
}

// PolicyStore keeps versioned policy envelopes per service, with bounded
// history and optional on-disk persistence (atomic file replacement).
type PolicyStore struct {
	mu         sync.RWMutex
	path       string
	st         storeState
	maxHistory int
	maxScopes  int
}

// NewPolicyStore loads state from dir (if non-empty).
func NewPolicyStore(dir string, maxHistory, maxScopes int) (*PolicyStore, error) {
	s := &PolicyStore{maxHistory: maxHistory, maxScopes: maxScopes,
		st: storeState{NextVersion: 1, Envelopes: map[string]*policy.Envelope{}}}
	if dir == "" {
		return s, nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	s.path = filepath.Join(dir, "policies.json")
	b, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	var st storeState
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, fmt.Errorf("policy store %s is corrupt: %w", s.path, err)
	}
	if st.Envelopes == nil {
		st.Envelopes = map[string]*policy.Envelope{}
	}
	for scope, env := range st.Envelopes {
		if err := env.Validate(); err != nil {
			return nil, fmt.Errorf("policy store: scope %s: %w", scope, err)
		}
	}
	s.st = st
	return s, nil
}

func (s *PolicyStore) persistLocked() error {
	if s.path == "" {
		return nil
	}
	b, err := json.MarshalIndent(s.st, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func scopeOf(service string) string {
	if service == "" {
		return DefaultScope
	}
	return service
}

// For returns the envelope that applies to service (service-specific first,
// then the default), or nil.
func (s *PolicyStore) For(service string) *policy.Envelope {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if env := s.st.Envelopes[service]; env != nil && service != "" {
		return env
	}
	return s.st.Envelopes[DefaultScope]
}

// All returns every envelope keyed by scope.
func (s *PolicyStore) All() map[string]*policy.Envelope {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*policy.Envelope, len(s.st.Envelopes))
	for k, v := range s.st.Envelopes {
		cp := *v
		out[k] = &cp
	}
	return out
}

// History returns policy history, newest first.
func (s *PolicyStore) History(service string) []HistoryEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []HistoryEntry
	for i := len(s.st.History) - 1; i >= 0; i-- {
		h := s.st.History[i]
		if service == "" || h.Service == scopeOf(service) {
			out = append(out, h)
		}
	}
	return out
}

func (s *PolicyStore) record(h HistoryEntry) {
	s.st.History = append(s.st.History, h)
	if over := len(s.st.History) - s.maxHistory; over > 0 {
		s.st.History = slices.Delete(s.st.History, 0, over)
	}
}

// Put installs a new policy for its scope with the given rollout. The
// previously active policy becomes the rollout baseline.
func (s *PolicyStore) Put(p *policy.Policy, rollout int, actor string) (*policy.Envelope, error) {
	if p == nil {
		return nil, errors.New("policy is required")
	}
	p = p.Clone()
	if p.Mode == "" {
		p.Mode = policy.ModeAdaptive
	}
	if rollout < 0 || rollout > 100 {
		return nil, errors.New("rollout_percent must be within [0, 100]")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	scope := scopeOf(p.Service)
	if _, exists := s.st.Envelopes[scope]; !exists && len(s.st.Envelopes) >= s.maxScopes {
		return nil, fmt.Errorf("too many policy scopes (max %d)", s.maxScopes)
	}
	p.Version = s.st.NextVersion
	p.CreatedAt = time.Now().UTC()
	p.CreatedBy = actor
	if err := p.Validate(); err != nil {
		return nil, err
	}
	env := &policy.Envelope{Current: p, RolloutPercent: rollout}
	if prev := s.st.Envelopes[scope]; prev != nil {
		env.Previous = prev.Current
	}
	if err := env.Validate(); err != nil {
		return nil, err
	}
	s.st.NextVersion++
	old := s.st.Envelopes[scope]
	s.st.Envelopes[scope] = env
	s.record(HistoryEntry{At: p.CreatedAt, Service: scope, Action: "put", Version: p.Version, RolloutPercent: rollout, Actor: actor, Policy: p})
	if err := s.persistLocked(); err != nil {
		s.st.Envelopes[scope] = old
		s.st.NextVersion--
		s.st.History = s.st.History[:len(s.st.History)-1]
		return nil, fmt.Errorf("%w: %w", ErrPersist, err)
	}
	return env, nil
}

// SetRollout changes the rollout percentage of the current policy.
func (s *PolicyStore) SetRollout(service string, percent int, actor string) (*policy.Envelope, error) {
	if percent < 0 || percent > 100 {
		return nil, errors.New("rollout_percent must be within [0, 100]")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	scope := scopeOf(service)
	env := s.st.Envelopes[scope]
	if env == nil {
		return nil, ErrNotFound
	}
	cp := *env
	cp.RolloutPercent = percent
	s.st.Envelopes[scope] = &cp
	s.record(HistoryEntry{At: time.Now().UTC(), Service: scope, Action: "rollout", Version: cp.Current.Version, RolloutPercent: percent, Actor: actor})
	if err := s.persistLocked(); err != nil {
		s.st.Envelopes[scope] = env
		return nil, fmt.Errorf("%w: %w", ErrPersist, err)
	}
	return &cp, nil
}

// Rollback makes the previous policy current for every instance.
func (s *PolicyStore) Rollback(service, actor string) (*policy.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	scope := scopeOf(service)
	env := s.st.Envelopes[scope]
	if env == nil {
		return nil, ErrNotFound
	}
	if env.Previous == nil {
		return nil, errors.New("no previous policy to roll back to")
	}
	next := &policy.Envelope{Current: env.Previous, RolloutPercent: 100}
	s.st.Envelopes[scope] = next
	s.record(HistoryEntry{At: time.Now().UTC(), Service: scope, Action: "rollback", Version: next.Current.Version, RolloutPercent: 100, Actor: actor})
	if err := s.persistLocked(); err != nil {
		s.st.Envelopes[scope] = env
		return nil, fmt.Errorf("%w: %w", ErrPersist, err)
	}
	return next, nil
}

// Delete removes a service-specific envelope (instances fall back to the
// default policy, or to their local policy if there is none).
func (s *PolicyStore) Delete(service, actor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	scope := scopeOf(service)
	env := s.st.Envelopes[scope]
	if env == nil {
		return ErrNotFound
	}
	delete(s.st.Envelopes, scope)
	s.record(HistoryEntry{At: time.Now().UTC(), Service: scope, Action: "delete", Actor: actor})
	if err := s.persistLocked(); err != nil {
		s.st.Envelopes[scope] = env
		return fmt.Errorf("%w: %w", ErrPersist, err)
	}
	return nil
}

// ErrNotFound is returned when a policy scope does not exist.
var ErrNotFound = errors.New("not found")

// ErrPersist wraps failures to write the policy file; the change is rolled back.
var ErrPersist = errors.New("policy store: persist failed")
