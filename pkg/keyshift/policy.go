package keyshift

import (
	"fmt"
	"time"

	"github.com/Huy-8bit/keyshift/internal/policy"
)

// Policy precedence, highest first: a programmatic policy (ApplyPolicy), a
// controller policy, the local configuration. Every policy is merged with the
// local configuration's hard limits by policy.Resolve.

// ApplyPolicy installs a policy programmatically. It is validated and merged
// with the local hard limits exactly like a controller policy, and it takes
// precedence over controller policies until ClearPolicy is called.
func (c *Client) ApplyPolicy(p *Policy) error {
	if err := p.Validate(); err != nil {
		return fmt.Errorf("keyshift: invalid policy: %w", err)
	}
	c.policyMu.Lock()
	defer c.policyMu.Unlock()
	c.manualPolicy = true
	c.engine.SetPolicy(policy.Resolve(c.local, p.Clone()), true)
	return nil
}

// ClearPolicy removes a programmatic policy and reverts to the local policy.
// Controller policies are applied again on the next controller contact.
func (c *Client) ClearPolicy() {
	c.policyMu.Lock()
	defer c.policyMu.Unlock()
	c.manualPolicy = false
	c.useLocalPolicyLocked()
}

// useLocalPolicyLocked installs the local configuration as the effective
// policy. c.policyMu must be held.
func (c *Client) useLocalPolicyLocked() {
	c.engine.SetPolicy(policy.Resolve(c.local, nil), true)
}

// remotePolicyActiveLocked reports whether a controller policy is in force
// and may be replaced by controller events. c.policyMu must be held.
func (c *Client) remotePolicyActiveLocked() bool {
	return !c.manualPolicy && c.engine.Policy().Source == policy.SourceRemote
}

// applyEnvelope validates and installs a remote policy envelope. Invalid
// envelopes are rejected and the last valid policy is retained.
func (c *Client) applyEnvelope(env *policy.Envelope) error {
	if c.cfg.Telemetry.DisableRemotePolicy {
		return nil
	}
	if err := env.Validate(); err != nil {
		c.policyErrors.Add(1)
		c.cfg.Logger.Warn("keyshift rejected invalid policy from controller", "error", err)
		return err
	}
	if svc := env.Current.Service; svc != "" && svc != c.cfg.ServiceName {
		c.policyErrors.Add(1)
		return fmt.Errorf("keyshift: policy for service %q does not apply to %q", svc, c.cfg.ServiceName)
	}
	c.policyMu.Lock()
	defer c.policyMu.Unlock()
	if c.manualPolicy {
		return nil
	}
	eff := policy.Resolve(c.local, env.Select(c.instanceID))
	if cur := c.engine.Policy(); cur.Source == eff.Source && cur.Version == eff.Version {
		return nil
	}
	c.engine.SetPolicy(eff, true)
	c.cfg.Logger.Info("keyshift policy applied", "version", eff.Version, "source", eff.Source, "mode", eff.Mode)
	return nil
}

// revertToLocalPolicy drops a remote policy when the controller reports that
// none applies any more (for example after it was deleted).
func (c *Client) revertToLocalPolicy() {
	if c.cfg.Telemetry.DisableRemotePolicy {
		return
	}
	c.policyMu.Lock()
	defer c.policyMu.Unlock()
	if !c.remotePolicyActiveLocked() {
		return
	}
	c.useLocalPolicyLocked()
	c.cfg.Logger.Info("keyshift controller has no policy for this service; reverted to local policy")
}

// checkPolicyStaleness reverts to the local policy when a remote policy has
// not been confirmed by the controller within its validity period.
func (c *Client) checkPolicyStaleness(now time.Time) {
	c.policyMu.Lock()
	defer c.policyMu.Unlock()
	if !c.remotePolicyActiveLocked() {
		return
	}
	eff := c.engine.Policy()
	if now.Sub(time.Unix(0, c.lastContact.Load())) <= eff.ValidFor {
		return
	}
	c.useLocalPolicyLocked()
	c.cfg.Logger.Warn("keyshift remote policy expired without controller contact; reverted to local policy",
		"version", eff.Version, "valid_for", eff.ValidFor)
}
