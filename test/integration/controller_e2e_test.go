//go:build integration

// Package integration contains end-to-end tests that combine real Redis,
// SDK instances, and an in-process controller.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/Huy-8bit/keyshift/internal/controller"
	"github.com/Huy-8bit/keyshift/internal/policy"
	"github.com/Huy-8bit/keyshift/internal/testutil"
	"github.com/Huy-8bit/keyshift/pkg/keyshift"
)

const (
	ingestTok = "e2e-ingest-token-0123456789"
	adminTok  = "e2e-admin-token-0123456789"
)

func TestSDKControllerEndToEnd(t *testing.T) {
	addr := testutil.RedisAddr(t)
	cfg := controller.DefaultConfig()
	cfg.Auth = controller.NewAuth([]string{ingestTok}, nil, []string{adminTok})
	cfg.PerInstanceMinInterval = 0
	srv, err := controller.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	secret := []byte("e2e-shared-fingerprint-secret")
	ctx := context.Background()
	ext := testutil.Plain(t, addr)
	hot := testutil.Key(t, "product:1")
	lock := testutil.Key(t, "lock:1")
	ext.Set(ctx, hot, "p1", 0)
	ext.Set(ctx, lock, "owner", 0)

	var clients []*keyshift.Client
	for i := range 4 {
		svc := "catalog"
		if i >= 2 {
			svc = "checkout"
		}
		c, err := keyshift.New(keyshift.Config{
			ServiceName: svc, InstanceID: svc + "-pod-" + strconv.Itoa(i),
			Redis:     keyshift.RedisConfig{Addr: addr},
			Detection: keyshift.DetectionConfig{Window: time.Hour},
			Telemetry: keyshift.TelemetryConfig{Endpoint: ts.URL, Token: ingestTok, Interval: 200 * time.Millisecond,
				FingerprintSecret: secret, DisplayKeyPatterns: []string{"kstest:*product*"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		clients = append(clients, c)
	}
	// Every instance reads both keys; checkout pods read hot much more.
	for range 2 {
		for i, c := range clients {
			n := 300
			if i >= 2 {
				n = 900
			}
			for range n {
				c.Get(ctx, hot)
				c.Get(ctx, lock)
			}
			c.RotateNow()
		}
	}
	for _, c := range clients {
		if d := c.Explain(hot); d.State != "admitted" {
			t.Fatalf("%s: hot not admitted: %+v", c.InstanceID(), d)
		}
	}
	// The SDK reporters upload through the real HTTP path.
	testutil.Eventually(t, 10*time.Second, func() bool {
		for _, c := range clients {
			if c.Stats().TelemetrySent == 0 {
				return false
			}
		}
		return true
	}, "every instance uploads telemetry")
	v := srv.Aggregate(time.Now())
	if v.Overview.ActiveInstances != 4 || v.Overview.Services != 2 {
		t.Fatalf("overview %+v", v.Overview)
	}
	var hotView *controller.KeyView
	for i := range v.Keys {
		if v.Keys[i].FP == clients[0].Fingerprint(hot) {
			hotView = &v.Keys[i]
		}
	}
	if hotView == nil {
		t.Fatalf("hot key not aggregated across instances; keys=%d", len(v.Keys))
	}
	if hotView.Instances != 4 || len(hotView.Services) != 2 || hotView.Services[0].Service != "checkout" || hotView.Display != hot {
		t.Fatalf("hot view %+v", hotView)
	}
	for _, k := range v.Keys {
		if k.FP == clients[0].Fingerprint(lock) && k.Display != "" {
			t.Fatal("lock key name leaked to controller")
		}
	}

	// Admin denies lock:* for checkout via the API; SDKs pick it up from
	// the next telemetry response.
	body, _ := json.Marshal(map[string]any{"policy": policy.Policy{Mode: policy.ModeAdaptive, DenyPatterns: []string{"kstest:*lock*"}, ValidForSeconds: 600}})
	req, _ := http.NewRequest("PUT", ts.URL+"/api/v1/policies/checkout", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+adminTok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("policy put: %v %d", err, resp.StatusCode)
	}
	resp.Body.Close()
	// A new checkout instance fetches the policy at startup.
	c2, err := keyshift.New(keyshift.Config{
		ServiceName: "checkout", InstanceID: "checkout-pod-live",
		Redis:     keyshift.RedisConfig{Addr: addr},
		Detection: keyshift.DetectionConfig{Window: time.Hour},
		Telemetry: keyshift.TelemetryConfig{Endpoint: ts.URL, Token: ingestTok, Interval: 50 * time.Millisecond, FingerprintSecret: secret},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	// Policy is fetched at startup.
	if st := c2.Stats(); st.PolicySource != "remote" || st.PolicyVersion != 1 {
		t.Fatalf("new instance did not fetch policy at startup: %+v", st)
	}
	for range 2 {
		for range 500 {
			c2.Get(ctx, lock)
		}
		c2.RotateNow()
	}
	if d := c2.Explain(lock); d.State != "bypassed" {
		t.Fatalf("lock key should be bypassed by remote policy: %+v", d)
	}
	// The catalog service has no specific policy and keeps its local one.
	if st := clients[0].Stats(); st.PolicySource != "local" {
		t.Fatalf("catalog policy: %+v", st)
	}
	// Telemetry flows through the reporter.
	testutil.Eventually(t, 5*time.Second, func() bool { return c2.Stats().TelemetrySent > 0 }, "reporter upload")
}
