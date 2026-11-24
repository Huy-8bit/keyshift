package rediscompat

import "testing"

func TestParseServer(t *testing.T) {
	cases := []struct {
		info string
		want Server
		str  string
	}{
		{"# Server\r\nredis_version:8.6.8\r\nredis_mode:standalone\r\n", Server{"Redis", "8.6.8", "standalone"}, "Redis 8.6.8"},
		{"# Server\r\nredis_version:7.2.4\r\nserver_name:valkey\r\nvalkey_version:8.1.12\r\nserver_mode:cluster\r\n", Server{"Valkey", "8.1.12", "cluster"}, "Valkey 8.1.12"},
		{"", Server{Product: "Redis"}, "Redis"},
	}
	for _, c := range cases {
		got := ParseServer(c.info)
		if got != c.want || got.String() != c.str {
			t.Errorf("ParseServer(%q) = %+v (%s), want %+v (%s)", c.info, got, got, c.want, c.str)
		}
	}
	if v := ParseInfoField("a:1\r\nb:2:3\r\n", "b"); v != "2:3" {
		t.Fatalf("ParseInfoField = %q", v)
	}
}
