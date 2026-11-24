package rediscompat

import "strings"

// Server identifies a Redis-compatible server from its INFO server section.
type Server struct {
	// Product is "Redis" or "Valkey".
	Product string
	// Version is the product's own version. Valkey also reports a
	// compatibility redis_version, which is not used.
	Version string
	// Mode is standalone, cluster, or sentinel ("" when not reported).
	Mode string
}

// ParseServer reads the server identity from INFO server output.
func ParseServer(info string) Server {
	f := infoFields(info)
	s := Server{Product: "Redis", Version: f["redis_version"], Mode: f["redis_mode"]}
	if f["server_name"] == "valkey" || f["valkey_version"] != "" {
		s.Product, s.Version = "Valkey", f["valkey_version"]
		if m := f["server_mode"]; m != "" {
			s.Mode = m
		}
	}
	return s
}

// String returns "Product Version", for example "Valkey 8.1.12".
func (s Server) String() string {
	if s.Version == "" {
		return s.Product
	}
	return s.Product + " " + s.Version
}

// ParseInfoField extracts a field from INFO output.
func ParseInfoField(info, field string) string { return infoFields(info)[field] }

func infoFields(info string) map[string]string {
	m := map[string]string{}
	for _, line := range strings.Split(info, "\n") {
		if k, v, ok := strings.Cut(strings.TrimSpace(line), ":"); ok && !strings.HasPrefix(k, "#") {
			m[k] = v
		}
	}
	return m
}
