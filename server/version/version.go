package version

// Version is the demo server version. It is a var (not a const) so the release
// tooling can override it at link time via
//
//	-ldflags "-X github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/server/version.Version=<v>"
//
// (goreleaser, the Dockerfiles, and the Makefile all inject it this way). The
// Go linker's -X flag can only patch a string var — a const is silently ignored.
var Version = "0.1.0"

// Name is the demo server's product name.
var Name = "resilience-demo"
