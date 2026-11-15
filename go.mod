module github.com/BrianObonyo/streamforge

go 1.22

require (
	github.com/twmb/franz-go v1.18.0
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20240920120915-f1cded5cc755
	github.com/twmb/franz-go/pkg/kmsg v1.9.0
)

require (
	github.com/klauspost/compress v1.17.8 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	golang.org/x/crypto v0.23.0 // indirect
)

// golang.org/x/crypto is needed only by franz-go's kfake (used in TESTS and
// tests/tools/kafkasim, build tag `redpanda`); the default build never imports it.
// The replace points at GitHub's official mirror so the module resolves in
// environments where golang.org is unreachable. It is safe to delete if yours can
// reach golang.org. kfake is pinned to a commit contemporary with franz-go v1.18
// because later kfake versions require Go >= 1.26.
replace golang.org/x/crypto => github.com/golang/crypto v0.23.0
