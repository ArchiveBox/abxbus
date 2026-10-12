module github.com/ArchiveBox/abxbus/abxbus-go/v2

go 1.25.0

require (
	github.com/google/uuid v1.6.0
	github.com/riyaneel/tachyon/bindings/go v0.0.0
	github.com/vmihailenco/msgpack/v5 v5.4.1
)

require github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect

replace github.com/riyaneel/tachyon/bindings/go => ./third_party/tachyon-go
