package bubus

import "github.com/google/uuid"

func newUUIDv7String() string {
	return uuid.Must(uuid.NewV7()).String()
}

func deterministicUUID(seed string) string {
	return uuid.NewSHA1(uuid.NameSpaceDNS, []byte(seed)).String()
}
