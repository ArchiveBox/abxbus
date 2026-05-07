package abxbus

import "github.com/google/uuid"

func newUUIDv7String() string {
	return uuid.Must(uuid.NewV7()).String()
}

func handlerIDNamespace() uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceDNS, []byte("abxbus-handler"))
}

func ComputeHandlerID(eventbusID string, handlerName string, handlerFilePath *string, handlerRegisteredAt string, eventPattern string) string {
	filePath := "unknown"
	if handlerFilePath != nil {
		filePath = *handlerFilePath
	}
	seed := eventbusID + "|" + handlerName + "|" + filePath + "|" + handlerRegisteredAt + "|" + eventPattern
	return uuid.NewSHA1(handlerIDNamespace(), []byte(seed)).String()
}
