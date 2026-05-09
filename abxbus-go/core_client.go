package abxbus

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/riyaneel/tachyon/bindings/go/tachyon"
	"github.com/vmihailenco/msgpack/v5"
)

const CoreProtocolVersion = 1
const coreRequestTypeID uint32 = 1
const fastHandlerCompletedTypeID uint32 = 3
const fastAckResponseTypeID uint32 = 4
const fastErrorResponseTypeID uint32 = 5
const fastMessagesResponseTypeID uint32 = 6
const fastQueueJumpTypeID uint32 = 7
const fastHandlerOutcomeErrorFlag uint16 = 0b0100_0000
const coreSpinThreshold uint32 = 50_000_000
const corePollingMode = 1
const handlerOutcomeBatchLimit = 4096
const coreStartupTimeout = 120 * time.Second

type CoreProtocolEnvelope struct {
	ProtocolVersion int            `json:"protocol_version" msgpack:"protocol_version"`
	SessionID       string         `json:"session_id" msgpack:"session_id"`
	RequestID       string         `json:"request_id,omitempty" msgpack:"request_id,omitempty"`
	LastPatchSeq    *uint64        `json:"last_patch_seq,omitempty" msgpack:"last_patch_seq,omitempty"`
	Message         map[string]any `json:"message" msgpack:"message"`
}

type coreProtocolRequestEnvelope struct {
	ProtocolVersion int     `msgpack:"protocol_version"`
	SessionID       string  `msgpack:"session_id"`
	RequestID       string  `msgpack:"request_id,omitempty"`
	LastPatchSeq    *uint64 `msgpack:"last_patch_seq,omitempty"`
	Message         any     `msgpack:"message"`
}

type RustCoreClient struct {
	sessionID    string
	socketPath   string
	busName      string
	command      string
	args         []string
	cmd          *exec.Cmd
	rpc          *tachyon.RpcBus
	mu           sync.Mutex
	lastPatchSeq uint64
	killCmd      bool
	closed       bool
}

func NewRustCoreClient(ctx context.Context, command string, args ...string) (*RustCoreClient, error) {
	socketPath := filepath.Join(os.TempDir(), "abxbus-core-"+uuid.NewString()+".sock")
	return newRustCoreClientAtSocket(ctx, socketPath, command, args...)
}

func NewRustCoreClientForBusName(ctx context.Context, busName string) (*RustCoreClient, error) {
	registryPath := StableCoreSocketPath(busName)
	command, args := DefaultCoreDaemonCommand(registryPath)
	client := &RustCoreClient{
		sessionID:  "go-" + uuid.NewString(),
		socketPath: registryPath,
		busName:    busName,
		command:    command,
		args:       args,
		killCmd:    false,
	}
	if _, statErr := os.Stat(registryPath); statErr == nil {
		if rpc, err := client.requestNamedSession(ctx, busName, 100*time.Millisecond); err == nil {
			client.rpc = rpc
			return client, nil
		}
		if err := client.ensureNamedDaemon(ctx, false); err != nil {
			return nil, err
		}
	} else {
		if err := client.ensureNamedDaemon(ctx, false); err != nil {
			return nil, err
		}
	}
	rpc, err := client.requestNamedSession(ctx, busName, coreStartupTimeout)
	if err != nil {
		if err := client.ensureNamedDaemon(ctx, true); err != nil {
			return nil, err
		}
		rpc, err = client.requestNamedSession(ctx, busName, coreStartupTimeout)
		if err != nil {
			return nil, err
		}
	}
	client.rpc = rpc
	return client, nil
}

func newRustCoreClientAtSocket(ctx context.Context, socketPath string, command string, args ...string) (*RustCoreClient, error) {
	if command == "" {
		command, args = DefaultCoreCommand(socketPath)
	} else {
		args = append(args, socketPath)
	}
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Env = coreProcessEnv(true)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	rpc, err := connectTachyonRPC(ctx, socketPath, cmd)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = os.Remove(socketPath)
		return nil, err
	}
	return &RustCoreClient{
		sessionID:  "go-" + uuid.NewString(),
		socketPath: socketPath,
		command:    command,
		args:       args,
		cmd:        cmd,
		rpc:        rpc,
		killCmd:    true,
	}, nil
}

func DefaultCoreCommand(socketPath string) (string, []string) {
	return defaultCoreCommand(socketPath, false)
}

func DefaultCoreDaemonCommand(socketPath string) (string, []string) {
	return defaultCoreCommand(socketPath, true)
}

func defaultCoreCommand(socketPath string, daemon bool) (string, []string) {
	socketArgs := []string{socketPath}
	if daemon {
		socketArgs = []string{"--daemon", socketPath}
	}
	if configured := os.Getenv("ABXBUS_CORE_TACHYON_BIN"); configured != "" {
		return configured, socketArgs
	}
	cwd, err := os.Getwd()
	if err != nil {
		cwd = "."
	}
	manifest := filepath.Join(cwd, "abxbus-core", "Cargo.toml")
	coreDir := filepath.Dir(manifest)
	for dir := cwd; ; dir = filepath.Dir(dir) {
		candidate := filepath.Join(dir, "abxbus-core", "Cargo.toml")
		if _, err := os.Stat(candidate); err == nil {
			manifest = candidate
			coreDir = filepath.Dir(candidate)
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
	}
	if _, err := os.Stat(filepath.Join(coreDir, "target", "release", "abxbus-core-tachyon")); err == nil {
		return filepath.Join(coreDir, "target", "release", "abxbus-core-tachyon"), socketArgs
	}
	if _, err := os.Stat(filepath.Join(coreDir, "target", "debug", "abxbus-core-tachyon")); err == nil {
		return filepath.Join(coreDir, "target", "debug", "abxbus-core-tachyon"), socketArgs
	}
	args := []string{"run", "--quiet", "--manifest-path", manifest, "--bin", "abxbus-core-tachyon", "--"}
	args = append(args, socketArgs...)
	return "cargo", args
}

func connectTachyonRPC(ctx context.Context, socketPath string, cmd *exec.Cmd) (*tachyon.RpcBus, error) {
	return connectTachyonRPCWithTimeout(ctx, socketPath, cmd, coreStartupTimeout)
}

func connectTachyonRPCWithTimeout(ctx context.Context, socketPath string, cmd *exec.Cmd, timeout time.Duration) (*tachyon.RpcBus, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if cmd != nil && cmd.ProcessState != nil {
			return nil, errors.New("rust tachyon core exited before connect")
		}
		rpc, err := tachyon.RpcConnect(socketPath)
		if err == nil {
			rpc.SetPollingMode(corePollingMode)
			return rpc, nil
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	return nil, lastErr
}

func StableCoreSocketPath(_ string) string {
	namespace := os.Getenv("ABXBUS_CORE_NAMESPACE")
	if namespace == "" {
		namespace = fmt.Sprintf("process-%d", os.Getpid())
	}
	sum := sha256.Sum256([]byte(namespace))
	return filepath.Join(os.TempDir(), "abxbus-core-"+hex.EncodeToString(sum[:])[:24]+".sock")
}

func StableCoreSessionSocketPath(busName string) string {
	sum := sha256.Sum256([]byte(busName))
	return filepath.Join(os.TempDir(), "abxbus-core-"+hex.EncodeToString(sum[:])[:16]+"-"+uuid.NewString()[:8]+".sock")
}

func NamedCoreLockPath(socketPath string) string {
	return socketPath + ".lock"
}

func (c *RustCoreClient) ensureNamedDaemon(ctx context.Context, forceRestart bool) error {
	lockPath := NamedCoreLockPath(c.socketPath)
	if forceRestart {
		_ = os.Remove(lockPath)
	}
	cmd := exec.Command(c.command, c.args...)
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Env = coreProcessEnv(os.Getenv("ABXBUS_CORE_NAMESPACE") == "")
	if err := cmd.Start(); err != nil {
		return err
	}
	c.cmd = cmd
	return nil
}

func coreProcessEnv(ownerPID bool) []string {
	env := os.Environ()
	if ownerPID {
		return append(env, fmt.Sprintf("ABXBUS_CORE_OWNER_PID=%d", os.Getpid()))
	}
	filtered := env[:0]
	for _, entry := range env {
		if entry == "ABXBUS_CORE_OWNER_PID" || len(entry) > len("ABXBUS_CORE_OWNER_PID=") && entry[:len("ABXBUS_CORE_OWNER_PID=")] == "ABXBUS_CORE_OWNER_PID=" {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

func (c *RustCoreClient) requestNamedSession(ctx context.Context, busName string, timeout time.Duration) (*tachyon.RpcBus, error) {
	sessionSocket := StableCoreSessionSocketPath(busName)
	control, err := connectTachyonRPCWithTimeout(ctx, c.socketPath, nil, timeout)
	if err != nil {
		return nil, err
	}
	defer control.Close()
	payload, _ := msgpack.Marshal(map[string]any{"socket_path": sessionSocket})
	cid, err := control.Call(payload, coreRequestTypeID)
	if err != nil {
		return nil, err
	}
	response, err := control.Wait(cid, coreSpinThreshold)
	if err != nil {
		return nil, err
	}
	defer response.Commit()
	var ack map[string]any
	if err := msgpack.Unmarshal(response.Data(), &ack); err != nil {
		return nil, err
	}
	if ok, _ := ack["ok"].(bool); !ok {
		return nil, errors.New("named core rejected session request")
	}
	rpc, err := connectTachyonRPC(ctx, sessionSocket, nil)
	if err != nil {
		_ = os.Remove(sessionSocket)
		return nil, err
	}
	return rpc, nil
}

func (c *RustCoreClient) requestNamedStop(ctx context.Context) error {
	control, err := connectTachyonRPCWithTimeout(ctx, c.socketPath, nil, 30*time.Second)
	if err != nil {
		return err
	}
	defer control.Close()
	payload, _ := msgpack.Marshal(map[string]any{"stop": true})
	cid, err := control.Call(payload, coreRequestTypeID)
	if err != nil {
		return err
	}
	response, err := control.Wait(cid, coreSpinThreshold)
	if err != nil {
		return err
	}
	defer response.Commit()
	var ack map[string]any
	if err := msgpack.Unmarshal(response.Data(), &ack); err != nil {
		return err
	}
	if ok, _ := ack["ok"].(bool); !ok {
		return errors.New("named core rejected stop request")
	}
	return nil
}

type coreRequestOptions struct {
	IncludePatches               bool
	AdvancePatchSeq              bool
	AdvancePatchSeqWhenNoPatches bool
}

func (c *RustCoreClient) Request(message any) ([]CoreProtocolEnvelope, error) {
	return c.RequestWithOptions(message, coreRequestOptions{IncludePatches: true})
}

func (c *RustCoreClient) RequestWithOptions(message any, options coreRequestOptions) ([]CoreProtocolEnvelope, error) {
	requestID := uuid.NewString()

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.rpc == nil {
		return nil, errors.New("tachyon: RPC bus is closed")
	}
	lastPatchSeq := c.lastPatchSeq
	envelope := coreProtocolRequestEnvelope{
		ProtocolVersion: CoreProtocolVersion,
		SessionID:       c.sessionID,
		RequestID:       requestID,
		Message:         message,
	}
	if options.IncludePatches {
		envelope.LastPatchSeq = &lastPatchSeq
	}
	line, err := msgpack.Marshal(envelope)
	if err != nil {
		return nil, err
	}

	correlationID, err := c.rpc.Call(line, coreRequestTypeID)
	if err != nil {
		return nil, err
	}
	response, err := c.rpc.Wait(correlationID, coreSpinThreshold)
	if err != nil {
		return nil, err
	}
	defer response.Commit()
	var responses []CoreProtocolEnvelope
	if err := msgpack.Unmarshal(response.Data(), &responses); err != nil {
		return nil, err
	}
	hasPatch := false
	for _, envelope := range responses {
		if envelope.Message != nil && envelope.Message["type"] == "patch" {
			hasPatch = true
			break
		}
	}
	for _, envelope := range responses {
		if options.AdvancePatchSeq {
			c.ackEnvelopeLocked(envelope)
		}
		if options.AdvancePatchSeqWhenNoPatches && !hasPatch && envelope.LastPatchSeq != nil {
			c.setPatchSeqLocked(*envelope.LastPatchSeq)
		}
	}
	return responses, nil
}

func (c *RustCoreClient) GetPatchSeq() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastPatchSeq
}

func (c *RustCoreClient) SetPatchSeq(lastPatchSeq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setPatchSeqLocked(lastPatchSeq)
}

func (c *RustCoreClient) FilterUnseenPatchMessages(messages []map[string]any) []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	filtered := make([]map[string]any, 0, len(messages))
	for _, message := range messages {
		patchSeq, ok := patchSeqFromMessage(message)
		if message["type"] != "patch" || !ok || patchSeq > c.lastPatchSeq {
			filtered = append(filtered, message)
		}
	}
	return filtered
}

func (c *RustCoreClient) AckPatchMessages(messages []map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, message := range messages {
		if message["type"] != "patch" {
			continue
		}
		if patchSeq, ok := patchSeqFromMessage(message); ok {
			c.setPatchSeqLocked(patchSeq)
		}
	}
}

func (c *RustCoreClient) ackEnvelopeLocked(envelope CoreProtocolEnvelope) {
	if envelope.LastPatchSeq != nil {
		c.setPatchSeqLocked(*envelope.LastPatchSeq)
	}
	message := envelope.Message
	if message == nil {
		return
	}
	if message["type"] != "patch" {
		return
	}
	if patchSeq, ok := patchSeqFromMessage(message); ok {
		c.setPatchSeqLocked(patchSeq)
	}
}

func (c *RustCoreClient) setPatchSeqLocked(lastPatchSeq uint64) {
	if lastPatchSeq > c.lastPatchSeq {
		c.lastPatchSeq = lastPatchSeq
	}
}

func patchSeqFromMessage(message map[string]any) (uint64, bool) {
	switch value := message["patch_seq"].(type) {
	case float64:
		if value < 0 {
			return 0, false
		}
		return uint64(value), true
	case int:
		if value < 0 {
			return 0, false
		}
		return uint64(value), true
	case int64:
		if value < 0 {
			return 0, false
		}
		return uint64(value), true
	case uint64:
		return value, true
	default:
		return 0, false
	}
}

func (c *RustCoreClient) RegisterBus(bus map[string]any) ([]CoreProtocolEnvelope, error) {
	return c.RequestWithOptions(map[string]any{"type": "register_bus", "bus": bus}, coreRequestOptions{AdvancePatchSeq: true})
}

func (c *RustCoreClient) UnregisterBus(busID string) ([]CoreProtocolEnvelope, error) {
	return c.RequestWithOptions(map[string]any{"type": "unregister_bus", "bus_id": busID}, coreRequestOptions{AdvancePatchSeq: true})
}

func (c *RustCoreClient) RegisterHandler(handler map[string]any) ([]CoreProtocolEnvelope, error) {
	return c.RequestWithOptions(map[string]any{"type": "register_handler", "handler": handler}, coreRequestOptions{AdvancePatchSeq: true})
}

func (c *RustCoreClient) ImportBusSnapshot(bus map[string]any, handlers []map[string]any, events []map[string]any, pendingEventIDs []string) ([]CoreProtocolEnvelope, error) {
	return c.RequestWithOptions(map[string]any{
		"type":              "import_bus_snapshot",
		"bus":               bus,
		"handlers":          handlers,
		"events":            events,
		"pending_event_ids": pendingEventIDs,
	}, coreRequestOptions{AdvancePatchSeq: true})
}

func (c *RustCoreClient) UnregisterHandler(handlerID string) ([]CoreProtocolEnvelope, error) {
	return c.RequestWithOptions(map[string]any{"type": "unregister_handler", "handler_id": handlerID}, coreRequestOptions{AdvancePatchSeq: true})
}

func (c *RustCoreClient) DisconnectHost(hostID string) ([]CoreProtocolEnvelope, error) {
	if hostID == "" {
		hostID = c.sessionID
	}
	return c.RequestWithOptions(map[string]any{"type": "disconnect_host", "host_id": hostID}, coreRequestOptions{AdvancePatchSeq: true})
}

func (c *RustCoreClient) StopCore(ctx context.Context) ([]CoreProtocolEnvelope, error) {
	if c.busName != "" {
		if err := c.requestNamedStop(ctx); err != nil {
			return nil, err
		}
		return []CoreProtocolEnvelope{}, nil
	}
	return c.RequestWithOptions(map[string]any{"type": "stop_core"}, coreRequestOptions{AdvancePatchSeq: true})
}

func (c *RustCoreClient) EmitEvent(event map[string]any, busID string) ([]CoreProtocolEnvelope, error) {
	return c.EmitEventWithDefer(event, busID, true)
}

func (c *RustCoreClient) EmitEventWithDefer(event map[string]any, busID string, deferStart bool) ([]CoreProtocolEnvelope, error) {
	return c.EmitEventWithDeferAndParent(event, busID, deferStart, "", false, false)
}

func (c *RustCoreClient) EmitEventWithDeferAndParent(event map[string]any, busID string, deferStart bool, parentInvocationID string, blockParentCompletion bool, pauseParentRoute bool) ([]CoreProtocolEnvelope, error) {
	return c.emitEventWithDeferAndParent(event, busID, deferStart, parentInvocationID, blockParentCompletion, pauseParentRoute, false)
}

func (c *RustCoreClient) EmitEventWithDeferAndParentCompact(event map[string]any, busID string, deferStart bool, parentInvocationID string, blockParentCompletion bool, pauseParentRoute bool) ([]CoreProtocolEnvelope, error) {
	return c.emitEventWithDeferAndParent(event, busID, deferStart, parentInvocationID, blockParentCompletion, pauseParentRoute, true)
}

func (c *RustCoreClient) emitEventWithDeferAndParent(event map[string]any, busID string, deferStart bool, parentInvocationID string, blockParentCompletion bool, pauseParentRoute bool, compactResponse bool) ([]CoreProtocolEnvelope, error) {
	request := map[string]any{"type": "emit_event", "event": event, "bus_id": busID, "defer_start": deferStart, "compact_response": compactResponse}
	if parentInvocationID != "" {
		request["parent_invocation_id"] = parentInvocationID
		request["block_parent_completion"] = blockParentCompletion
		request["pause_parent_route"] = pauseParentRoute
	}
	return c.RequestWithOptions(
		request,
		coreRequestOptions{IncludePatches: true, AdvancePatchSeqWhenNoPatches: compactResponse},
	)
}

func (c *RustCoreClient) ForwardEventWithDeferAndParent(eventID string, busID string, deferStart bool, parentInvocationID string, blockParentCompletion bool, pauseParentRoute bool) ([]CoreProtocolEnvelope, error) {
	return c.forwardEventWithDeferAndParent(eventID, busID, deferStart, parentInvocationID, blockParentCompletion, pauseParentRoute, false, nil)
}

func (c *RustCoreClient) ForwardEventWithDeferAndParentCompact(eventID string, busID string, deferStart bool, parentInvocationID string, blockParentCompletion bool, pauseParentRoute bool) ([]CoreProtocolEnvelope, error) {
	return c.forwardEventWithDeferAndParent(eventID, busID, deferStart, parentInvocationID, blockParentCompletion, pauseParentRoute, true, nil)
}

func (c *RustCoreClient) ForwardEventWithDeferAndParentCompactOptions(eventID string, busID string, deferStart bool, parentInvocationID string, blockParentCompletion bool, pauseParentRoute bool, eventOptions map[string]any) ([]CoreProtocolEnvelope, error) {
	return c.forwardEventWithDeferAndParent(eventID, busID, deferStart, parentInvocationID, blockParentCompletion, pauseParentRoute, true, eventOptions)
}

func (c *RustCoreClient) ForwardEventWithDeferAndParentOptions(eventID string, busID string, deferStart bool, parentInvocationID string, blockParentCompletion bool, pauseParentRoute bool, eventOptions map[string]any) ([]CoreProtocolEnvelope, error) {
	return c.forwardEventWithDeferAndParent(eventID, busID, deferStart, parentInvocationID, blockParentCompletion, pauseParentRoute, false, eventOptions)
}

func (c *RustCoreClient) forwardEventWithDeferAndParent(eventID string, busID string, deferStart bool, parentInvocationID string, blockParentCompletion bool, pauseParentRoute bool, compactResponse bool, eventOptions map[string]any) ([]CoreProtocolEnvelope, error) {
	request := map[string]any{"type": "forward_event", "event_id": eventID, "bus_id": busID, "defer_start": deferStart, "compact_response": compactResponse}
	if parentInvocationID != "" {
		request["parent_invocation_id"] = parentInvocationID
		request["block_parent_completion"] = blockParentCompletion
		request["pause_parent_route"] = pauseParentRoute
	}
	for key, value := range eventOptions {
		if value != nil {
			request[key] = value
		}
	}
	return c.RequestWithOptions(
		request,
		coreRequestOptions{IncludePatches: true, AdvancePatchSeqWhenNoPatches: compactResponse},
	)
}

func (c *RustCoreClient) ProcessNextRoute(busID string) ([]CoreProtocolEnvelope, error) {
	return c.RequestWithOptions(
		map[string]any{"type": "process_next_route", "bus_id": busID, "compact_response": false},
		coreRequestOptions{IncludePatches: true},
	)
}

func (c *RustCoreClient) ProcessNextRouteLimited(busID string, limit int) ([]CoreProtocolEnvelope, error) {
	return c.RequestWithOptions(
		map[string]any{"type": "process_next_route", "bus_id": busID, "limit": limit, "compact_response": true},
		coreRequestOptions{IncludePatches: true, AdvancePatchSeqWhenNoPatches: true},
	)
}

func (c *RustCoreClient) WaitInvocations(busID *string, limit *int) ([]CoreProtocolEnvelope, error) {
	return c.Request(map[string]any{"type": "wait_invocations", "bus_id": busID, "limit": limit})
}

func (c *RustCoreClient) WaitEventCompleted(eventID string) ([]CoreProtocolEnvelope, error) {
	return c.Request(map[string]any{"type": "wait_event_completed", "event_id": eventID})
}

func (c *RustCoreClient) ProcessRoute(routeID string) ([]CoreProtocolEnvelope, error) {
	return c.RequestWithOptions(
		map[string]any{"type": "process_route", "route_id": routeID},
		coreRequestOptions{IncludePatches: true, AdvancePatchSeqWhenNoPatches: true},
	)
}

func (c *RustCoreClient) ProcessRouteCompact(routeID string, limit int) ([]CoreProtocolEnvelope, error) {
	return c.RequestWithOptions(
		map[string]any{"type": "process_route", "route_id": routeID, "limit": limit, "compact_response": true},
		coreRequestOptions{IncludePatches: true, AdvancePatchSeqWhenNoPatches: true},
	)
}

func (c *RustCoreClient) QueueJumpEvent(eventID string, parentInvocationID string) ([]CoreProtocolEnvelope, error) {
	return c.QueueJumpEventWithOptions(eventID, parentInvocationID, true, true)
}

func (c *RustCoreClient) QueueJumpEventWithOptions(eventID string, parentInvocationID string, blockParentCompletion bool, pauseParentRoute bool) ([]CoreProtocolEnvelope, error) {
	return c.QueueJumpEventWithResponseOptions(eventID, parentInvocationID, blockParentCompletion, pauseParentRoute, true)
}

func (c *RustCoreClient) QueueJumpEventWithResponseOptions(eventID string, parentInvocationID string, blockParentCompletion bool, pauseParentRoute bool, includePatches bool) ([]CoreProtocolEnvelope, error) {
	return c.requestFastQueueJumpEvent(eventID, parentInvocationID, blockParentCompletion, pauseParentRoute, true, includePatches)
}

func (c *RustCoreClient) AwaitEvent(eventID string, parentInvocationID *string) ([]CoreProtocolEnvelope, error) {
	return c.Request(map[string]any{"type": "await_event", "event_id": eventID, "parent_invocation_id": parentInvocationID})
}

func (c *RustCoreClient) GetEvent(eventID string) (map[string]any, error) {
	responses, err := c.RequestWithOptions(map[string]any{"type": "get_event", "event_id": eventID}, coreRequestOptions{})
	if err != nil {
		return nil, err
	}
	for _, response := range responses {
		message := response.Message
		if message["type"] != "event_snapshot" {
			continue
		}
		event, _ := message["event"].(map[string]any)
		return event, nil
	}
	return nil, nil
}

func (c *RustCoreClient) ListEvents(eventPattern string, limit *int) ([]map[string]any, error) {
	return c.ListBusEvents("", eventPattern, limit)
}

func (c *RustCoreClient) ListBusEvents(busID string, eventPattern string, limit *int) ([]map[string]any, error) {
	if eventPattern == "" {
		eventPattern = "*"
	}
	request := map[string]any{"type": "list_events", "event_pattern": eventPattern, "limit": limit}
	if busID != "" {
		request["bus_id"] = busID
	}
	responses, err := c.RequestWithOptions(request, coreRequestOptions{})
	if err != nil {
		return nil, err
	}
	for _, response := range responses {
		message := response.Message
		if message["type"] != "event_list" {
			continue
		}
		rawEvents, _ := message["events"].([]any)
		events := make([]map[string]any, 0, len(rawEvents))
		for _, raw := range rawEvents {
			if event, ok := raw.(map[string]any); ok {
				events = append(events, event)
			}
		}
		return events, nil
	}
	return []map[string]any{}, nil
}

func (c *RustCoreClient) CompleteHandler(invocation map[string]any, value any, resultIsEventReference bool) ([]CoreProtocolEnvelope, error) {
	return c.completeHandlerFast(invocation, value, resultIsEventReference, false, false, false, true)
}

func (c *RustCoreClient) completeHandler(invocation map[string]any, value any, resultIsEventReference bool, processRouteAfter bool, processAvailableAfter bool) ([]CoreProtocolEnvelope, error) {
	return c.completeHandlerFast(invocation, value, resultIsEventReference, processRouteAfter, processAvailableAfter, true, false)
}

func (c *RustCoreClient) completeHandlerNoPatches(invocation map[string]any, value any, resultIsEventReference bool, processRouteAfter bool, processAvailableAfter bool) ([]CoreProtocolEnvelope, error) {
	return c.completeHandlerFast(invocation, value, resultIsEventReference, processRouteAfter, processAvailableAfter, true, false)
}

func (c *RustCoreClient) completeHandlerFast(invocation map[string]any, value any, resultIsEventReference bool, processRouteAfter bool, processAvailableAfter bool, compactResponse bool, includePatches bool) ([]CoreProtocolEnvelope, error) {
	resultID, err := requiredFastString(invocation["result_id"], "result_id")
	if err != nil {
		return nil, err
	}
	invocationID, err := requiredFastString(invocation["invocation_id"], "invocation_id")
	if err != nil {
		return nil, err
	}
	fence, err := requiredFastUint64(invocation["fence"], "fence")
	if err != nil {
		return nil, err
	}
	var flags uint16
	var valueBytes []byte
	if resultIsEventReference {
		flags |= 0b0000_0001
	}
	if value != nil {
		coreValue, err := coreJSONCompatibleValue(value)
		if err != nil {
			return nil, err
		}
		valueBytes, err = msgpack.Marshal(coreValue)
		if err != nil {
			return nil, err
		}
	}
	if processRouteAfter {
		flags |= 0b0000_0100
	}
	if processAvailableAfter {
		flags |= 0b0000_1000
	}
	if compactResponse {
		flags |= 0b0001_0000
	}
	if includePatches {
		flags |= 0b0010_0000
	}
	sessionBytes := []byte(c.sessionID)
	resultBytes := []byte(resultID)
	invocationBytes := []byte(invocationID)
	if len(sessionBytes) > 0xffff || len(resultBytes) > 0xffff || len(invocationBytes) > 0xffff {
		return nil, errors.New("fast handler completion id fields exceed 65535 bytes")
	}
	if uint64(len(valueBytes)) > 0xffffffff {
		return nil, errors.New("fast handler completion value exceeds 4294967295 bytes")
	}
	lastPatchSeq := c.GetPatchSeq()
	payload := make([]byte, 30+len(sessionBytes)+len(resultBytes)+len(invocationBytes)+len(valueBytes))
	offset := 0
	binary.LittleEndian.PutUint16(payload[offset:], CoreProtocolVersion)
	offset += 2
	binary.LittleEndian.PutUint16(payload[offset:], flags)
	offset += 2
	binary.LittleEndian.PutUint64(payload[offset:], fence)
	offset += 8
	binary.LittleEndian.PutUint64(payload[offset:], lastPatchSeq)
	offset += 8
	binary.LittleEndian.PutUint16(payload[offset:], uint16(len(sessionBytes)))
	offset += 2
	binary.LittleEndian.PutUint16(payload[offset:], uint16(len(resultBytes)))
	offset += 2
	binary.LittleEndian.PutUint16(payload[offset:], uint16(len(invocationBytes)))
	offset += 2
	binary.LittleEndian.PutUint32(payload[offset:], uint32(len(valueBytes)))
	offset += 4
	copy(payload[offset:], sessionBytes)
	offset += len(sessionBytes)
	copy(payload[offset:], resultBytes)
	offset += len(resultBytes)
	copy(payload[offset:], invocationBytes)
	offset += len(invocationBytes)
	copy(payload[offset:], valueBytes)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.rpc == nil {
		return nil, errors.New("tachyon: RPC bus is closed")
	}
	return c.waitFastCoreMessagesLocked(payload, fastHandlerCompletedTypeID, "fast handler completion")
}

func (c *RustCoreClient) requestFastQueueJumpEvent(eventID string, parentInvocationID string, blockParentCompletion bool, pauseParentRoute bool, compactResponse bool, includePatches bool) ([]CoreProtocolEnvelope, error) {
	if eventID == "" {
		return nil, errors.New("fast queue jump requires string event_id")
	}
	if parentInvocationID == "" {
		return nil, errors.New("fast queue jump requires string parent_invocation_id")
	}
	var flags uint16
	if blockParentCompletion {
		flags |= 0b0000_0001
	}
	if pauseParentRoute {
		flags |= 0b0000_0010
	}
	if compactResponse {
		flags |= 0b0000_0100
	}
	if includePatches {
		flags |= 0b0000_1000
	}
	sessionBytes := []byte(c.sessionID)
	parentBytes := []byte(parentInvocationID)
	eventBytes := []byte(eventID)
	if len(sessionBytes) > 0xffff || len(parentBytes) > 0xffff || len(eventBytes) > 0xffff {
		return nil, errors.New("fast queue jump id fields exceed 65535 bytes")
	}
	lastPatchSeq := c.GetPatchSeq()
	payload := make([]byte, 18+len(sessionBytes)+len(parentBytes)+len(eventBytes))
	offset := 0
	binary.LittleEndian.PutUint16(payload[offset:], CoreProtocolVersion)
	offset += 2
	binary.LittleEndian.PutUint16(payload[offset:], flags)
	offset += 2
	binary.LittleEndian.PutUint64(payload[offset:], lastPatchSeq)
	offset += 8
	binary.LittleEndian.PutUint16(payload[offset:], uint16(len(sessionBytes)))
	offset += 2
	binary.LittleEndian.PutUint16(payload[offset:], uint16(len(parentBytes)))
	offset += 2
	binary.LittleEndian.PutUint16(payload[offset:], uint16(len(eventBytes)))
	offset += 2
	copy(payload[offset:], sessionBytes)
	offset += len(sessionBytes)
	copy(payload[offset:], parentBytes)
	offset += len(parentBytes)
	copy(payload[offset:], eventBytes)

	return c.waitFastCoreMessages(payload, fastQueueJumpTypeID, "fast queue jump")
}

func (c *RustCoreClient) waitFastCoreMessages(payload []byte, typeID uint32, label string) ([]CoreProtocolEnvelope, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waitFastCoreMessagesLocked(payload, typeID, label)
}

func (c *RustCoreClient) waitFastCoreMessagesLocked(payload []byte, typeID uint32, label string) ([]CoreProtocolEnvelope, error) {
	if c.closed || c.rpc == nil {
		return nil, errors.New("tachyon: RPC bus is closed")
	}
	correlationID, err := c.rpc.Call(payload, typeID)
	if err != nil {
		return nil, err
	}
	response, err := c.rpc.Wait(correlationID, coreSpinThreshold)
	if err != nil {
		return nil, err
	}
	defer response.Commit()
	data := response.Data()
	switch response.TypeID() {
	case fastAckResponseTypeID:
		patchSeq, err := readFastPatchSeq(data)
		if err != nil {
			return nil, err
		}
		c.setPatchSeqLocked(patchSeq)
		return []CoreProtocolEnvelope{}, nil
	case fastErrorResponseTypeID:
		return nil, errors.New(string(data))
	case fastMessagesResponseTypeID:
		patchSeq, err := readFastPatchSeq(data)
		if err != nil {
			return nil, err
		}
		var messages []map[string]any
		if err := msgpack.Unmarshal(data[8:], &messages); err != nil {
			return nil, err
		}
		if !messagesHavePatch(messages) {
			c.setPatchSeqLocked(patchSeq)
		}
		envelopes := make([]CoreProtocolEnvelope, 0, len(messages))
		for _, message := range messages {
			if message["type"] == "error" {
				return nil, errors.New(fmt.Sprint(message["message"]))
			}
			envelopes = append(envelopes, CoreProtocolEnvelope{
				ProtocolVersion: CoreProtocolVersion,
				SessionID:       c.sessionID,
				Message:         message,
			})
		}
		return envelopes, nil
	default:
		return nil, fmt.Errorf("unexpected Rust core %s response type: %d", label, response.TypeID())
	}
}

func messagesHavePatch(messages []map[string]any) bool {
	for _, message := range messages {
		if message["type"] == "patch" {
			return true
		}
	}
	return false
}

func requiredFastString(value any, label string) (string, error) {
	text, ok := value.(string)
	if !ok || text == "" {
		return "", fmt.Errorf("fast handler completion requires string %s", label)
	}
	return text, nil
}

func requiredFastUint64(value any, label string) (uint64, error) {
	switch typed := value.(type) {
	case uint64:
		return typed, nil
	case uint:
		return uint64(typed), nil
	case uint32:
		return uint64(typed), nil
	case uint16:
		return uint64(typed), nil
	case uint8:
		return uint64(typed), nil
	case int:
		if typed >= 0 {
			return uint64(typed), nil
		}
	case int64:
		if typed >= 0 {
			return uint64(typed), nil
		}
	case int32:
		if typed >= 0 {
			return uint64(typed), nil
		}
	case int16:
		if typed >= 0 {
			return uint64(typed), nil
		}
	case int8:
		if typed >= 0 {
			return uint64(typed), nil
		}
	case float64:
		if typed >= 0 && typed <= float64(^uint64(0)) && typed == float64(uint64(typed)) {
			return uint64(typed), nil
		}
	}
	return 0, fmt.Errorf("fast handler completion requires uint64 %s", label)
}

func readFastPatchSeq(data []byte) (uint64, error) {
	if len(data) < 8 {
		return 0, errors.New("fast handler completion response missing patch sequence")
	}
	return binary.LittleEndian.Uint64(data[:8]), nil
}

func (c *RustCoreClient) completeHandlerOutcomes(outcomes []map[string]any) ([]CoreProtocolEnvelope, error) {
	includePatches := len(outcomes) <= 1
	responses := make([]CoreProtocolEnvelope, 0)
	for index := 0; index < len(outcomes); index += handlerOutcomeBatchLimit {
		end := index + handlerOutcomeBatchLimit
		if end > len(outcomes) {
			end = len(outcomes)
		}
		chunkResponses, err := c.RequestWithOptions(map[string]any{
			"type":             "handler_outcomes",
			"outcomes":         outcomes[index:end],
			"compact_response": true,
		}, coreRequestOptions{
			IncludePatches:               includePatches,
			AdvancePatchSeq:              !includePatches,
			AdvancePatchSeqWhenNoPatches: true,
		})
		if err != nil {
			return nil, err
		}
		responses = append(responses, chunkResponses...)
	}
	return responses, nil
}

func (c *RustCoreClient) completedHandlerOutcome(value any, resultIsEventReference bool) map[string]any {
	return map[string]any{
		"status":                    "completed",
		"value":                     value,
		"result_is_event_reference": resultIsEventReference,
		"result_is_undefined":       false,
	}
}

func (c *RustCoreClient) handlerOutcomeRecord(invocation map[string]any, outcome map[string]any, processAvailableAfter bool) map[string]any {
	return map[string]any{
		"result_id":               invocation["result_id"],
		"invocation_id":           invocation["invocation_id"],
		"fence":                   invocation["fence"],
		"process_available_after": processAvailableAfter,
		"outcome":                 outcome,
	}
}

func (c *RustCoreClient) ErrorHandler(invocation map[string]any, err error) ([]CoreProtocolEnvelope, error) {
	return c.RequestWithOptions(map[string]any{
		"type":                    "handler_outcome",
		"result_id":               invocation["result_id"],
		"invocation_id":           invocation["invocation_id"],
		"fence":                   invocation["fence"],
		"process_route_after":     false,
		"process_available_after": false,
		"outcome":                 c.erroredHandlerOutcome(err),
	}, coreRequestOptions{IncludePatches: true})
}

func (c *RustCoreClient) errorHandler(invocation map[string]any, err error, processRouteAfter bool, processAvailableAfter bool) ([]CoreProtocolEnvelope, error) {
	return c.errorHandlerFast(invocation, err, processRouteAfter, processAvailableAfter, true)
}

func (c *RustCoreClient) errorHandlerFast(invocation map[string]any, err error, processRouteAfter bool, processAvailableAfter bool, compactResponse bool) ([]CoreProtocolEnvelope, error) {
	resultID, valueErr := requiredFastString(invocation["result_id"], "result_id")
	if valueErr != nil {
		return nil, valueErr
	}
	invocationID, valueErr := requiredFastString(invocation["invocation_id"], "invocation_id")
	if valueErr != nil {
		return nil, valueErr
	}
	fence, valueErr := requiredFastUint64(invocation["fence"], "fence")
	if valueErr != nil {
		return nil, valueErr
	}
	kind, message := handlerErrorKindMessage(err)
	valueBytes, valueErr := msgpack.Marshal(map[string]any{"kind": kind, "message": message})
	if valueErr != nil {
		return nil, valueErr
	}
	var flags uint16 = fastHandlerOutcomeErrorFlag
	if processRouteAfter {
		flags |= 0b0000_0100
	}
	if processAvailableAfter {
		flags |= 0b0000_1000
	}
	if compactResponse {
		flags |= 0b0001_0000
	}
	sessionBytes := []byte(c.sessionID)
	resultBytes := []byte(resultID)
	invocationBytes := []byte(invocationID)
	if len(sessionBytes) > 0xffff || len(resultBytes) > 0xffff || len(invocationBytes) > 0xffff {
		return nil, errors.New("fast handler error id fields exceed 65535 bytes")
	}
	payload := make([]byte, 30+len(sessionBytes)+len(resultBytes)+len(invocationBytes)+len(valueBytes))
	offset := 0
	binary.LittleEndian.PutUint16(payload[offset:], CoreProtocolVersion)
	offset += 2
	binary.LittleEndian.PutUint16(payload[offset:], flags)
	offset += 2
	binary.LittleEndian.PutUint64(payload[offset:], fence)
	offset += 8
	lastPatchSeq := c.GetPatchSeq()
	binary.LittleEndian.PutUint64(payload[offset:], lastPatchSeq)
	offset += 8
	binary.LittleEndian.PutUint16(payload[offset:], uint16(len(sessionBytes)))
	offset += 2
	binary.LittleEndian.PutUint16(payload[offset:], uint16(len(resultBytes)))
	offset += 2
	binary.LittleEndian.PutUint16(payload[offset:], uint16(len(invocationBytes)))
	offset += 2
	binary.LittleEndian.PutUint32(payload[offset:], uint32(len(valueBytes)))
	offset += 4
	copy(payload[offset:], sessionBytes)
	offset += len(sessionBytes)
	copy(payload[offset:], resultBytes)
	offset += len(resultBytes)
	copy(payload[offset:], invocationBytes)
	offset += len(invocationBytes)
	copy(payload[offset:], valueBytes)

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waitFastCoreMessagesLocked(payload, fastHandlerCompletedTypeID, "fast handler error")
}

func (c *RustCoreClient) erroredHandlerOutcome(err error) map[string]any {
	kind, message := handlerErrorKindMessage(err)
	return map[string]any{
		"status": "errored",
		"error": map[string]any{
			"kind":    kind,
			"message": message,
		},
	}
}

func handlerErrorKindMessage(err error) (string, string) {
	message := ""
	if err != nil {
		message = err.Error()
	}
	kind := "host_error"
	switch err.(type) {
	case *EventHandlerTimeoutError:
		kind = "handler_timeout"
	case *EventHandlerCancelledError:
		kind = "handler_cancelled"
	case *EventHandlerAbortedError:
		kind = "handler_aborted"
	}
	return kind, message
}

func (c *RustCoreClient) Close() error {
	return c.Disconnect()
}

func (c *RustCoreClient) Disconnect() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	rpc := c.rpc
	c.mu.Unlock()

	if c.killCmd && c.cmd != nil && c.cmd.Process != nil {
		_ = c.cmd.Process.Kill()
	}
	if rpc != nil {
		_ = rpc.Close()
	}
	if c.killCmd && c.cmd != nil && c.socketPath != "" {
		_ = os.Remove(c.socketPath)
	}
	if c.killCmd && c.cmd != nil {
		return c.cmd.Wait()
	}
	return nil
}

func (c *RustCoreClient) Stop(ctx context.Context) error {
	_, err := c.StopCore(ctx)
	closeErr := c.Disconnect()
	if err != nil {
		return err
	}
	return closeErr
}
