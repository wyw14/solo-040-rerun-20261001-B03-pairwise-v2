package cycle

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

const ackTestChamber = "chamber-1"

type ackTestEnv struct {
	coord    *Coordinator
	gates    *gate.Controller
	commands *gate.Commands
	state    *gate.State
	store    *journal.MemoryStore
}

func newAckTestEnv(t *testing.T) *ackTestEnv {
	t.Helper()
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, time.Now)
	state := gate.NewState(ackTestChamber)
	commands := gate.NewCommands()
	gates := gate.NewController(state, commands, recorder, time.Now)
	hydro := hydraulics.NewController(commands, recorder, hydraulics.NewRamp(time.Millisecond, time.Now), time.Now)
	return &ackTestEnv{
		coord:    NewCoordinator(gates, hydro, recorder, time.Now),
		gates:    gates,
		commands: commands,
		state:    state,
		store:    store,
	}
}

func (e *ackTestEnv) startCycle(t *testing.T) model.TransitCycle {
	t.Helper()
	vessel, err := model.NewVessel("test-vessel", "TST", 80)
	if err != nil {
		t.Fatalf("new vessel: %v", err)
	}
	plan, err := model.NewConvoyPlan(ackTestChamber, model.DirectionUpstream, []model.Vessel{vessel}, time.Now())
	if err != nil {
		t.Fatalf("new plan: %v", err)
	}
	cycleValue, err := e.coord.Start(context.Background(), plan)
	if err != nil {
		t.Fatalf("start cycle: %v", err)
	}
	return cycleValue
}

func (e *ackTestEnv) requestGate(t *testing.T, cycleValue model.TransitCycle) model.DeviceCommand {
	t.Helper()
	command, err := e.gates.Request(context.Background(), cycleValue, model.DirectionUpstream, true)
	if err != nil {
		t.Fatalf("request gate: %v", err)
	}
	return command
}

func (e *ackTestEnv) events(t *testing.T) []model.Event {
	t.Helper()
	events, err := e.store.Events(context.Background(), ackTestChamber, 0)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	return events
}

func ackFor(command model.DeviceCommand) model.DeviceAck {
	return model.DeviceAck{
		CommandID: command.ID, CycleID: command.CycleID, ChamberID: command.ChamberID,
		Generation: command.Generation, Epoch: command.Epoch, Device: command.Device,
		Settled: true, ObservedAt: time.Now().UTC(),
	}
}

func countEvents(events []model.Event, kind model.EventType) int {
	count := 0
	for _, event := range events {
		if event.Type == kind {
			count++
		}
	}
	return count
}

func TestApplyGateAckAfterCancelDoesNotMutate(t *testing.T) {
	env := newAckTestEnv(t)
	cycleValue := env.startCycle(t)
	command := env.requestGate(t, cycleValue)
	if _, err := env.coord.Cancel(context.Background(), ackTestChamber, "operator cancellation"); err != nil {
		t.Fatalf("cancel cycle: %v", err)
	}
	eventsBefore := len(env.events(t))

	err := env.coord.ApplyGateAck(context.Background(), ackFor(command))
	if err == nil {
		t.Fatal("expected late ack for a cancelled transit to be rejected")
	}

	stored, ok := env.commands.Get(command.ID)
	if !ok {
		t.Fatal("command missing after rejected ack")
	}
	if stored.State != model.CommandQueued {
		t.Errorf("command state = %s, want %s", stored.State, model.CommandQueued)
	}
	if position := env.state.Position(ackTestChamber, model.DirectionUpstream).Position; position != model.GateOpening {
		t.Errorf("gate position = %s, want %s", position, model.GateOpening)
	}
	events := env.events(t)
	if len(events) != eventsBefore {
		t.Errorf("event count = %d, want %d (no events on rejection)", len(events), eventsBefore)
	}
	if count := countEvents(events, model.EventCommandSettled); count != 0 {
		t.Errorf("command.settled events = %d, want 0", count)
	}
}

func TestApplyGateAckFromOldGenerationDoesNotMutate(t *testing.T) {
	env := newAckTestEnv(t)
	first := env.startCycle(t)
	staleCommand := env.requestGate(t, first)
	if _, err := env.coord.Cancel(context.Background(), ackTestChamber, "operator cancellation"); err != nil {
		t.Fatalf("cancel first cycle: %v", err)
	}
	second := env.startCycle(t)
	if second.Generation != first.Generation+1 {
		t.Fatalf("second generation = %d, want %d", second.Generation, first.Generation+1)
	}
	liveCommand := env.requestGate(t, second)
	eventsBefore := len(env.events(t))

	err := env.coord.ApplyGateAck(context.Background(), ackFor(staleCommand))
	if err == nil {
		t.Fatal("expected ack for a superseded generation to be rejected")
	}

	stored, _ := env.commands.Get(staleCommand.ID)
	if stored.State != model.CommandQueued {
		t.Errorf("stale command state = %s, want %s", stored.State, model.CommandQueued)
	}
	live, _ := env.commands.Get(liveCommand.ID)
	if live.State != model.CommandQueued {
		t.Errorf("live command state = %s, want %s", live.State, model.CommandQueued)
	}
	if position := env.state.Position(ackTestChamber, model.DirectionUpstream).Position; position != model.GateOpening {
		t.Errorf("gate position = %s, want %s", position, model.GateOpening)
	}
	current, ok := env.coord.Current(ackTestChamber)
	if !ok || current.Stage != model.StagePlanned {
		t.Errorf("current stage = %s, want %s", current.Stage, model.StagePlanned)
	}
	events := env.events(t)
	if len(events) != eventsBefore {
		t.Errorf("event count = %d, want %d (no events on rejection)", len(events), eventsBefore)
	}
}

func TestApplyGateAckDuplicateDoesNotMutate(t *testing.T) {
	env := newAckTestEnv(t)
	cycleValue := env.startCycle(t)
	command := env.requestGate(t, cycleValue)
	if err := env.coord.ApplyGateAck(context.Background(), ackFor(command)); err != nil {
		t.Fatalf("first ack: %v", err)
	}
	eventsBefore := len(env.events(t))

	err := env.coord.ApplyGateAck(context.Background(), ackFor(command))
	if err == nil {
		t.Fatal("expected duplicate ack to be rejected")
	}

	stored, _ := env.commands.Get(command.ID)
	if stored.State != model.CommandSettled {
		t.Errorf("command state = %s, want %s", stored.State, model.CommandSettled)
	}
	if position := env.state.Position(ackTestChamber, model.DirectionUpstream).Position; position != model.GateOpen {
		t.Errorf("gate position = %s, want %s", position, model.GateOpen)
	}
	current, _ := env.coord.Current(ackTestChamber)
	if current.Stage != model.StageAdmitting {
		t.Errorf("current stage = %s, want %s", current.Stage, model.StageAdmitting)
	}
	events := env.events(t)
	if len(events) != eventsBefore {
		t.Errorf("event count = %d, want %d (no events on rejection)", len(events), eventsBefore)
	}
	if count := countEvents(events, model.EventCommandSettled); count != 1 {
		t.Errorf("command.settled events = %d, want 1", count)
	}
}

func TestApplyGateAckBeyondGateStageDoesNotMutate(t *testing.T) {
	env := newAckTestEnv(t)
	cycleValue := env.startCycle(t)
	// Walk the transit to releasing: planned → admitting → sealed → leveling → releasing.
	for i := 0; i < 8 && cycleValue.Stage != model.StageReleasing; i++ {
		command := env.requestGate(t, cycleValue)
		if err := env.coord.ApplyGateAck(context.Background(), ackFor(command)); err != nil {
			t.Fatalf("ack advancing from %s: %v", cycleValue.Stage, err)
		}
		cycleValue, _ = env.coord.Current(ackTestChamber)
	}
	if cycleValue.Stage != model.StageReleasing {
		t.Fatalf("stage = %s, want %s", cycleValue.Stage, model.StageReleasing)
	}
	extra := env.requestGate(t, cycleValue)
	eventsBefore := len(env.events(t))

	err := env.coord.ApplyGateAck(context.Background(), ackFor(extra))
	if err == nil {
		t.Fatal("expected ack beyond the last gate stage to be rejected")
	}

	stored, _ := env.commands.Get(extra.ID)
	if stored.State != model.CommandQueued {
		t.Errorf("command state = %s, want %s", stored.State, model.CommandQueued)
	}
	if position := env.state.Position(ackTestChamber, model.DirectionUpstream).Position; position != model.GateOpening {
		t.Errorf("gate position = %s, want %s", position, model.GateOpening)
	}
	current, _ := env.coord.Current(ackTestChamber)
	if current.Stage != model.StageReleasing {
		t.Errorf("current stage = %s, want %s", current.Stage, model.StageReleasing)
	}
	events := env.events(t)
	if len(events) != eventsBefore {
		t.Errorf("event count = %d, want %d (no events on rejection)", len(events), eventsBefore)
	}
}

func TestApplyGateAckAdvancesCurrentCycle(t *testing.T) {
	env := newAckTestEnv(t)
	cycleValue := env.startCycle(t)
	command := env.requestGate(t, cycleValue)

	if err := env.coord.ApplyGateAck(context.Background(), ackFor(command)); err != nil {
		t.Fatalf("legitimate ack: %v", err)
	}

	stored, _ := env.commands.Get(command.ID)
	if stored.State != model.CommandSettled {
		t.Errorf("command state = %s, want %s", stored.State, model.CommandSettled)
	}
	if position := env.state.Position(ackTestChamber, model.DirectionUpstream).Position; position != model.GateOpen {
		t.Errorf("gate position = %s, want %s", position, model.GateOpen)
	}
	current, ok := env.coord.Current(ackTestChamber)
	if !ok || current.Stage != model.StageAdmitting {
		t.Errorf("current stage = %s, want %s", current.Stage, model.StageAdmitting)
	}
	events := env.events(t)
	if count := countEvents(events, model.EventCommandSettled); count != 1 {
		t.Errorf("command.settled events = %d, want 1", count)
	}
	if count := countEvents(events, model.EventCycleAdvanced); count != 1 {
		t.Errorf("cycle.advanced events = %d, want 1", count)
	}
}
