package handler

import (
	"fmt"
	"strings"

	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/model"
)

type diagramTransition struct {
	from  model.SagaState
	to    model.SagaState
	event string
}

var diagramStates = []model.SagaState{
	model.SagaStarted,
	model.SagaPaymentPending,
	model.SagaInventoryPending,
	model.SagaCancelPaymentPending,
	model.SagaCompleted,
	model.SagaFailed,
}

// diagramTransitions contains all state changes the orchestrator can persist.
// Timeout transitions occur after the retry limit is exhausted.
var diagramTransitions = []diagramTransition{
	{model.SagaStarted, model.SagaPaymentPending, "StartSaga"},
	{model.SagaPaymentPending, model.SagaInventoryPending, "PaymentReserved"},
	{model.SagaPaymentPending, model.SagaFailed, "PaymentFailed"},
	{model.SagaInventoryPending, model.SagaCompleted, "InventoryReserved"},
	{model.SagaInventoryPending, model.SagaCancelPaymentPending, "InventoryFailed"},
	{model.SagaCancelPaymentPending, model.SagaFailed, "PaymentCancelled"},
	{model.SagaPaymentPending, model.SagaFailed, "Timeout"},
	{model.SagaInventoryPending, model.SagaCancelPaymentPending, "Timeout"},
	{model.SagaCancelPaymentPending, model.SagaFailed, "Timeout"},
}

func sagaDiagram(saga *model.SagaInstance, history []*model.SagaHistoryEntry) string {
	visitedStates := make(map[model.SagaState]bool, len(history)*2)
	for _, entry := range history {
		visitedStates[entry.FromState] = true
		visitedStates[entry.ToState] = true
	}

	var diagram strings.Builder
	diagram.WriteString("stateDiagram-v2\n")
	diagram.WriteString("    classDef completedStep fill:#15803d,color:#fff,stroke:#4ade80,stroke-width:2px\n")
	diagram.WriteString("    classDef currentStep fill:#2563eb,color:#fff,stroke:#93c5fd,stroke-width:3px\n")
	for _, state := range diagramStates {
		label := string(state)
		if state == saga.State {
			label += " (current)"
		}
		_, _ = fmt.Fprintf(&diagram, "    state %q as %s\n", label, state)
	}
	diagram.WriteString("\n    [*] --> STARTED\n")
	for _, transition := range diagramTransitions {
		_, _ = fmt.Fprintf(&diagram, "    %s --> %s : %s\n",
			transition.from, transition.to, transition.event)
	}
	diagram.WriteString("    COMPLETED --> [*]\n    FAILED --> [*]\n")

	for _, state := range diagramStates {
		switch {
		case state == saga.State:
			_, _ = fmt.Fprintf(&diagram, "    class %s currentStep\n", state)
		case visitedStates[state]:
			_, _ = fmt.Fprintf(&diagram, "    class %s completedStep\n", state)
		}
	}

	return diagram.String()
}
