package terraform

import (
	"log"

	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/instances"
	"github.com/hashicorp/terraform/internal/plans"
	"github.com/hashicorp/terraform/internal/refactoring"
	"github.com/hashicorp/terraform/internal/states"
	"github.com/hashicorp/terraform/internal/tfdiags"
)

// graphWalkOpts captures some transient values we use (and possibly mutate)
// during a graph walk.
//
// The way these options get used unfortunately varies between the different
// walkOperation types. This is a historical design wart that dates back to
// us using the same graph structure for all operations; hopefully we'll
// make the necessary differences between the walk types more explicit someday.
type graphWalkOpts struct {
	InputState *states.State
	Changes    *plans.Changes

	RootVariableValues InputValues
	MoveResults        map[addrs.UniqueKey]refactoring.MoveResult
}

func (c *Context) walk(graph *Graph, operation walkOperation, opts *graphWalkOpts) (*ContextGraphWalker, tfdiags.Diagnostics) {
	log.Printf("[DEBUG] Starting graph walk: %s", operation.String())

	walker := c.graphWalker(operation, opts)

	// Watch for a stop so we can call the provider Stop() API.
	watchStop, watchWait := c.watchStop(walker)

	// Walk the real graph, this will block until it completes
	diags := graph.Walk(walker)

	// Close the channel so the watcher stops, and wait for it to return.
	close(watchStop)
	<-watchWait

	return walker, diags
}

func (c *Context) graphWalker(operation walkOperation, opts *graphWalkOpts) *ContextGraphWalker {
	var state *states.SyncState
	var refreshState *states.SyncState
	var prevRunState *states.SyncState

	switch operation {
	case walkValidate:
		// validate should not use any state
		state = states.NewState().SyncWrapper()

		// validate currently uses the plan graph, so we have to populate the
		// refreshState and the prevRunState.
		refreshState = states.NewState().SyncWrapper()
		prevRunState = states.NewState().SyncWrapper()

	case walkPlan, walkPlanDestroy:
		state = opts.InputState.DeepCopy().SyncWrapper()
		refreshState = opts.InputState.DeepCopy().SyncWrapper()
		prevRunState = opts.InputState.DeepCopy().SyncWrapper()

	default:
		state = opts.InputState.SyncWrapper()
	}

	return &ContextGraphWalker{
		Context:            c,
		State:              state,
		RefreshState:       refreshState,
		PrevRunState:       prevRunState,
		Changes:            opts.Changes.SyncWrapper(),
		InstanceExpander:   instances.NewExpander(),
		MoveResults:        opts.MoveResults,
		Operation:          operation,
		StopContext:        c.runContext,
		RootVariableValues: opts.RootVariableValues,
	}
}
