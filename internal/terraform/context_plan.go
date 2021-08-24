package terraform

import (
	"fmt"
	"log"

	"github.com/zclconf/go-cty/cty"

	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/configs"
	"github.com/hashicorp/terraform/internal/instances"
	"github.com/hashicorp/terraform/internal/plans"
	"github.com/hashicorp/terraform/internal/refactoring"
	"github.com/hashicorp/terraform/internal/states"
	"github.com/hashicorp/terraform/internal/tfdiags"
)

// PlanOpts are the various options that affect the details of how Terraform
// will build a plan.
type PlanOpts struct {
	Mode         plans.Mode
	SkipRefresh  bool
	SetVariables InputValues
	Targets      []addrs.Targetable
	ForceReplace []addrs.AbsResourceInstance
}

// Plan generates an execution plan for the given context, and returns the
// refreshed state.
//
// The execution plan encapsulates the context and can be stored
// in order to reinstantiate a context later for Apply.
//
// Plan also updates the diff of this context to be the diff generated
// by the plan, so Apply can be called after.
func (c *Context) Plan(config *configs.Config, prevRunState *states.State, opts *PlanOpts) (*plans.Plan, tfdiags.Diagnostics) {
	defer c.acquireRun("plan")()
	var diags tfdiags.Diagnostics

	// Save the downstream functions from needing to deal with these broken situations.
	// No real callers should rely on these, but we have a bunch of old and
	// sloppy tests that don't always populate arguments properly.
	if config == nil {
		config = configs.NewEmptyConfig()
	}
	if prevRunState == nil {
		prevRunState = states.NewState()
	}
	if opts == nil {
		opts = &PlanOpts{
			Mode: plans.NormalMode,
		}
	}

	moreDiags := CheckCoreVersionRequirements(config)
	diags = diags.Append(moreDiags)
	// If version constraints are not met then we'll bail early since otherwise
	// we're likely to just see a bunch of other errors related to
	// incompatibilities, which could be overwhelming for the user.
	if diags.HasErrors() {
		return nil, diags
	}

	switch opts.Mode {
	case plans.NormalMode, plans.DestroyMode:
		// OK
	case plans.RefreshOnlyMode:
		if opts.SkipRefresh {
			// The CLI layer (and other similar callers) should prevent this
			// combination of options.
			diags = diags.Append(tfdiags.Sourceless(
				tfdiags.Error,
				"Incompatible plan options",
				"Cannot skip refreshing in refresh-only mode. This is a bug in Terraform.",
			))
			return nil, diags
		}
	default:
		// The CLI layer (and other similar callers) should not try to
		// create a context for a mode that Terraform Core doesn't support.
		diags = diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Unsupported plan mode",
			fmt.Sprintf("Terraform Core doesn't know how to handle plan mode %s. This is a bug in Terraform.", opts.Mode),
		))
		return nil, diags
	}
	if len(opts.ForceReplace) > 0 && opts.Mode != plans.NormalMode {
		// The other modes don't generate no-op or update actions that we might
		// upgrade to be "replace", so doesn't make sense to combine those.
		diags = diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Unsupported plan mode",
			fmt.Sprintf("Forcing resource instance replacement (with -replace=...) is allowed only in normal planning mode."),
		))
		return nil, diags
	}

	variables := mergeDefaultInputVariableValues(opts.SetVariables, config.Module.Variables)

	// By the time we get here, we should have values defined for all of
	// the root module variables, even if some of them are "unknown". It's the
	// caller's responsibility to have already handled the decoding of these
	// from the various ways the CLI allows them to be set and to produce
	// user-friendly error messages if they are not all present, and so
	// the error message from checkInputVariables should never be seen and
	// includes language asking the user to report a bug.
	varDiags := checkInputVariables(config.Module.Variables, variables)
	diags = diags.Append(varDiags)

	if len(opts.Targets) > 0 {
		diags = diags.Append(tfdiags.Sourceless(
			tfdiags.Warning,
			"Resource targeting is in effect",
			`You are creating a plan with the -target option, which means that the result of this plan may not represent all of the changes requested by the current configuration.
		
The -target option is not for routine use, and is provided only for exceptional situations such as recovering from errors or mistakes, or when Terraform specifically suggests to use it as part of an error message.`,
		))
	}

	var plan *plans.Plan
	var planDiags tfdiags.Diagnostics
	switch opts.Mode {
	case plans.NormalMode:
		plan, planDiags = c.plan(config, prevRunState, variables, opts)
	case plans.DestroyMode:
		plan, planDiags = c.destroyPlan(config, prevRunState, variables, opts)
	case plans.RefreshOnlyMode:
		plan, planDiags = c.refreshOnlyPlan(config, prevRunState, variables, opts)
	default:
		panic(fmt.Sprintf("unsupported plan mode %s", opts.Mode))
	}
	diags = diags.Append(planDiags)
	if diags.HasErrors() {
		return nil, diags
	}

	// convert the variables into the format expected for the plan
	varVals := make(map[string]plans.DynamicValue, len(variables))
	for k, iv := range variables {
		// We use cty.DynamicPseudoType here so that we'll save both the
		// value _and_ its dynamic type in the plan, so we can recover
		// exactly the same value later.
		dv, err := plans.NewDynamicValue(iv.Value, cty.DynamicPseudoType)
		if err != nil {
			diags = diags.Append(tfdiags.Sourceless(
				tfdiags.Error,
				"Failed to prepare variable value for plan",
				fmt.Sprintf("The value for variable %q could not be serialized to store in the plan: %s.", k, err),
			))
			continue
		}
		varVals[k] = dv
	}

	// insert the run-specific data from the context into the plan; variables,
	// targets and provider SHAs.
	plan.VariableValues = varVals
	plan.TargetAddrs = opts.Targets
	plan.ProviderSHA256s = c.providerSHA256s

	return plan, diags
}

var DefaultPlanOpts = &PlanOpts{
	Mode: plans.NormalMode,
}

func (c *Context) plan(config *configs.Config, prevRunState *states.State, rootVariables InputValues, opts *PlanOpts) (*plans.Plan, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics

	plan, walkDiags := c.planWalk(config, prevRunState, rootVariables, opts)
	diags = diags.Append(walkDiags)
	if diags.HasErrors() {
		return nil, diags
	}

	// The refreshed state ends up with some placeholder objects in it for
	// objects pending creation. We only really care about those being in
	// the working state, since that's what we're going to use when applying,
	// so we'll prune them all here.
	plan.PriorState.SyncWrapper().RemovePlannedResourceInstanceObjects()

	return plan, diags
}

func (c *Context) refreshOnlyPlan(config *configs.Config, prevRunState *states.State, rootVariables InputValues, opts *PlanOpts) (*plans.Plan, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics

	plan, walkDiags := c.planWalk(config, prevRunState, rootVariables, opts)
	diags = diags.Append(walkDiags)
	if diags.HasErrors() {
		return nil, diags
	}

	// If the graph builder and graph nodes correctly obeyed our directive
	// to refresh only, the set of resource changes should always be empty.
	// We'll safety-check that here so we can return a clear message about it,
	// rather than probably just generating confusing output at the UI layer.
	if len(plan.Changes.Resources) != 0 {
		// Some extra context in the logs in case the user reports this message
		// as a bug, as a starting point for debugging.
		for _, rc := range plan.Changes.Resources {
			if depKey := rc.DeposedKey; depKey == states.NotDeposed {
				log.Printf("[DEBUG] Refresh-only plan includes %s change for %s", rc.Action, rc.Addr)
			} else {
				log.Printf("[DEBUG] Refresh-only plan includes %s change for %s deposed object %s", rc.Action, rc.Addr, depKey)
			}
		}
		diags = diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Invalid refresh-only plan",
			"Terraform generated planned resource changes in a refresh-only plan. This is a bug in Terraform.",
		))
	}

	// Prune out any placeholder objects we put in the state to represent
	// objects that would need to be created.
	plan.PriorState.SyncWrapper().RemovePlannedResourceInstanceObjects()

	return plan, diags
}

func (c *Context) destroyPlan(config *configs.Config, prevRunState *states.State, rootVariables InputValues, opts *PlanOpts) (*plans.Plan, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics
	pendingPlan := &plans.Plan{}

	// A destroy plan starts by running Refresh to read any pending data
	// sources, and remove missing managed resources. This is required because
	// a "destroy plan" is only creating delete changes, and is essentially a
	// local operation.
	//
	// NOTE: if skipRefresh _is_ set then we'll rely on the destroy-plan walk
	// below to upgrade the prevRunState and priorState both to the latest
	// resource type schemas, so NodePlanDestroyableResourceInstance.Execute
	// must coordinate with this by taking that action only when c.skipRefresh
	// _is_ set. This coupling between the two is unfortunate but necessary
	// to work within our current structure.
	if !opts.SkipRefresh {
		refreshPlan, refreshDiags := c.plan(config, prevRunState, rootVariables, opts)
		diags = diags.Append(refreshDiags)
		if diags.HasErrors() {
			return nil, diags
		}

		// insert the refreshed state into the destroy plan result, and ignore
		// the changes recorded from the refresh.
		pendingPlan.PriorState = refreshPlan.PriorState.DeepCopy()
		pendingPlan.PrevRunState = refreshPlan.PrevRunState.DeepCopy()
	}

	destroyPlan, walkDiags := c.planWalk(config, prevRunState, rootVariables, opts)
	if walkDiags.HasErrors() {
		return nil, diags
	}

	if !opts.SkipRefresh {
		// If we didn't skip refreshing then we want the previous run and
		// prior states to be the ones that resulted from us refreshing
		// earlier.
		destroyPlan.PrevRunState = pendingPlan.PrevRunState
		destroyPlan.PriorState = pendingPlan.PriorState
	}

	return destroyPlan, diags
}

func (c *Context) prePlanFindAndApplyMoves(config *configs.Config, prevRunState *states.State, targets []addrs.Targetable) ([]refactoring.MoveStatement, map[addrs.UniqueKey]refactoring.MoveResult) {
	moveStmts := refactoring.FindMoveStatements(config)
	moveResults := refactoring.ApplyMoves(moveStmts, prevRunState)
	if len(targets) > 0 {
		for _, result := range moveResults {
			matchesTarget := false
			for _, targetAddr := range targets {
				if targetAddr.TargetContains(result.From) {
					matchesTarget = true
					break
				}
			}
			if !matchesTarget {
				// TODO: Return an error stating that a targeted plan is
				// only valid if it includes this address that was moved.
			}
		}
	}
	return moveStmts, moveResults
}

func (c *Context) postPlanValidateMoves(config *configs.Config, stmts []refactoring.MoveStatement, allInsts instances.Set) tfdiags.Diagnostics {
	return refactoring.ValidateMoves(stmts, config, allInsts)
}

// planWalkOpts captures some extra temporary data we need in order to run a
// graph walk for planning.
type planWalkOpts struct {
	// PlanOpts are the options originally provided by the caller, specifying
	// what sort of plan we're aiming to create.
	PlanOpts *PlanOpts

	// Config is the configuration we're going to plan with.
	Config *configs.Config

	// PrevRunState is the state that resulted from the previous run, or nil
	// if there was no previous run.
	PrevRunState *states.State

	// MoveResults is a table of the results of applying move statements prior
	// to a plan walk. Irrelevant and totally ignored for non-plan walks.
	MoveResults map[addrs.UniqueKey]refactoring.MoveResult
}

func (c *Context) planWalk(config *configs.Config, prevRunState *states.State, rootVariables InputValues, opts *PlanOpts) (*plans.Plan, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics
	log.Printf("[DEBUG] Building and walking plan graph for %s", opts.Mode)

	schemas, moreDiags := c.Schemas(config, prevRunState)
	diags = diags.Append(moreDiags)
	if diags.HasErrors() {
		return nil, diags
	}

	moveStmts, moveResults := c.prePlanFindAndApplyMoves(config, prevRunState, opts.Targets)

	var graph *Graph
	switch mode := opts.Mode; mode {
	case plans.NormalMode:
		graph, moreDiags = (&PlanGraphBuilder{
			Config:       config,
			State:        prevRunState,
			Components:   c.components,
			Schemas:      schemas,
			Targets:      opts.Targets,
			ForceReplace: opts.ForceReplace,
			Validate:     true,
			skipRefresh:  opts.SkipRefresh,
		}).Build(addrs.RootModuleInstance)
	case plans.RefreshOnlyMode:
		graph, moreDiags = (&PlanGraphBuilder{
			Config:          config,
			State:           prevRunState,
			Components:      c.components,
			Schemas:         schemas,
			Targets:         opts.Targets,
			Validate:        true,
			skipRefresh:     opts.SkipRefresh,
			skipPlanChanges: true, // this activates "refresh only" mode.
		}).Build(addrs.RootModuleInstance)
	case plans.DestroyMode:
		graph, moreDiags = (&DestroyPlanGraphBuilder{
			Config:      config,
			State:       prevRunState,
			Components:  c.components,
			Schemas:     schemas,
			Targets:     opts.Targets,
			Validate:    true,
			skipRefresh: opts.SkipRefresh,
		}).Build(addrs.RootModuleInstance)
	default:
		// The above should cover all plans.Mode values
		panic(fmt.Sprintf("unsupported plan mode %s", mode))
	}
	diags = diags.Append(moreDiags)
	if diags.HasErrors() {
		return nil, diags
	}

	// If we get here then we should definitely have a non-nil "graph", which
	// we can now walk.
	changes := plans.NewChanges()
	walker, walkDiags := c.walk(graph, walkPlan, &graphWalkOpts{
		InputState:         prevRunState,
		Changes:            changes,
		MoveResults:        moveResults,
		RootVariableValues: rootVariables,
	})
	diags = diags.Append(walker.NonFatalDiagnostics)
	diags = diags.Append(walkDiags)
	diags = diags.Append(c.postPlanValidateMoves(config, moveStmts, walker.InstanceExpander.AllInstances()))

	plan := &plans.Plan{
		UIMode:       opts.Mode,
		Changes:      changes,
		PriorState:   walker.RefreshState.Close(),
		PrevRunState: walker.PrevRunState.Close(),

		// Other fields get populated by Context.Plan after we return
	}
	return plan, diags
}
