package terraform

import (
	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/configs"
	"github.com/hashicorp/terraform/internal/lang"
	"github.com/hashicorp/terraform/internal/states"
	"github.com/hashicorp/terraform/internal/tfdiags"
)

// Eval produces a scope in which expressions can be evaluated for
// the given module path.
//
// This method must first evaluate any ephemeral values (input variables, local
// values, and output values) in the configuration. These ephemeral values are
// not included in the persisted state, so they must be re-computed using other
// values in the state before they can be properly evaluated. The updated
// values are retained in the main state associated with the receiving context.
//
// This function takes no action against remote APIs but it does need access
// to all provider and provisioner instances in order to obtain their schemas
// for type checking.
//
// The result is an evaluation scope that can be used to resolve references
// against the root module. If the returned diagnostics contains errors then
// the returned scope may be nil. If it is not nil then it may still be used
// to attempt expression evaluation or other analysis, but some expressions
// may not behave as expected.
func (c *Context) Eval(config *configs.Config, state *states.State, moduleAddr addrs.ModuleInstance) (*lang.Scope, tfdiags.Diagnostics) {
	// This is intended for external callers such as the "terraform console"
	// command. Internally, we create an evaluator in c.walk before walking
	// the graph, and create scopes in ContextGraphWalker.

	var diags tfdiags.Diagnostics
	defer c.acquireRun("eval")()

	schemas, moreDiags := c.Schemas(config, nil)
	diags = diags.Append(moreDiags)
	if moreDiags.HasErrors() {
		return nil, diags
	}

	// Start with a copy of state so that we don't affect the instance that
	// the caller is holding.
	state = state.DeepCopy()
	var walker *ContextGraphWalker

	graph, moreDiags := (&EvalGraphBuilder{
		Config:     config,
		State:      state,
		Components: c.components,
		Schemas:    schemas,
	}).Build(addrs.RootModuleInstance)
	diags = diags.Append(moreDiags)
	if moreDiags.HasErrors() {
		return nil, diags
	}

	walkOpts := &graphWalkOpts{
		InputState: state,
	}

	walker, moreDiags = c.walk(graph, walkEval, walkOpts)
	diags = diags.Append(walker.NonFatalDiagnostics)
	diags = diags.Append(moreDiags)

	if walker == nil {
		// If we skipped walking the graph (due to errors) then we'll just
		// use a placeholder graph walker here, which'll refer to the
		// unmodified state.
		walker = c.graphWalker(walkEval, walkOpts)
	}

	// This is a bit weird since we don't normally evaluate outside of
	// the context of a walk, but we'll "re-enter" our desired path here
	// just to get hold of an EvalContext for it. ContextGraphWalker
	// caches its contexts, so we should get hold of the context that was
	// previously used for evaluation here, unless we skipped walking.
	evalCtx := walker.EnterPath(moduleAddr)
	return evalCtx.EvaluationScope(nil, EvalDataForNoInstanceKey), diags
}
