package terraform

import (
	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/configs"
	"github.com/hashicorp/terraform/internal/states"
	"github.com/hashicorp/terraform/internal/tfdiags"
)

// Validate performs semantic validation of a configuration, and returns
// any warnings or errors.
//
// Syntax and structural checks are performed by the configuration loader,
// and so are not repeated here.
func (c *Context) Validate(config *configs.Config) tfdiags.Diagnostics {
	defer c.acquireRun("validate")()

	var diags tfdiags.Diagnostics

	moreDiags := CheckCoreVersionRequirements(config)
	diags = diags.Append(moreDiags)
	// If version constraints are not met then we'll bail early since otherwise
	// we're likely to just see a bunch of other errors related to
	// incompatibilities, which could be overwhelming for the user.
	if diags.HasErrors() {
		return diags
	}

	schemas, moreDiags := c.Schemas(config, nil)
	diags = diags.Append(moreDiags)
	if moreDiags.HasErrors() {
		return diags
	}

	graph, moreDiags := ValidateGraphBuilder(&PlanGraphBuilder{
		Config:     config,
		Components: c.components,
		Schemas:    schemas,
		Validate:   true,
		State:      states.NewState(),
	}).Build(addrs.RootModuleInstance)
	diags = diags.Append(moreDiags)
	if moreDiags.HasErrors() {
		return diags
	}

	walker, walkDiags := c.walk(graph, walkValidate, &graphWalkOpts{})
	diags = diags.Append(walker.NonFatalDiagnostics)
	diags = diags.Append(walkDiags)
	if walkDiags.HasErrors() {
		return diags
	}

	return diags
}
