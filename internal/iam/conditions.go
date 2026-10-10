package iam

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/cel-go/cel"
)

// conditionContext is the CEL evaluation context of one authorization check.
// Only the fields the local emulator can resolve are exposed; everything else
// stays unset and any condition referencing it fails closed.
type conditionContext struct {
	ResourceType string
	ResourceName string
	Service      string
	Now          time.Time
}

// conditionResult is the verdict of one condition evaluation.
type conditionResult struct {
	Allowed bool
	Reason  string
}

// evaluated runs the CEL program against the context.
type evaluated struct {
	program cel.Program
	title   string
}

// errConditionExpression rejects a condition without an expression.
var errConditionExpression = errors.New("a binding condition requires an expression")

// compileCondition parses and checks one condition expression. An
// uncompilable condition fails closed at policy write time.
func compileCondition(raw json.RawMessage) (*evaluated, error) {
	var shape struct {
		Expression string `json:"expression"`
		Title      string `json:"title"`
	}
	if json.Unmarshal(raw, &shape) != nil || strings.TrimSpace(shape.Expression) == "" {
		return nil, errConditionExpression
	}
	env, err := cel.NewEnv(
		cel.Variable("resource", cel.MapType(cel.StringType, cel.StringType)),
		cel.Variable("request", cel.MapType(cel.StringType, cel.StringType)),
	)
	if err != nil {
		return nil, fmt.Errorf("build the condition environment: %w", err)
	}
	parsed, issues := env.Parse(shape.Expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("parse the condition: %w", issues.Err())
	}
	checked, issues := env.Check(parsed)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("check the condition: %w", issues.Err())
	}
	program, err := env.Program(checked)
	if err != nil {
		return nil, fmt.Errorf("compile the condition: %w", err)
	}
	return &evaluated{program: program, title: shape.Title}, nil
}

// evaluate evaluates one compiled condition; any error fails closed.
func (e *evaluated) evaluate(ctx conditionContext) conditionResult {
	values := map[string]any{
		"resource": map[string]string{
			"type":    ctx.ResourceType,
			"name":    ctx.ResourceName,
			"service": ctx.Service,
		},
		"request": map[string]string{
			"time": ctx.Now.Format(time.RFC3339),
		},
	}
	out, _, err := e.program.Eval(values)
	if err != nil {
		return conditionResult{Allowed: false, Reason: "condition " + e.title + " could not be evaluated"}
	}
	if truth, ok := out.Value().(bool); ok && truth {
		return conditionResult{Allowed: true, Reason: "condition " + e.title + " matched"}
	}
	return conditionResult{Allowed: false, Reason: "condition " + e.title + " did not match"}
}

// conditionGuard is the compiled form of a policy's conditions by binding.
type conditionGuard struct {
	bindingIndex int
	condition    *evaluated
}

// evaluateBinding evaluates the condition of one stored binding index.
func (s storedPolicy) evaluateBinding(index int) conditionResult {
	for _, guard := range s.conditions {
		if guard.bindingIndex != index {
			continue
		}
		return guard.condition.evaluate(conditionContext{Now: time.Now()})
	}
	return conditionResult{Allowed: false, Reason: "the binding condition was not compiled"}
}
