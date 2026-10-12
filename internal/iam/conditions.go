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
// Only the fields the provider supplied are exposed; an empty field is absent,
// and any expression that reads it fails to evaluate.
type conditionContext struct {
	ResourceType string
	ResourceName string
	Service      string
	Now          time.Time
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
		cel.Variable("request", cel.MapType(cel.StringType, cel.DynType)),
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

// errConditionValue reports an expression that produced no boolean.
var errConditionValue = errors.New("the condition did not produce a boolean")

// evaluate runs one compiled condition. Only attributes the provider supplied
// are present, so an expression that reads a missing attribute returns an
// error instead of comparing against a default. Callers decide what an error
// means: it never grants an allow binding and always applies a deny rule.
func (e *evaluated) evaluate(ctx conditionContext) (bool, error) {
	resource := map[string]string{}
	for key, value := range map[string]string{"type": ctx.ResourceType, "name": ctx.ResourceName, "service": ctx.Service} {
		if value != "" {
			resource[key] = value
		}
	}
	// request.time is a timestamp, so comparisons with timestamp() work.
	out, _, err := e.program.Eval(map[string]any{"resource": resource, "request": map[string]any{"time": ctx.Now}})
	if err != nil {
		return false, fmt.Errorf("evaluate the condition: %w", err)
	}
	truth, ok := out.Value().(bool)
	if !ok {
		return false, errConditionValue
	}
	return truth, nil
}

// conditionGuard is the compiled form of a policy's conditions by binding.
type conditionGuard struct {
	bindingIndex int
	condition    *evaluated
}

// evaluateBinding evaluates the condition of one stored binding index. A
// condition that is missing, false, or cannot be evaluated grants nothing.
func (s storedPolicy) evaluateBinding(index int, attributes conditionContext) (bool, string) {
	for _, guard := range s.conditions {
		if guard.bindingIndex != index {
			continue
		}
		matched, err := guard.condition.evaluate(attributes)
		switch {
		case err != nil:
			return false, "condition " + guard.condition.title + " could not be evaluated"
		case matched:
			return true, "condition " + guard.condition.title + " matched"
		}
		return false, "condition " + guard.condition.title + " did not match"
	}
	return false, "the binding condition was not compiled"
}
