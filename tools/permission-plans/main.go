// Command permission-plans prints the permission plans a provider registers
// with the IAM engine, for offline drift reports. It starts no backend, opens
// no listener, and makes no network call.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	secretmanagerprovider "github.com/evanmarshall/gcp-local/internal/providers/security/secretmanager"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// plan is one method's exported rule.
type plan struct {
	Baseline []string            `json:"baseline"`
	When     map[string][]string `json:"when"`
}

// registrars construct only in-memory providers; each entry registers its
// plans without starting a backend.
var registrars = map[string]func(*gateway.Registry, *backend.Manager, *state.Index, *iam.Engine) error{
	"secretmanager": func(registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) error {
		_, err := secretmanagerprovider.New(context.Background(), registry, manager, index, engine)
		return err //nolint:wrapcheck // reported once with the service name below
	},
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run writes the selected service's plans as sorted JSON.
func run(args []string, output, errors io.Writer) int {
	flags := flag.NewFlagSet("permission-plans", flag.ContinueOnError)
	flags.SetOutput(errors)
	service := flags.String("service", "", "provider whose registered plans to print")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return 2
	}
	register, ok := registrars[*service]
	if !ok {
		fmt.Fprintln(errors, "Select a supported -service: secretmanager")
		return 2
	}
	index := state.NewIndex()
	engine, err := iam.New(index)
	if err == nil {
		err = register(gateway.NewRegistry(), backend.NewManager(time.Second, time.Second), index, engine)
	}
	if err != nil {
		fmt.Fprintln(errors, "register "+*service+": "+err.Error())
		return 1
	}
	plans := map[string]plan{}
	for method, rule := range engine.Plans(*service + ".") {
		sort.Strings(rule.Baseline)
		plans[method] = plan{Baseline: rule.Baseline, When: rule.When}
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(map[string]any{"schema": 1, "service": *service, "plans": plans}); err != nil {
		return 1
	}
	return 0
}
