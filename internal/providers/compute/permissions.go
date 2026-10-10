package compute

import (
	"fmt"

	"github.com/evanmarshall/gcp-local/internal/iam"
)

// permissions follows Google's documented compute access-control rows for the
// implemented resources. These are the simulator's plan, not fidelity claims.
var permissions = map[string]string{
	methodNetworksCreate:        "compute.networks.create",
	methodNetworksGet:           "compute.networks.get",
	methodNetworksList:          "compute.networks.list",
	methodNetworksDelete:        "compute.networks.delete",
	methodSubnetworksCreate:     "compute.subnetworks.create",
	methodSubnetworksGet:        "compute.subnetworks.get",
	methodSubnetworksList:       "compute.subnetworks.list",
	methodSubnetworksDelete:     "compute.subnetworks.delete",
	methodFirewallsCreate:       "compute.firewalls.create",
	methodFirewallsGet:          "compute.firewalls.get",
	methodFirewallsList:         "compute.firewalls.list",
	methodFirewallsDelete:       "compute.firewalls.delete",
	methodFirewallsUpdate:       "compute.firewalls.update",
	methodRoutesCreate:          "compute.routes.create",
	methodRoutesGet:             "compute.routes.get",
	methodRoutesList:            "compute.routes.list",
	methodRoutesDelete:          "compute.routes.delete",
	methodNetworksAddPeering:    "compute.networks.addPeering",
	methodNetworksRemovePeering: "compute.networks.removePeering",
	methodOperationsGet:         "compute.globalOperations.get",
	methodOperationsList:        "compute.globalOperations.list",
	methodRegionOperationsGet:   "compute.regionOperations.get",
}

// Method name constants; the generated route table uses the same identifiers.
const (
	methodNetworksCreate        = "compute.networks.create"
	methodNetworksGet           = "compute.networks.get"
	methodNetworksList          = "compute.networks.list"
	methodNetworksDelete        = "compute.networks.delete"
	methodSubnetworksCreate     = "compute.subnetworks.create"
	methodSubnetworksGet        = "compute.subnetworks.get"
	methodSubnetworksList       = "compute.subnetworks.list"
	methodSubnetworksDelete     = "compute.subnetworks.delete"
	methodFirewallsCreate       = "compute.firewalls.create"
	methodFirewallsGet          = "compute.firewalls.get"
	methodFirewallsList         = "compute.firewalls.list"
	methodFirewallsDelete       = "compute.firewalls.delete"
	methodFirewallsUpdate       = "compute.firewalls.update"
	methodRoutesCreate          = "compute.routes.create"
	methodRoutesGet             = "compute.routes.get"
	methodRoutesList            = "compute.routes.list"
	methodRoutesDelete          = "compute.routes.delete"
	methodNetworksAddPeering    = "compute.networks.addPeering"
	methodNetworksRemovePeering = "compute.networks.removePeering"
	methodOperationsGet         = "compute.globalOperations.get"
	methodOperationsList        = "compute.globalOperations.list"
	methodRegionOperationsGet   = "compute.regionOperations.get"
)

// methodNames lists every registered method.
func methodNames() []string {
	return []string{
		methodNetworksCreate, methodNetworksGet, methodNetworksList, methodNetworksDelete,
		methodNetworksAddPeering, methodNetworksRemovePeering,
		methodSubnetworksCreate, methodSubnetworksGet, methodSubnetworksList, methodSubnetworksDelete,
		methodFirewallsCreate, methodFirewallsGet, methodFirewallsList, methodFirewallsDelete,
		methodFirewallsUpdate,
		methodRoutesCreate, methodRoutesGet, methodRoutesList, methodRoutesDelete,
		methodOperationsGet, methodOperationsList, methodRegionOperationsGet,
	}
}

// registerPermissions binds every method to its permission.
func registerPermissions(engine *iam.Engine) error {
	for method, permission := range permissions {
		if err := engine.Register(method, iam.Rule{Baseline: []string{permission}}); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}
