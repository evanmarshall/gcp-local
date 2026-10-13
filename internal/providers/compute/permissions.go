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
	methodSubnetworksGetIam:     "compute.subnetworks.getIamPolicy",
	methodSubnetworksSetIam:     "compute.subnetworks.setIamPolicy",
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
	methodRoutersCreate:         "compute.routers.create",
	methodRoutersGet:            "compute.routers.get",
	methodRoutersList:           "compute.routers.list",
	methodRoutersDelete:         "compute.routers.delete",
	methodNetworksAddPeering:    "compute.networks.addPeering",
	methodNetworksRemovePeering: "compute.networks.removePeering",
	methodConnectivityCreate:    "networkmanagement.connectivitytests.create",
	methodConnectivityList:      "networkmanagement.connectivitytests.list",
	methodConnectivityRerun:     "networkmanagement.connectivitytests.rerun",
	methodConnectivityGet:       "networkmanagement.connectivitytests.get",
	methodConnectivityDelete:    "networkmanagement.connectivitytests.delete",
	methodOperationsGet:         "compute.globalOperations.get",
	methodOperationsList:        "compute.globalOperations.list",
	methodRegionOperationsGet:   "compute.regionOperations.get",
	methodRegionOperationsList:  "compute.regionOperations.list",
	methodZoneOperationsList:    "compute.zoneOperations.list",
	// Waiting reads the operation; deleting needs its own permission.
	methodOperationsWait:         "compute.globalOperations.get",
	methodRegionOperationsWait:   "compute.regionOperations.get",
	methodZoneOperationsWait:     "compute.zoneOperations.get",
	methodOperationsDelete:       "compute.globalOperations.delete",
	methodRegionOperationsDelete: "compute.regionOperations.delete",
	methodZoneOperationsDelete:   "compute.zoneOperations.delete",
	methodInstancesInsert:        "compute.instances.create",
	methodInstancesGet:           "compute.instances.get",
	methodInstancesList:          "compute.instances.list",
	methodInstancesDelete:        "compute.instances.delete",
	methodZoneOperationsGet:      "compute.zoneOperations.get",
	methodInstancesActAs:         "iam.serviceAccounts.actAs",
}

// extraPermissions are the documented additional checks of a method: creating
// an instance also creates its boot disk and attaches it to a subnetwork. Each
// permission is in the pinned roles/compute.admin role.
var extraPermissions = map[string][]string{
	methodInstancesInsert: {"compute.disks.create", "compute.subnetworks.use"},
}

// Method name constants; the generated route table uses the same identifiers.
const (
	methodNetworksCreate         = "compute.networks.create"
	methodNetworksGet            = "compute.networks.get"
	methodNetworksList           = "compute.networks.list"
	methodNetworksDelete         = "compute.networks.delete"
	methodSubnetworksCreate      = "compute.subnetworks.create"
	methodSubnetworksGet         = "compute.subnetworks.get"
	methodSubnetworksGetIam      = "compute.subnetworks.getIamPolicy"
	methodSubnetworksSetIam      = "compute.subnetworks.setIamPolicy"
	methodSubnetworksList        = "compute.subnetworks.list"
	methodSubnetworksDelete      = "compute.subnetworks.delete"
	methodFirewallsCreate        = "compute.firewalls.create"
	methodFirewallsGet           = "compute.firewalls.get"
	methodFirewallsList          = "compute.firewalls.list"
	methodFirewallsDelete        = "compute.firewalls.delete"
	methodFirewallsUpdate        = "compute.firewalls.update"
	methodRoutesCreate           = "compute.routes.create"
	methodRoutesGet              = "compute.routes.get"
	methodRoutesList             = "compute.routes.list"
	methodRoutesDelete           = "compute.routes.delete"
	methodRoutersCreate          = "compute.routers.create"
	methodRoutersGet             = "compute.routers.get"
	methodRoutersList            = "compute.routers.list"
	methodRoutersDelete          = "compute.routers.delete"
	methodNetworksAddPeering     = "compute.networks.addPeering"
	methodNetworksRemovePeering  = "compute.networks.removePeering"
	methodConnectivityCreate     = "networkmanagement.connectivityTests.create"
	methodConnectivityList       = "networkmanagement.connectivityTests.list"
	methodConnectivityRerun      = "networkmanagement.connectivityTests.rerun"
	methodConnectivityGet        = "networkmanagement.connectivityTests.get"
	methodConnectivityDelete     = "networkmanagement.connectivityTests.delete"
	methodOperationsGet          = "compute.globalOperations.get"
	methodOperationsList         = "compute.globalOperations.list"
	methodRegionOperationsGet    = "compute.regionOperations.get"
	methodInstancesInsert        = "compute.instances.insert"
	methodInstancesGet           = "compute.instances.get"
	methodInstancesList          = "compute.instances.list"
	methodInstancesDelete        = "compute.instances.delete"
	methodZoneOperationsGet      = "compute.zoneOperations.get"
	methodRegionOperationsList   = "compute.regionOperations.list"
	methodZoneOperationsList     = "compute.zoneOperations.list"
	methodOperationsWait         = "compute.globalOperations.wait"
	methodRegionOperationsWait   = "compute.regionOperations.wait"
	methodZoneOperationsWait     = "compute.zoneOperations.wait"
	methodOperationsDelete       = "compute.globalOperations.delete"
	methodRegionOperationsDelete = "compute.regionOperations.delete"
	methodZoneOperationsDelete   = "compute.zoneOperations.delete"
)

// methodNames lists every registered method.
func methodNames() []string {
	return []string{
		methodNetworksCreate, methodNetworksGet, methodNetworksList, methodNetworksDelete,
		methodNetworksAddPeering, methodNetworksRemovePeering,
		methodSubnetworksCreate, methodSubnetworksGet, methodSubnetworksList, methodSubnetworksDelete,
		methodSubnetworksGetIam, methodSubnetworksSetIam,
		methodFirewallsCreate, methodFirewallsGet, methodFirewallsList, methodFirewallsDelete,
		methodFirewallsUpdate,
		methodRoutesCreate, methodRoutesGet, methodRoutesList, methodRoutesDelete,
		methodRoutersCreate, methodRoutersGet, methodRoutersList, methodRoutersDelete,
		methodConnectivityCreate, methodConnectivityRerun,
		methodConnectivityList, methodConnectivityGet, methodConnectivityDelete,
		methodOperationsGet, methodOperationsList, methodRegionOperationsGet,
		methodInstancesInsert, methodInstancesGet, methodInstancesList, methodInstancesDelete,
		methodZoneOperationsGet, methodRegionOperationsList, methodZoneOperationsList,
		methodOperationsWait, methodRegionOperationsWait, methodZoneOperationsWait,
		methodOperationsDelete, methodRegionOperationsDelete, methodZoneOperationsDelete,
	}
}

// registerPermissions binds every method to its permission.
func registerPermissions(engine *iam.Engine) error {
	for method, permission := range permissions {
		if err := engine.Register(method, iam.Rule{Baseline: append([]string{permission}, extraPermissions[method]...)}); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}
