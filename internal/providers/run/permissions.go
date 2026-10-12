package run

// permissions follows Google's documented Cloud Run access-control table. These
// are the simulator's permission plan, not recorded cloud-fidelity claims.
var permissions = map[string]string{
	methodServicesCreate:   "run.services.create",
	methodServicesGet:      "run.services.get",
	methodServicesList:     "run.services.list",
	methodServicesUpdate:   "run.services.update",
	methodServicesDelete:   "run.services.delete",
	methodServicesGetIam:   "run.services.getIamPolicy",
	methodServicesSetIam:   "run.services.setIamPolicy",
	methodRevisionsList:    "run.revisions.list",
	methodRevisionsGet:     "run.revisions.get",
	methodRevisionsDelete:  "run.revisions.delete",
	methodOperationsGet:    "run.operations.get",
	methodOperationsList:   "run.operations.list",
	methodOperationsDelete: "run.operations.delete",
	methodServicesInvoke:   "run.routes.invoke",
	methodServicesActAs:    "iam.serviceAccounts.actAs",
}

// Method name constants; the gateway route table uses the same identifiers.
const (
	methodServicesCreate   = "run.services.create"
	methodServicesGet      = "run.services.get"
	methodServicesList     = "run.services.list"
	methodServicesUpdate   = "run.services.update"
	methodServicesDelete   = "run.services.delete"
	methodServicesGetIam   = "run.services.getIamPolicy"
	methodServicesSetIam   = "run.services.setIamPolicy"
	methodRevisionsList    = "run.revisions.list"
	methodRevisionsGet     = "run.revisions.get"
	methodRevisionsDelete  = "run.revisions.delete"
	methodOperationsGet    = "run.operations.get"
	methodOperationsList   = "run.operations.list"
	methodOperationsDelete = "run.operations.delete"
	methodServicesInvoke   = "run.services.invoke"
)
