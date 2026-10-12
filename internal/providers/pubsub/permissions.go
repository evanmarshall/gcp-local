package pubsub

const publisher = "/google.pubsub.v1.Publisher/"
const subscriber = "/google.pubsub.v1.Subscriber/"
const schemaService = "/google.pubsub.v1.SchemaService/"

// schemaAttach is checked on the schema a created topic names; the Admin and
// Editor roles carry pubsub.schemas.attach (access-control table).
const schemaAttach = "pubsub.schemas.attachTarget"
const attach = "pubsub.attachSubscription"

// CreateSnapshot also needs consume on the source subscription, and Seek to a
// snapshot needs snapshots.seek on it (access-control table).
const (
	snapshotSource = "pubsub.snapshots.source"
	snapshotSeek   = "pubsub.snapshots.seekTarget"
)
const policyRPC = "/google.iam.v1.IAMPolicy/"

// IAM policy permissions shared by the gRPC policy surface and the REST twins.
const (
	permTopicsGetIam        = "pubsub.topics.getIamPolicy"
	permTopicsSetIam        = "pubsub.topics.setIamPolicy"
	permSubscriptionsGetIam = "pubsub.subscriptions.getIamPolicy"
	permSubscriptionsSetIam = "pubsub.subscriptions.setIamPolicy"
	permSchemasGetIam       = "pubsub.schemas.getIamPolicy"
	permSchemasSetIam       = "pubsub.schemas.setIamPolicy"
)

// Permission targets follow Google's documented access-control table. Live
// Pub/Sub permission probes remain unrecorded; these are not fidelity claims.
var permissions = map[string]string{
	publisher + "CreateTopic":         "pubsub.topics.create",
	publisher + "GetTopic":            "pubsub.topics.get",
	publisher + "DeleteTopic":         "pubsub.topics.delete",
	publisher + "ListTopics":          "pubsub.topics.list",
	publisher + "Publish":             "pubsub.topics.publish",
	subscriber + "CreateSubscription": "pubsub.subscriptions.create",
	subscriber + "GetSubscription":    "pubsub.subscriptions.get",
	subscriber + "DeleteSubscription": "pubsub.subscriptions.delete",
	subscriber + "ListSubscriptions":  "pubsub.subscriptions.list",
	subscriber + "Pull":               permissionSubscriptionsConsume,
	subscriber + "Acknowledge":        permissionSubscriptionsConsume,
	subscriber + "ModifyAckDeadline":  permissionSubscriptionsConsume,
	subscriber + "StreamingPull":      permissionSubscriptionsConsume,
	subscriber + "Seek":               permissionSubscriptionsConsume,
	subscriber + "CreateSnapshot":     "pubsub.snapshots.create",
	subscriber + "ListSnapshots":      "pubsub.snapshots.list",
	subscriber + "DeleteSnapshot":     "pubsub.snapshots.delete",
	// The access-control table has no GetSnapshot row; this follows the
	// snapshots.get permission that the Viewer, Editor, and Admin roles carry.
	subscriber + "GetSnapshot": "pubsub.snapshots.get",
	snapshotSource:             permissionSubscriptionsConsume,
	snapshotSeek:               "pubsub.snapshots.seek",
	deadLetterPublish:          "pubsub.topics.publish",
	deadLetterAcknowledge:      permissionSubscriptionsConsume,
	attach:                     "pubsub.topics.attachSubscription",
	permTopicsGetIam:           permTopicsGetIam,
	permTopicsSetIam:           permTopicsSetIam,
	permSubscriptionsGetIam:    permSubscriptionsGetIam,
	permSubscriptionsSetIam:    permSubscriptionsSetIam,
	"pubsub.push.actAs":        "iam.serviceAccounts.actAs",
	// Schema rows of the access-control table. DeleteSchemaRevision needs
	// pubsub.schemas.delete; ValidateMessage needs pubsub.schemas.validate on
	// the project even when it names a schema.
	schemaService + "CreateSchema":         "pubsub.schemas.create",
	schemaService + "GetSchema":            "pubsub.schemas.get",
	schemaService + "ListSchemas":          "pubsub.schemas.list",
	schemaService + "ListSchemaRevisions":  "pubsub.schemas.listRevisions",
	schemaService + "CommitSchema":         "pubsub.schemas.commit",
	schemaService + "RollbackSchema":       "pubsub.schemas.rollback",
	schemaService + "DeleteSchemaRevision": "pubsub.schemas.delete",
	schemaService + "DeleteSchema":         "pubsub.schemas.delete",
	schemaService + "ValidateSchema":       "pubsub.schemas.validate",
	schemaService + "ValidateMessage":      "pubsub.schemas.validate",
	schemaAttach:                           "pubsub.schemas.attach",
	permSchemasGetIam:                      permSchemasGetIam,
	permSchemasSetIam:                      permSchemasSetIam,
	policyRPC + "GetIamPolicy":             permTopicsGetIam,
	policyRPC + "SetIamPolicy":             permTopicsSetIam,
}

// permissionSubscriptionsConsume is the single permission that governs every
// subscription message operation.
const permissionSubscriptionsConsume = "pubsub.subscriptions.consume"
