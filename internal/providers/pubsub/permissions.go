package pubsub

const publisher = "/google.pubsub.v1.Publisher/"
const subscriber = "/google.pubsub.v1.Subscriber/"
const attach = "pubsub.attachSubscription"
const policyRPC = "/google.iam.v1.IAMPolicy/"

// IAM policy permissions shared by the gRPC policy surface and the REST twins.
const (
	permTopicsGetIam        = "pubsub.topics.getIamPolicy"
	permTopicsSetIam        = "pubsub.topics.setIamPolicy"
	permSubscriptionsGetIam = "pubsub.subscriptions.getIamPolicy"
	permSubscriptionsSetIam = "pubsub.subscriptions.setIamPolicy"
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
	attach:                            "pubsub.topics.attachSubscription",
	permTopicsGetIam:                  permTopicsGetIam,
	permTopicsSetIam:                  permTopicsSetIam,
	permSubscriptionsGetIam:           permSubscriptionsGetIam,
	permSubscriptionsSetIam:           permSubscriptionsSetIam,
	"pubsub.push.actAs":               "iam.serviceAccounts.actAs",
	policyRPC + "GetIamPolicy":        permTopicsGetIam,
	policyRPC + "SetIamPolicy":        permTopicsSetIam,
}

// permissionSubscriptionsConsume is the single permission that governs every
// subscription message operation.
const permissionSubscriptionsConsume = "pubsub.subscriptions.consume"
