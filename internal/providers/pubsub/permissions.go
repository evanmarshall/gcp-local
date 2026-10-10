package pubsub

import "github.com/evanmarshall/gcp-local/internal/iam"

const publisher = "/google.pubsub.v1.Publisher/"
const subscriber = "/google.pubsub.v1.Subscriber/"
const attach = "pubsub.attachSubscription"
const policyRPC = "/google.iam.v1.IAMPolicy/"

// Permission targets follow Google's documented access-control table. Live
// Pub/Sub permission probes remain unrecorded; these are not fidelity claims.
var permissions = map[string]string{
	publisher + "CreateTopic":           "pubsub.topics.create",
	publisher + "GetTopic":              "pubsub.topics.get",
	publisher + "DeleteTopic":           "pubsub.topics.delete",
	publisher + "ListTopics":            "pubsub.topics.list",
	publisher + "Publish":               "pubsub.topics.publish",
	subscriber + "CreateSubscription":   "pubsub.subscriptions.create",
	subscriber + "GetSubscription":      "pubsub.subscriptions.get",
	subscriber + "DeleteSubscription":   "pubsub.subscriptions.delete",
	subscriber + "ListSubscriptions":    "pubsub.subscriptions.list",
	subscriber + "Pull":                 permissionSubscriptionsConsume,
	subscriber + "Acknowledge":          permissionSubscriptionsConsume,
	subscriber + "ModifyAckDeadline":    permissionSubscriptionsConsume,
	subscriber + "StreamingPull":        permissionSubscriptionsConsume,
	attach:                              "pubsub.topics.attachSubscription",
	"pubsub.topics.getIamPolicy":        "pubsub.topics.getIamPolicy",
	"pubsub.topics.setIamPolicy":        "pubsub.topics.setIamPolicy",
	"pubsub.subscriptions.getIamPolicy": "pubsub.subscriptions.getIamPolicy",
	"pubsub.subscriptions.setIamPolicy": "pubsub.subscriptions.setIamPolicy",
	"pubsub.push.actAs":                 "iam.serviceAccounts.actAs",
}

// permissionSubscriptionsConsume is the single permission that governs every
// subscription message operation.
const permissionSubscriptionsConsume = "pubsub.subscriptions.consume"

func registerPermissions(engine *iam.Engine) error {
	for method, permission := range permissions {
		if err := engine.Register(method, iam.Rule{Baseline: []string{permission}}); err != nil {
			return err
		}
	}
	return nil
}
