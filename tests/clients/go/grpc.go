package main

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/logging"
	"cloud.google.com/go/logging/logadmin"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/iterator"
)

// runPubSub creates a topic and subscription, publishes, receives, lists, and deletes.
func runPubSub(ctx context.Context, e *env) error {
	conn, err := e.grpcOption(e.owner)
	if err != nil {
		return err
	}
	client, err := pubsub.NewClient(ctx, e.project, conn)
	if err != nil {
		return step("new client", err)
	}
	defer client.Close()
	topic := "projects/" + e.project + "/topics/go-matrix"
	subscription := "projects/" + e.project + "/subscriptions/go-matrix"
	if _, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		return step("create topic", err)
	}
	if _, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{Name: subscription, Topic: topic}); err != nil {
		return step("create subscription", err)
	}
	publisher := client.Publisher(topic)
	if _, err := publisher.Publish(ctx, &pubsub.Message{Data: []byte("hello from go")}).Get(ctx); err != nil {
		return step("publish", err)
	}
	publisher.Stop()
	receive, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var got []byte
	err = client.Subscriber(subscription).Receive(receive, func(_ context.Context, message *pubsub.Message) {
		got = message.Data
		message.Ack()
		cancel()
	})
	if err != nil || !bytes.Equal(got, []byte("hello from go")) {
		return fmt.Errorf("receive: got %q, %v", got, err)
	}
	if err := listed(client.TopicAdminClient.ListTopics(ctx, &pubsubpb.ListTopicsRequest{Project: "projects/" + e.project}).Next, topic, (*pubsubpb.Topic).GetName); err != nil {
		return step("list topics", err)
	}
	deniedConn, err := e.grpcOption(e.denied)
	if err != nil {
		return err
	}
	denied, err := pubsub.NewClient(ctx, e.project, deniedConn)
	if err != nil {
		return step("new ungranted client", err)
	}
	defer denied.Close()
	deniedPublisher := denied.Publisher(topic)
	_, err = deniedPublisher.Publish(ctx, &pubsub.Message{Data: []byte("must not land")}).Get(ctx)
	deniedPublisher.Stop()
	if err := expectDenied("ungranted publish", err); err != nil {
		return err
	}
	if err := client.SubscriptionAdminClient.DeleteSubscription(ctx, &pubsubpb.DeleteSubscriptionRequest{Subscription: subscription}); err != nil {
		return step("delete subscription", err)
	}
	return step("delete topic", client.TopicAdminClient.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: topic}))
}

// runFirestore sets, gets, queries, and deletes a document.
func runFirestore(ctx context.Context, e *env) error {
	conn, err := e.grpcOption(e.owner)
	if err != nil {
		return err
	}
	client, err := firestore.NewClient(ctx, e.project, conn)
	if err != nil {
		return step("new client", err)
	}
	defer client.Close()
	cities := client.Collection("go-matrix-cities")
	doc := cities.Doc("sf")
	if _, err := doc.Set(ctx, map[string]any{"name": "San Francisco", "population": 870000}); err != nil {
		return step("set document", err)
	}
	snapshot, err := doc.Get(ctx)
	if err != nil || snapshot.Data()["name"] != "San Francisco" {
		return fmt.Errorf("get document: got %v, %v", snapshot, err)
	}
	matches, err := cities.Where("population", ">", 100000).Documents(ctx).GetAll()
	if err != nil || len(matches) != 1 || matches[0].Ref.ID != "sf" {
		return fmt.Errorf("query: got %v, %v", matches, err)
	}
	deniedConn, err := e.grpcOption(e.denied)
	if err != nil {
		return err
	}
	denied, err := firestore.NewClient(ctx, e.project, deniedConn)
	if err != nil {
		return step("new ungranted client", err)
	}
	defer denied.Close()
	_, err = denied.Collection("go-matrix-cities").Doc("sf").Get(ctx)
	if err := expectDenied("ungranted get", err); err != nil {
		return err
	}
	_, err = doc.Delete(ctx)
	return step("delete document", err)
}

// runLogging writes an entry, reads it back, lists logs, and deletes the log.
func runLogging(ctx context.Context, e *env) error {
	conn, err := e.grpcOption(e.owner)
	if err != nil {
		return err
	}
	client, err := logging.NewClient(ctx, e.project, conn)
	if err != nil {
		return step("new client", err)
	}
	defer client.Close()
	if err := client.Logger("go-matrix").LogSync(ctx, logging.Entry{Payload: "hello from go", Severity: logging.Info}); err != nil {
		return step("write entry", err)
	}
	admin, err := logadmin.NewClient(ctx, e.project, conn)
	if err != nil {
		return step("new admin client", err)
	}
	defer admin.Close()
	logName := "projects/" + e.project + "/logs/go-matrix"
	entry, err := admin.Entries(ctx, logadmin.Filter(`logName="`+logName+`"`)).Next()
	if err != nil || entry.Payload != "hello from go" {
		return fmt.Errorf("list entries: got %v, %v", entry, err)
	}
	if err := listed(admin.Logs(ctx).Next, "go-matrix", func(name string) string { return name }); err != nil {
		return step("list logs", err)
	}
	deniedConn, err := e.grpcOption(e.denied)
	if err != nil {
		return err
	}
	denied, err := logging.NewClient(ctx, e.project, deniedConn)
	if err != nil {
		return step("new ungranted client", err)
	}
	defer denied.Close()
	err = denied.Logger("go-matrix").LogSync(ctx, logging.Entry{Payload: "must not land"})
	if err := expectDenied("ungranted write", err); err != nil {
		return err
	}
	return step("delete log", admin.DeleteLog(ctx, "go-matrix"))
}

// listed pages through an iterator until it finds want.
func listed[T any](next func() (T, error), want string, name func(T) string) error {
	for {
		item, err := next()
		if err == iterator.Done {
			return fmt.Errorf("%s not listed", want)
		}
		if err != nil {
			return err
		}
		if name(item) == want {
			return nil
		}
	}
}
