//go:build backendprobe

package main

import (
	"context"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The provider forwards Avro schemas to the emulator and serves Protocol
// Buffer schemas itself (docs/services/messaging/pubsub.md). These are the emulator behaviors
// that split relies on.
const (
	probeSchema     = "projects/backend-probe/schemas/avro-probe"
	schemaTopicName = "projects/backend-probe/topics/avro-typed"
	schemaSubName   = "projects/backend-probe/subscriptions/avro-typed"
	avroV1          = `{"type":"record","name":"Event","fields":[{"name":"id","type":"string"},{"name":"count","type":"int"}]}`
	avroV2          = `{"type":"record","name":"Event","fields":[{"name":"id","type":"string"},{"name":"count","type":"int"},{"name":"note","type":["null","string"],"default":null}]}`
)

func probeSchemas(ctx context.Context, connection grpc.ClientConnInterface, publisher pb.PublisherClient, subscriber pb.SubscriberClient) map[string]any {
	schemas := pb.NewSchemaServiceClient(connection)
	_, err := schemas.CreateSchema(ctx, &pb.CreateSchemaRequest{Parent: "projects/backend-probe", SchemaId: "proto-probe", Schema: &pb.Schema{Type: pb.Schema_PROTOCOL_BUFFER, Definition: "syntax = \"proto3\";\nmessage Event { string id = 1; }\n"}})
	check(status.Code(err) == codes.Unimplemented, "the emulator now accepts Protocol Buffer schemas")
	first, err := schemas.CreateSchema(ctx, &pb.CreateSchemaRequest{Parent: "projects/backend-probe", SchemaId: "avro-probe", Schema: &pb.Schema{Type: pb.Schema_AVRO, Definition: avroV1}})
	must(err)
	check(len(first.GetRevisionId()) == 8, "Avro revision IDs changed shape")
	second, err := schemas.CommitSchema(ctx, &pb.CommitSchemaRequest{Name: probeSchema, Schema: &pb.Schema{Type: pb.Schema_AVRO, Definition: avroV2}})
	must(err)
	pinned, err := schemas.GetSchema(ctx, &pb.GetSchemaRequest{Name: probeSchema + "@" + first.GetRevisionId()})
	must(err)
	check(pinned.GetDefinition() == avroV1, "GetSchema ignored a revision name")
	listed, err := schemas.ListSchemaRevisions(ctx, &pb.ListSchemaRevisionsRequest{Name: probeSchema})
	must(err)
	// The provider strips definitions from lists unless the view is FULL.
	check(len(listed.GetSchemas()) == 2 && listed.GetSchemas()[0].GetRevisionId() == first.GetRevisionId() && listed.GetSchemas()[0].GetDefinition() != "", "revision listing changed order or view")
	_, err = schemas.DeleteSchemaRevision(ctx, &pb.DeleteSchemaRevisionRequest{Name: probeSchema + "@" + second.GetRevisionId()})
	must(err)
	_, err = schemas.DeleteSchemaRevision(ctx, &pb.DeleteSchemaRevisionRequest{Name: probeSchema + "@" + first.GetRevisionId()})
	check(status.Code(err) == codes.InvalidArgument, "the last revision became deletable")
	_, err = publisher.CreateTopic(ctx, &pb.Topic{Name: schemaTopicName, SchemaSettings: &pb.SchemaSettings{Schema: probeSchema, Encoding: pb.Encoding_JSON}})
	must(err)
	_, err = subscriber.CreateSubscription(ctx, &pb.Subscription{Name: schemaSubName, Topic: schemaTopicName, AckDeadlineSeconds: 10})
	must(err)
	_, err = publisher.Publish(ctx, &pb.PublishRequest{Topic: schemaTopicName, Messages: []*pb.PubsubMessage{{Data: []byte(`{"id":"a"}`)}}})
	check(status.Code(err) == codes.InvalidArgument, "a nonconforming Avro message was published")
	_, err = publisher.Publish(ctx, &pb.PublishRequest{Topic: schemaTopicName, Messages: []*pb.PubsubMessage{{Data: []byte(`{"id":"a","count":1}`)}}})
	must(err)
	pulled, err := subscriber.Pull(ctx, &pb.PullRequest{Subscription: schemaSubName, MaxMessages: 1})
	must(err)
	check(len(pulled.GetReceivedMessages()) == 1 && pulled.GetReceivedMessages()[0].GetMessage().GetAttributes()["googclient_schemarevisionid"] == first.GetRevisionId(), "schema attributes are missing")
	_, err = schemas.DeleteSchema(ctx, &pb.DeleteSchemaRequest{Name: probeSchema})
	must(err)
	topic, err := publisher.GetTopic(ctx, &pb.GetTopicRequest{Topic: schemaTopicName})
	must(err)
	check(topic.GetSchemaSettings().GetSchema() == "_deleted-schema_", "a deleted schema is no longer marked on its topic")
	_, err = publisher.Publish(ctx, &pb.PublishRequest{Topic: schemaTopicName, Messages: []*pb.PubsubMessage{{Data: []byte(`{"id":"a","count":1}`)}}})
	check(status.Code(err) == codes.NotFound, "publishing to a topic with a deleted schema succeeded")
	_, err = subscriber.DeleteSubscription(ctx, &pb.DeleteSubscriptionRequest{Subscription: schemaSubName})
	must(err)
	_, err = publisher.DeleteTopic(ctx, &pb.DeleteTopicRequest{Topic: schemaTopicName})
	must(err)
	return map[string]any{"protocol_buffer": "unimplemented", "avro_revisions": true, "avro_publish_validation": true, "deleted_schema_marker": true}
}
