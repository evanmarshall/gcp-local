package main

import (
	"bytes"
	"context"
	"fmt"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	admin "cloud.google.com/go/iam/admin/apiv1"
	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// runSecretManager creates a secret, adds and accesses a version, lists, and deletes.
func runSecretManager(ctx context.Context, e *env) error {
	conn, err := e.grpcOption(e.owner)
	if err != nil {
		return err
	}
	client, err := secretmanager.NewClient(ctx, conn)
	if err != nil {
		return step("new client", err)
	}
	defer client.Close()
	parent := "projects/" + e.project
	secret, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{Parent: parent, SecretId: "go-matrix", Secret: &secretmanagerpb.Secret{
		Replication: &secretmanagerpb.Replication{Replication: &secretmanagerpb.Replication_Automatic_{Automatic: &secretmanagerpb.Replication_Automatic{}}},
	}})
	if err != nil {
		return step("create secret", err)
	}
	if _, err := client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{Parent: secret.GetName(), Payload: &secretmanagerpb.SecretPayload{Data: []byte("hello from go")}}); err != nil {
		return step("add version", err)
	}
	latest := secret.GetName() + "/versions/latest"
	access, err := client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: latest})
	if err != nil || !bytes.Equal(access.GetPayload().GetData(), []byte("hello from go")) {
		return fmt.Errorf("access version: got %v, %v", access, err)
	}
	if err := listed(client.ListSecrets(ctx, &secretmanagerpb.ListSecretsRequest{Parent: parent}).Next, secret.GetName(), (*secretmanagerpb.Secret).GetName); err != nil {
		return step("list secrets", err)
	}
	deniedConn, err := e.grpcOption(e.denied)
	if err != nil {
		return err
	}
	denied, err := secretmanager.NewClient(ctx, deniedConn)
	if err != nil {
		return step("new ungranted client", err)
	}
	defer denied.Close()
	_, err = denied.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: latest})
	if err := expectDenied("ungranted access", err); err != nil {
		return err
	}
	return step("delete secret", client.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{Name: secret.GetName()}))
}

// runKMS creates a key ring and key, encrypts and decrypts, lists, and destroys the version.
func runKMS(ctx context.Context, e *env) error {
	conn, err := e.grpcOption(e.owner)
	if err != nil {
		return err
	}
	client, err := kms.NewKeyManagementClient(ctx, conn)
	if err != nil {
		return step("new client", err)
	}
	defer client.Close()
	ring, err := client.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{Parent: "projects/" + e.project + "/locations/us-central1", KeyRingId: "go-matrix", KeyRing: &kmspb.KeyRing{}})
	if err != nil {
		return step("create key ring", err)
	}
	key, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: ring.GetName(), CryptoKeyId: "key", CryptoKey: &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT}})
	if err != nil {
		return step("create crypto key", err)
	}
	encrypted, err := client.Encrypt(ctx, &kmspb.EncryptRequest{Name: key.GetName(), Plaintext: []byte("hello from go")})
	if err != nil {
		return step("encrypt", err)
	}
	decrypted, err := client.Decrypt(ctx, &kmspb.DecryptRequest{Name: key.GetName(), Ciphertext: encrypted.GetCiphertext()})
	if err != nil || !bytes.Equal(decrypted.GetPlaintext(), []byte("hello from go")) {
		return fmt.Errorf("decrypt: got %v, %v", decrypted, err)
	}
	if err := listed(client.ListCryptoKeys(ctx, &kmspb.ListCryptoKeysRequest{Parent: ring.GetName()}).Next, key.GetName(), (*kmspb.CryptoKey).GetName); err != nil {
		return step("list crypto keys", err)
	}
	deniedConn, err := e.grpcOption(e.denied)
	if err != nil {
		return err
	}
	denied, err := kms.NewKeyManagementClient(ctx, deniedConn)
	if err != nil {
		return step("new ungranted client", err)
	}
	defer denied.Close()
	_, err = denied.Encrypt(ctx, &kmspb.EncryptRequest{Name: key.GetName(), Plaintext: []byte("must not encrypt")})
	if err := expectDenied("ungranted encrypt", err); err != nil {
		return err
	}
	_, err = client.DestroyCryptoKeyVersion(ctx, &kmspb.DestroyCryptoKeyVersionRequest{Name: key.GetName() + "/cryptoKeyVersions/1"})
	return step("destroy key version", err)
}

// runTasks creates a queue and a scheduled HTTP task, reads and lists it, and deletes both.
func runTasks(ctx context.Context, e *env) error {
	conn, err := e.grpcOption(e.owner)
	if err != nil {
		return err
	}
	client, err := cloudtasks.NewClient(ctx, conn)
	if err != nil {
		return step("new client", err)
	}
	defer client.Close()
	location := "projects/" + e.project + "/locations/us-central1"
	queue, err := client.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{Parent: location, Queue: &cloudtaskspb.Queue{Name: location + "/queues/go-matrix"}})
	if err != nil {
		return step("create queue", err)
	}
	// Scheduled an hour out, so the task stays queued while it is read.
	task := &cloudtaskspb.Task{
		ScheduleTime: timestamppb.New(time.Now().Add(time.Hour)),
		MessageType: &cloudtaskspb.Task_HttpRequest{HttpRequest: &cloudtaskspb.HttpRequest{
			Url: "http://127.0.0.1:9/task", HttpMethod: cloudtaskspb.HttpMethod_POST, Body: []byte("hello from go"),
		}},
	}
	created, err := client.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{Parent: queue.GetName(), Task: task})
	if err != nil {
		return step("create task", err)
	}
	if _, err := client.GetTask(ctx, &cloudtaskspb.GetTaskRequest{Name: created.GetName()}); err != nil {
		return step("get task", err)
	}
	if err := listed(client.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{Parent: queue.GetName()}).Next, created.GetName(), (*cloudtaskspb.Task).GetName); err != nil {
		return step("list tasks", err)
	}
	deniedConn, err := e.grpcOption(e.denied)
	if err != nil {
		return err
	}
	denied, err := cloudtasks.NewClient(ctx, deniedConn)
	if err != nil {
		return step("new ungranted client", err)
	}
	defer denied.Close()
	_, err = denied.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{Parent: queue.GetName(), Task: task})
	if err := expectDenied("ungranted create task", err); err != nil {
		return err
	}
	if err := client.DeleteTask(ctx, &cloudtaskspb.DeleteTaskRequest{Name: created.GetName()}); err != nil {
		return step("delete task", err)
	}
	return step("delete queue", client.DeleteQueue(ctx, &cloudtaskspb.DeleteQueueRequest{Name: queue.GetName()}))
}

// runIAM creates, gets, lists, and deletes a service account.
func runIAM(ctx context.Context, e *env) error {
	conn, err := e.grpcOption(e.owner)
	if err != nil {
		return err
	}
	client, err := admin.NewIamClient(ctx, conn)
	if err != nil {
		return step("new client", err)
	}
	defer client.Close()
	project := "projects/" + e.project
	account, err := client.CreateServiceAccount(ctx, &adminpb.CreateServiceAccountRequest{Name: project, AccountId: "go-matrix", ServiceAccount: &adminpb.ServiceAccount{DisplayName: "Go matrix"}})
	if err != nil {
		return step("create service account", err)
	}
	name := project + "/serviceAccounts/" + account.GetEmail()
	if _, err := client.GetServiceAccount(ctx, &adminpb.GetServiceAccountRequest{Name: name}); err != nil {
		return step("get service account", err)
	}
	if err := listed(client.ListServiceAccounts(ctx, &adminpb.ListServiceAccountsRequest{Name: project}).Next, account.GetEmail(), (*adminpb.ServiceAccount).GetEmail); err != nil {
		return step("list service accounts", err)
	}
	deniedConn, err := e.grpcOption(e.denied)
	if err != nil {
		return err
	}
	denied, err := admin.NewIamClient(ctx, deniedConn)
	if err != nil {
		return step("new ungranted client", err)
	}
	defer denied.Close()
	_, err = denied.GetServiceAccount(ctx, &adminpb.GetServiceAccountRequest{Name: name})
	if err := expectDenied("ungranted get", err); err != nil {
		return err
	}
	return step("delete service account", client.DeleteServiceAccount(ctx, &adminpb.DeleteServiceAccountRequest{Name: name}))
}
