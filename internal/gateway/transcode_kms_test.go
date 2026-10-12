package gateway

import (
	"context"
	"net/url"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	kmsRing    = "projects/p/locations/l/keyRings/r"
	kmsKey     = kmsRing + "/cryptoKeys/k"
	kmsVersion = kmsKey + "/cryptoKeyVersions/1"
)

// wantGap requires an UNIMPLEMENTED status with one message.
func wantGap(t *testing.T, step string, err error, message string) {
	t.Helper()
	if s := status.Convert(err); s.Code() != codes.Unimplemented || s.Message() != message {
		t.Fatalf("%s: got %v, want UNIMPLEMENTED %q", step, err, message)
	}
}

func kmsDeadline(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestTranscodedKMSCreate pins the body and query of a KMS create.
func TestTranscodedKMSCreate(t *testing.T) {
	ctx := kmsDeadline(t)
	g, created := capturingGateway(t, "cloudkms.cryptoKeys.create", map[string]any{"name": kmsKey, "purpose": "MAC"})
	key, err := kmspb.NewKeyManagementServiceClient(grpcClient(t, g)).CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent: kmsRing, CryptoKeyId: "k", SkipInitialVersionCreation: true, CryptoKey: &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_MAC},
	})
	if err != nil || key.GetPurpose() != kmspb.CryptoKey_MAC {
		t.Fatalf("create = %v, %v", key, err)
	}
	got := <-created
	if got.call.Resource != "/v1/"+kmsRing+"/cryptoKeys" || string(got.request.JSON) != `{"purpose":"MAC"}` ||
		!reflect.DeepEqual(got.request.Query, url.Values{"cryptoKeyId": {"k"}, "skipInitialVersionCreation": {"true"}}) {
		t.Fatalf("create reached the provider as %+v %+v", got.call, got.request)
	}
}

// TestTranscodedKMSUpdateMask pins field mask paths as the camelCase
// updateMask query.
func TestTranscodedKMSUpdateMask(t *testing.T) {
	ctx := kmsDeadline(t)
	g, patched := capturingGateway(t, "cloudkms.cryptoKeys.update", map[string]any{"name": kmsKey})
	if _, err := kmspb.NewKeyManagementServiceClient(grpcClient(t, g)).UpdateCryptoKey(ctx, &kmspb.UpdateCryptoKeyRequest{
		CryptoKey:  &kmspb.CryptoKey{Name: kmsKey, Labels: map[string]string{"a": "b"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels", "version_template.algorithm"}},
	}); err != nil {
		t.Fatal(err)
	}
	if got := <-patched; got.request.Path["cryptoKey"] != "k" || got.request.Query.Get("updateMask") != "labels,versionTemplate.algorithm" {
		t.Fatalf("update reached the provider as %+v", got.request)
	}
}

// TestTranscodedKMSPolicyRead sends a key ring IAM policy read through the
// shared mixin. KMS binds GetIamPolicy to GET, so the options become query
// parameters.
func TestTranscodedKMSPolicyRead(t *testing.T) {
	ctx := kmsDeadline(t)
	g, policy := capturingGateway(t, "cloudkms.keyRings.getIamPolicy", map[string]any{"etag": "MA=="})
	request := &iampb.GetIamPolicyRequest{Resource: kmsRing, Options: &iampb.GetPolicyOptions{RequestedPolicyVersion: 3}}
	if _, err := iampb.NewIAMPolicyClient(grpcClient(t, g)).GetIamPolicy(ctx, request); err != nil {
		t.Fatal(err)
	}
	if got := <-policy; got.request.Path["keyRing"] != "r" || got.request.Query.Get("options.requestedPolicyVersion") != "3" || got.request.JSON != nil {
		t.Fatalf("policy reached the provider as %+v", got.request)
	}
}

// TestIAMPolicyRoutesByResourceOwner sends KMS resources to KMS and every
// other resource to Pub/Sub, and refuses unpaired IAM methods on KMS
// resources as a KMS gap.
func TestIAMPolicyRoutesByResourceOwner(t *testing.T) {
	ctx := kmsDeadline(t)
	r := NewRegistry()
	iamClient := iampb.NewIAMPolicyClient(grpcClient(t, New(r, nil)))
	_, err := iamClient.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: kmsKey})
	wantGap(t, "disabled KMS", err, "Service cloudkms is not enabled on this server; add kms to -services")
	_, err = iamClient.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: kmsKey})
	wantGap(t, "test permissions", err,
		"No provider implements /google.iam.v1.IAMPolicy/TestIamPermissions for projects/*/locations/*/keyRings/*/cryptoKeys/* resources")
	_, err = iamClient.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: kmsRing})
	wantGap(t, "ring test permissions", err,
		"No provider implements /google.iam.v1.IAMPolicy/TestIamPermissions for projects/*/locations/*/keyRings/* resources")

	topics := make(chan Call, 1)
	register(t, r, "/google.iam.v1.IAMPolicy/GetIamPolicy", func(call Call, input any) (any, error) {
		if _, ok := input.(*iampb.GetIamPolicyRequest); !ok {
			return nil, errBadDecodedRequest
		}
		topics <- call
		return &iampb.Policy{}, nil
	})
	if _, err := iamClient.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: "projects/p/topics/t"}); err != nil {
		t.Fatal(err)
	}
	if call := <-topics; call.Resource != "projects/p/topics/t" || call.Method != "/google.iam.v1.IAMPolicy/GetIamPolicy" {
		t.Fatalf("topic policy reached %+v", call)
	}
}

// TestKMSUnpairedAndWiderBindingsAreGaps covers an RPC with no REST route and
// a binding wider than its route.
func TestKMSUnpairedAndWiderBindingsAreGaps(t *testing.T) {
	ctx := kmsDeadline(t)
	g, _ := capturingGateway(t, "cloudkms.cryptoKeys.encrypt", map[string]any{})
	client := kmspb.NewKeyManagementServiceClient(grpcClient(t, g))
	_, err := client.RawEncrypt(ctx, &kmspb.RawEncryptRequest{Name: kmsVersion})
	wantGap(t, "raw encrypt", err, "No provider implements /google.cloud.kms.v1.KeyManagementService/RawEncrypt")
	// Encrypt binds cryptoKeys/**; the REST route takes a key only.
	_, err = client.Encrypt(ctx, &kmspb.EncryptRequest{Name: kmsVersion})
	wantGap(t, "encrypt with a version", err,
		"No provider implements /google.cloud.kms.v1.KeyManagementService/Encrypt for "+kmsVersion)
}
