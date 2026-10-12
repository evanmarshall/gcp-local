package cloudkms

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	core "github.com/evanmarshall/gcp-local/internal/runtime"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/local"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// grpcServer runs the KMS provider behind the real runtime with local
// external-account identities: the fixture holds roles/cloudkms.admin on the
// project; the allowed and denied identities hold nothing there.
type grpcServer struct {
	t                  *testing.T
	endpoint, grpcAddr string
	directory          string
}

const (
	grpcSubjectPrefix = "gls.synthetic-kms-grpc-subject-"
	grpcParent        = "projects/" + testProject + "/locations/us-central1"
)

func startGRPC(t *testing.T) *grpcServer {
	t.Helper()
	registry, index := gateway.NewRegistry(), state.NewIndex()
	project := provider.ProjectResource(testProject)
	if err := index.Put("resourcemanager", state.Resource{Name: project, Service: "resourcemanager", Kind: "project", Number: "123456", Scope: state.Scope{Project: testProject, Location: state.Global}}); err != nil {
		t.Fatal(err)
	}
	engine, err := iam.New(index)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.SetPolicy(project, iam.Policy{Bindings: []iam.Binding{{Role: "roles/cloudkms.admin", Members: []string{member(testFixture)}}}}); err != nil {
		t.Fatal(err)
	}
	actors := []string{testFixture, testAllowed, testDenied}
	enrollments := make([]identity.Enrollment, 0, len(actors))
	for _, actor := range actors {
		enrollments = append(enrollments, identity.Enrollment{Subject: member(actor), Project: testProject, SubjectToken: grpcSubjectPrefix + actor})
	}
	identities, err := identity.New(identity.Config{Principals: enrollments}, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := core.WithGateway("cloudkms-grpc", 5*time.Second, gateway.New(registry, identities.Resolve))
	server.Identity, server.IAM = identities, engine
	if _, err := New(context.Background(), registry, server.Backends, index, engine); err != nil {
		t.Fatal(err)
	}
	listen := func() net.Listener {
		var config net.ListenConfig
		l, err := config.Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	listeners := core.Listeners{HTTP: listen(), GRPC: listen()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listeners) }()
	t.Cleanup(func() { cancel(); <-done })
	return &grpcServer{t: t, endpoint: "http://" + listeners.HTTP.Addr().String(), grpcAddr: listeners.GRPC.Addr().String(), directory: t.TempDir()}
}

// client builds the Go KMS client on its default gRPC transport with one
// actor's local external-account ADC file.
func (s *grpcServer) client(actor string) *kms.KeyManagementClient {
	s.t.Helper()
	subject := filepath.Join(s.directory, actor+".txt")
	if err := os.WriteFile(subject, []byte(grpcSubjectPrefix+actor), 0o600); err != nil {
		s.t.Fatal(err)
	}
	raw, err := json.Marshal(map[string]any{"type": "external_account", "audience": identity.Audience, "subject_token_type": identity.AccessTokenType, "token_url": s.endpoint + "/v1/token", "credential_source": map[string]string{"file": subject}})
	if err != nil {
		s.t.Fatal(err)
	}
	adc := filepath.Join(s.directory, actor+"-adc.json")
	if err := os.WriteFile(adc, raw, 0o600); err != nil {
		s.t.Fatal(err)
	}
	client, err := kms.NewKeyManagementClient(context.Background(),
		option.WithEndpoint(s.grpcAddr),
		option.WithAuthCredentialsFile(option.ExternalAccount, adc),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(local.NewCredentials())))
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(func() { _ = client.Close() })
	return client
}

// refused requires one gRPC status code and message.
func refused(t *testing.T, step string, err error, code codes.Code, message string) {
	t.Helper()
	if s := status.Convert(err); s.Code() != code || s.Message() != message {
		t.Fatalf("%s: got %v, want %s %q", step, err, code, message)
	}
}

// grant adds roles for the allowed identity on a crypto key over gRPC.
func grant(t *testing.T, admin *kms.KeyManagementClient, key string, roles ...string) {
	t.Helper()
	ctx := context.Background()
	policy, err := admin.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: key})
	if err != nil {
		t.Fatalf("get policy: %v", err)
	}
	for _, role := range roles {
		policy.Bindings = append(policy.Bindings, &iampb.Binding{Role: role, Members: []string{member(testAllowed)}})
	}
	if _, err := admin.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: key, Policy: policy}); err != nil {
		t.Fatalf("set policy: %v", err)
	}
}

// createKey creates one crypto key over gRPC.
func createKey(t *testing.T, admin *kms.KeyManagementClient, ring, id string, purpose kmspb.CryptoKey_CryptoKeyPurpose, algorithm kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm) *kmspb.CryptoKey {
	t.Helper()
	key, err := admin.CreateCryptoKey(context.Background(), &kmspb.CreateCryptoKeyRequest{
		Parent: ring, CryptoKeyId: id,
		CryptoKey: &kmspb.CryptoKey{Purpose: purpose, VersionTemplate: &kmspb.CryptoKeyVersionTemplate{Algorithm: algorithm}},
	})
	if err != nil {
		t.Fatalf("create key %s: %v", id, err)
	}
	return key
}

// TestGoClientOverGRPC drives cloud.google.com/go/kms/apiv1 at its default
// gRPC transport: symmetric encrypt and decrypt, EC signing verified with the
// public key, HMAC, and denials for an ungranted identity.
func TestGoClientOverGRPC(t *testing.T) {
	server := startGRPC(t)
	admin, app, peer := server.client(testFixture), server.client(testAllowed), server.client(testDenied)
	_, err := admin.GetKeyRing(context.Background(), &kmspb.GetKeyRingRequest{Name: grpcParent + "/keyRings/go"})
	refused(t, "missing ring", err, codes.NotFound, "KeyRing "+grpcParent+"/keyRings/go not found.")
	ring, err := admin.CreateKeyRing(context.Background(), &kmspb.CreateKeyRingRequest{Parent: grpcParent, KeyRingId: "go"})
	if err != nil || ring.GetName() != grpcParent+"/keyRings/go" || ring.GetCreateTime() == nil {
		t.Fatalf("create ring: %v %v", ring, err)
	}
	if policy, err := admin.GetIamPolicy(context.Background(), &iampb.GetIamPolicyRequest{Resource: ring.GetName()}); err != nil || len(policy.GetEtag()) == 0 {
		t.Fatalf("ring policy: %v %v", policy, err)
	}
	goSymmetric(t, admin, app, peer, ring.GetName())
	goSign(t, admin, app, ring.GetName())
	goMAC(t, admin, app, peer, ring.GetName())
}

// goSymmetric encrypts and decrypts with AAD after a denial.
func goSymmetric(t *testing.T, admin, app, peer *kms.KeyManagementClient, ring string) {
	t.Helper()
	ctx := context.Background()
	key := createKey(t, admin, ring, "symmetric", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION)
	if key.GetPrimary().GetState() != kmspb.CryptoKeyVersion_ENABLED {
		t.Fatalf("symmetric primary: %v", key)
	}
	_, err := peer.Encrypt(ctx, &kmspb.EncryptRequest{Name: key.GetName(), Plaintext: []byte(testPlaintext)})
	refused(t, "ungranted encrypt", err, codes.PermissionDenied,
		"Missing permission: cloudkms.cryptoKeyVersions.useToEncrypt //cloudkms.googleapis.com/"+key.GetName())
	grant(t, admin, key.GetName(), testEncrypter)
	encrypted, err := app.Encrypt(ctx, &kmspb.EncryptRequest{Name: key.GetName(), Plaintext: []byte(testPlaintext), AdditionalAuthenticatedData: []byte(testAad)})
	if err != nil || encrypted.GetCiphertextCrc32C() == nil || encrypted.GetProtectionLevel() != kmspb.ProtectionLevel_SOFTWARE {
		t.Fatalf("encrypt: %v %v", encrypted, err)
	}
	decrypted, err := app.Decrypt(ctx, &kmspb.DecryptRequest{Name: key.GetName(), Ciphertext: encrypted.GetCiphertext(), AdditionalAuthenticatedData: []byte(testAad)})
	if err != nil || string(decrypted.GetPlaintext()) != testPlaintext || !decrypted.GetUsedPrimary() {
		t.Fatalf("decrypt: %v %v", decrypted, err)
	}
	_, err = app.Decrypt(ctx, &kmspb.DecryptRequest{Name: key.GetName(), Ciphertext: encrypted.GetCiphertext()})
	refused(t, "wrong AAD", err, codes.InvalidArgument, "Decryption failed: the ciphertext is invalid.")
}

// goSign signs a digest and verifies it with the version's public key.
func goSign(t *testing.T, admin, app *kms.KeyManagementClient, ring string) {
	t.Helper()
	ctx := context.Background()
	key := createKey(t, admin, ring, "sign", kmspb.CryptoKey_ASYMMETRIC_SIGN, kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256)
	grant(t, admin, key.GetName(), "roles/cloudkms.signerVerifier", "roles/cloudkms.cryptoKeyDecrypter")
	version := key.GetName() + "/cryptoKeyVersions/1"
	digest := sha256.Sum256([]byte(testPlaintext))
	signed, err := app.AsymmetricSign(ctx, &kmspb.AsymmetricSignRequest{Name: version, Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: digest[:]}}})
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	public, err := app.GetPublicKey(ctx, &kmspb.GetPublicKeyRequest{Name: version})
	if err != nil || public.GetAlgorithm() != kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256 {
		t.Fatalf("public key: %v %v", public, err)
	}
	block, _ := pem.Decode([]byte(public.GetPem()))
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatalf("parse public key: %v", err)
	}
	ec, ok := parsed.(*ecdsa.PublicKey)
	if !ok || !ecdsa.VerifyASN1(ec, digest[:], signed.GetSignature()) {
		t.Fatal("signature does not verify with the public key")
	}
	_, err = app.AsymmetricDecrypt(ctx, &kmspb.AsymmetricDecryptRequest{Name: version, Ciphertext: []byte("x")})
	refused(t, "wrong purpose", err, codes.FailedPrecondition, version+" has purpose ASYMMETRIC_SIGN, which does not support this operation.")
}

// goMAC signs and verifies an HMAC, disables the version, and refuses an
// ungranted caller.
func goMAC(t *testing.T, admin, app, peer *kms.KeyManagementClient, ring string) {
	t.Helper()
	ctx := context.Background()
	key := createKey(t, admin, ring, "mac", kmspb.CryptoKey_MAC, kmspb.CryptoKeyVersion_HMAC_SHA256)
	grant(t, admin, key.GetName(), "roles/cloudkms.signerVerifier")
	version := key.GetName() + "/cryptoKeyVersions/1"
	tag, err := app.MacSign(ctx, &kmspb.MacSignRequest{Name: version, Data: []byte(testPlaintext)})
	if err != nil {
		t.Fatalf("mac sign: %v", err)
	}
	verified, err := app.MacVerify(ctx, &kmspb.MacVerifyRequest{Name: version, Data: []byte(testPlaintext), Mac: tag.GetMac()})
	if err != nil || !verified.GetSuccess() {
		t.Fatalf("mac verify: %v %v", verified, err)
	}
	disabled, err := admin.UpdateCryptoKeyVersion(ctx, &kmspb.UpdateCryptoKeyVersionRequest{
		CryptoKeyVersion: &kmspb.CryptoKeyVersion{Name: version, State: kmspb.CryptoKeyVersion_DISABLED},
		UpdateMask:       &fieldmaskpb.FieldMask{Paths: []string{"state"}},
	})
	if err != nil || disabled.GetState() != kmspb.CryptoKeyVersion_DISABLED {
		t.Fatalf("disable: %v %v", disabled, err)
	}
	_, err = app.MacSign(ctx, &kmspb.MacSignRequest{Name: version, Data: []byte(testPlaintext)})
	refused(t, "disabled mac sign", err, codes.FailedPrecondition, version+" is not enabled, current state is: DISABLED.")
	_, err = peer.MacSign(ctx, &kmspb.MacSignRequest{Name: version, Data: []byte(testPlaintext)})
	refused(t, "ungranted mac sign", err, codes.PermissionDenied,
		"Missing permission: cloudkms.cryptoKeyVersions.useToSign //cloudkms.googleapis.com/"+key.GetName())
}

// TestGRPCUnservedMethodsAreGaps pins the explicit refusals of RPCs and
// resource shapes the REST provider does not serve.
func TestGRPCUnservedMethodsAreGaps(t *testing.T) {
	server := startGRPC(t)
	admin := server.client(testFixture)
	ctx := context.Background()
	ring, err := admin.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{Parent: grpcParent, KeyRingId: "gaps"})
	if err != nil {
		t.Fatal(err)
	}
	key := createKey(t, admin, ring.GetName(), "k", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION)
	version := key.GetName() + "/cryptoKeyVersions/1"
	_, err = admin.RawEncrypt(ctx, &kmspb.RawEncryptRequest{Name: version, Plaintext: []byte("x")})
	refused(t, "raw encrypt", err, codes.Unimplemented, "No provider implements /google.cloud.kms.v1.KeyManagementService/RawEncrypt")
	_, err = admin.ListImportJobs(ctx, &kmspb.ListImportJobsRequest{Parent: ring.GetName()}).Next()
	refused(t, "import jobs", err, codes.Unimplemented, "No provider implements /google.cloud.kms.v1.KeyManagementService/ListImportJobs")
	// Encrypt binds cryptoKeys/** so it may name a version; the REST route
	// takes a key only, so the version form is a gap on both transports.
	_, err = admin.Encrypt(ctx, &kmspb.EncryptRequest{Name: version, Plaintext: []byte("x")})
	refused(t, "encrypt with a version", err, codes.Unimplemented, "No provider implements /google.cloud.kms.v1.KeyManagementService/Encrypt for "+version)
	_, err = admin.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: key.GetName(), Permissions: []string{"cloudkms.cryptoKeys.get"}})
	refused(t, "test permissions", err, codes.Unimplemented,
		"No provider implements /google.iam.v1.IAMPolicy/TestIamPermissions for projects/*/locations/*/keyRings/*/cryptoKeys/* resources")
	// The format reaches the REST handler as the publicKeyFormat query.
	grant(t, admin, key.GetName(), "roles/cloudkms.publicKeyViewer")
	_, err = server.client(testAllowed).GetPublicKey(ctx, &kmspb.GetPublicKeyRequest{Name: version, PublicKeyFormat: kmspb.PublicKey_NIST_PQC})
	refused(t, "public key format", err, codes.Unimplemented, "Cloud KMS coverage gap: publicKeyFormat")
}
