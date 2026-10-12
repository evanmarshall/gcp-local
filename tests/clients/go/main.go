// Command client-matrix drives one service workflow with the pinned Go Cloud
// client libraries against gcp-local, at each library's default transport.
// scripts/run/clients.py starts the emulator and runs this once per service:
//
//	client-matrix SERVICE
//
// It exits 0 when the workflow passes and prints the failing step otherwise.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"github.com/googleapis/gax-go/v2/apierror"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcinsecure "google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const cloudPlatform = "https://www.googleapis.com/auth/cloud-platform"

var workflows = map[string]func(context.Context, *env) error{
	"bigquery":       runBigQuery,
	"bigtable":       runBigtable,
	"cloudkms":       runKMS,
	"cloudscheduler": runScheduler,
	"cloudtasks":     runTasks,
	"firestore":      runFirestore,
	"iam":            runIAM,
	"iamcredentials": runIAMCredentials,
	"logging":        runLogging,
	"pubsub":         runPubSub,
	"run":            runCloudRun,
	"secretmanager":  runSecretManager,
	"spanner":        runSpanner,
	"storage":        runStorage,
}

func main() {
	if len(os.Args) != 2 || workflows[os.Args[1]] == nil {
		names := make([]string, 0, len(workflows))
		for name := range workflows {
			names = append(names, name)
		}
		sort.Strings(names)
		fmt.Fprintln(os.Stderr, "usage: client-matrix SERVICE; services:", names)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	e, err := loadEnv()
	if err == nil {
		err = workflows[os.Args[1]](ctx, e)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// env is what scripts/run/clients.py hands every runner: the project, the emulator's
// HTTP and gRPC addresses, and the granted and ungranted identities.
type env struct {
	project, http, grpcAddr string
	owner, denied           *auth.Credentials
}

func loadEnv() (*env, error) {
	e := &env{project: os.Getenv("GOOGLE_CLOUD_PROJECT"), http: os.Getenv("GCP_LOCAL_HTTP"), grpcAddr: os.Getenv("GCP_LOCAL_GRPC")}
	if e.project == "" || e.http == "" || e.grpcAddr == "" {
		return nil, errors.New("set GOOGLE_CLOUD_PROJECT, GCP_LOCAL_HTTP, and GCP_LOCAL_GRPC")
	}
	var err error
	// The granted identity is plain ADC (GOOGLE_APPLICATION_CREDENTIALS). REST
	// clients load it themselves with their default scopes. gRPC clients take
	// a prebuilt connection (grpcOption), which bypasses their credential
	// loading, so these credentials pick cloud-platform.
	if e.owner, err = credentials.DetectDefault(&credentials.DetectOptions{Scopes: []string{cloudPlatform}}); err != nil {
		return nil, fmt.Errorf("load ADC: %w", err)
	}
	if e.denied, err = credentials.DetectDefault(&credentials.DetectOptions{Scopes: []string{cloudPlatform}, CredentialsFile: os.Getenv("GCP_LOCAL_DENIED_ADC")}); err != nil {
		return nil, fmt.Errorf("load the ungranted ADC: %w", err)
	}
	return e, nil
}

// deniedFile loads the ungranted identity the way plain ADC loads: the client
// applies its own default scopes.
func (e *env) deniedFile() option.ClientOption {
	return option.WithAuthCredentialsFile(option.ExternalAccount, os.Getenv("GCP_LOCAL_DENIED_ADC"))
}

// bearer attaches a local ADC token to each gRPC call. The gateway is
// plaintext loopback, so unlike the libraries' own credentials it must not
// require transport security.
type bearer struct{ creds *auth.Credentials }

func (b bearer) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	token, err := b.creds.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("mint a local token: %w", err)
	}
	return map[string]string{"authorization": "Bearer " + token.Value}, nil
}

func (bearer) RequireTransportSecurity() bool { return false }

// grpcOption is the one setting every gRPC client needs: a plaintext loopback
// connection that still carries ADC. The library's endpoint option cannot do
// this, because its credentials refuse to travel without TLS.
func (e *env) grpcOption(creds *auth.Credentials) (option.ClientOption, error) {
	conn, err := grpc.NewClient("passthrough:///"+e.grpcAddr,
		grpc.WithTransportCredentials(grpcinsecure.NewCredentials()),
		grpc.WithPerRPCCredentials(bearer{creds}))
	if err != nil {
		return nil, fmt.Errorf("dial the gRPC gateway: %w", err)
	}
	return option.WithGRPCConn(conn), nil
}

// expectDenied requires err to be the ungranted identity's permission denial.
func expectDenied(step string, err error) error {
	if err == nil {
		return fmt.Errorf("%s: the ungranted identity was allowed", step)
	}
	var api *apierror.APIError
	if errors.As(err, &api) && (api.HTTPCode() == 403 || (api.GRPCStatus() != nil && api.GRPCStatus().Code() == codes.PermissionDenied)) {
		return nil
	}
	var gerr *googleapi.Error
	if errors.As(err, &gerr) && gerr.Code == 403 {
		return nil
	}
	if status.Code(err) == codes.PermissionDenied {
		return nil
	}
	return fmt.Errorf("%s: want permission denied, got: %w", step, err)
}

// step wraps err with the workflow step that produced it.
func step(name string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", name, err)
}
