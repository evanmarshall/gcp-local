package terraform_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

// localBigQueryCase drives one BigQuery case through the adapter plumbing.
func localBigQueryCase(t *testing.T, test resource.TestCase) {
	t.Helper()
	for i, step := range test.Steps {
		if step.Config == "" {
			continue
		}
		step.ConfigPlanChecks.PostApplyPostRefresh = append(step.ConfigPlanChecks.PostApplyPostRefresh, plancheck.ExpectEmptyPlan())
		step.Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 big_query_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "2s"
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/bigquery/v2/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// bqName mints one owned resource name part.
func bqName() string {
	return "gcp_local_tf_" + fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
}

// TestLocalBigQueryDatasetAndTable creates a dataset, a table, and destroys both.
func TestLocalBigQueryDatasetAndTable(t *testing.T) {
	name := bqName()

	localBigQueryCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testAccBigQueryDestroy(t, name),
		Steps: []resource.TestStep{
			{
				Config: testBigQueryConfig(name),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_bigquery_dataset.dataset", "dataset_id", name),
					resource.TestCheckResourceAttr("google_bigquery_table.table", "table_id", "events"),
				),
			},
		},
	})
}

// testBigQueryConfig renders one dataset plus a table with a schema.
func testBigQueryConfig(name string) string {
	return fmt.Sprintf(`
resource "google_bigquery_dataset" "dataset" {
 dataset_id    = "%s"
 friendly_name = "acceptance"
 delete_contents_on_destroy = true
}

resource "google_bigquery_table" "table" {
 dataset_id = google_bigquery_dataset.dataset.dataset_id
 table_id   = "events"
 deletion_protection = false
 schema = <<EOS
[
 {"name": "title", "type": "STRING"}
]
EOS
}

`, name)
}

// TestLocalBigQueryPartitionsAndAccess creates time- and range-partitioned,
// clustered tables under a dataset default partition expiration, a dataset
// with inline access entries, and google_bigquery_dataset_access entries for
// a user and an authorized dataset; then removes the user entry.
func TestLocalBigQueryPartitionsAndAccess(t *testing.T) {
	name := bqName()
	reader := "allowed@" + projectID + ".iam.gserviceaccount.com"
	localBigQueryCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testAccBigQueryDestroy(t, name+"_src"),
		Steps: []resource.TestStep{
			{
				Config: testBigQueryLayoutConfig(name, true),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_bigquery_table.events", "time_partitioning.0.type", "DAY"),
					resource.TestCheckResourceAttr("google_bigquery_table.events", "time_partitioning.0.field", "ts"),
					resource.TestCheckResourceAttr("google_bigquery_table.events", "time_partitioning.0.expiration_ms", "172800000"),
					resource.TestCheckResourceAttr("google_bigquery_table.events", "require_partition_filter", "true"),
					resource.TestCheckResourceAttr("google_bigquery_table.events", "clustering.#", "2"),
					resource.TestCheckResourceAttr("google_bigquery_table.ranged", "range_partitioning.0.range.0.interval", "10"),
					resource.TestCheckResourceAttr("google_bigquery_dataset.views", "access.#", "2"),
					testAccBigQueryAccess(name+"_src", []string{`"userByEmail":"` + reader + `"`, `"datasetId":"` + name + `_views"`}, nil),
				),
			},
			{
				Config: testBigQueryLayoutConfig(name, false),
				Check:  testAccBigQueryAccess(name+"_src", []string{`"targetTypes":["VIEWS"]`}, []string{reader}),
			},
		},
	})
}

// testBigQueryLayoutConfig renders the partitioned tables, the two datasets,
// and the access entries; withReader keeps the user entry.
func testBigQueryLayoutConfig(name string, withReader bool) string {
	reader := ""
	if withReader {
		reader = fmt.Sprintf(`
resource "google_bigquery_dataset_access" "reader" {
 dataset_id    = google_bigquery_dataset.source.dataset_id
 role          = "READER"
 user_by_email = "allowed@%s.iam.gserviceaccount.com"
}
`, projectID)
	}
	return fmt.Sprintf(`
resource "google_bigquery_dataset" "source" {
 dataset_id                      = "%[1]s_src"
 delete_contents_on_destroy      = true
 default_partition_expiration_ms = 172800000
}

resource "google_bigquery_dataset" "views" {
 dataset_id                  = "%[1]s_views"
 delete_contents_on_destroy  = true
 default_table_expiration_ms = 3600000
 access {
  role          = "OWNER"
  user_by_email = "fixture@%[2]s.iam.gserviceaccount.com"
 }
 access {
  role           = "READER"
  group_by_email = "gcp-local-allowed@stitchwink.com"
 }
}

resource "google_bigquery_table" "events" {
 dataset_id               = google_bigquery_dataset.source.dataset_id
 table_id                 = "events"
 deletion_protection      = false
 require_partition_filter = true
 clustering               = ["name", "n"]
 time_partitioning {
  type  = "DAY"
  field = "ts"
 }
 schema = <<EOS
[
 {"name": "ts", "type": "TIMESTAMP"},
 {"name": "name", "type": "STRING"},
 {"name": "n", "type": "INTEGER"}
]
EOS
}

resource "google_bigquery_table" "ranged" {
 dataset_id          = google_bigquery_dataset.source.dataset_id
 table_id            = "ranged"
 deletion_protection = false
 range_partitioning {
  field = "n"
  range {
   start    = 0
   end      = 100
   interval = 10
  }
 }
 schema = <<EOS
[
 {"name": "n", "type": "INTEGER"}
]
EOS
}

resource "google_bigquery_dataset_access" "authorized" {
 dataset_id = google_bigquery_dataset.source.dataset_id
 dataset {
  dataset {
   project_id = "%[2]s"
   dataset_id = google_bigquery_dataset.views.dataset_id
  }
  target_types = ["VIEWS"]
 }
}
%[3]s`, name, projectID, reader)
}

// testAccBigQueryAccess requires a dataset's access list to name each of
// present and none of absent.
func testAccBigQueryAccess(dataset string, present, absent []string) resource.TestCheckFunc {
	return func(_ *terraform.State) error {
		body, err := bqBody("/bigquery/v2/projects/" + projectID + "/datasets/" + dataset)
		if err != nil {
			return err
		}
		for _, text := range present {
			if !strings.Contains(body, text) {
				return fmt.Errorf("dataset %s access lacks %s: %s", dataset, text, body)
			}
		}
		for _, text := range absent {
			if strings.Contains(body, text) {
				return fmt.Errorf("dataset %s access still names %s: %s", dataset, text, body)
			}
		}
		return nil
	}
}

// bqBody reads one local BigQuery document as the fixture principal.
func bqBody(path string) (string, error) {
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, endpoint+path, nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("Authorization", "Bearer "+fixtureToken)
	response, err := httpClient.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<16))
	if err != nil || response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("read %s: status %d, %v", path, response.StatusCode, err)
	}
	return string(body), nil
}

// bqAPI reads one local BigQuery path as the fixture principal.
func bqAPI(path string) (int, error) {
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, endpoint+path, nil)
	if err != nil {
		return 0, err
	}
	request.Header.Set("Authorization", "Bearer "+fixtureToken)
	response, err := httpClient.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<10))
	return response.StatusCode, nil
}

// testAccBigQueryDestroy requires the dataset to be gone.
func testAccBigQueryDestroy(t *testing.T, name string) func(*terraform.State) error {
	return func(_ *terraform.State) error {
		code, err := bqAPI("/bigquery/v2/projects/" + projectID + "/datasets/" + name)
		if err != nil {
			return err
		}
		if code != 404 {
			t.Errorf("dataset read after destroy = %d", code)
		}
		return nil
	}
}

// localFirestoreCase drives one Firestore case through the adapter plumbing.
// The first document request starts the emulator's JVM, which takes over two
// seconds on hosted runners; a shorter timeout cancels a create the emulator
// still applies, and the provider's retry then fails with 409.
func localFirestoreCase(t *testing.T, test resource.TestCase) {
	t.Helper()
	for i, step := range test.Steps {
		if step.Config == "" {
			continue
		}
		step.ConfigPlanChecks.PostApplyPostRefresh = append(step.ConfigPlanChecks.PostApplyPostRefresh, plancheck.ExpectEmptyPlan())
		step.Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 firestore_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "30s"
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/v1/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// firestoreName mints one owned document name part.
func firestoreName() string {
	return "gcp-local-tf-" + fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
}

// TestLocalFirestoreDocument creates one document, reads it back, and destroys it.
func TestLocalFirestoreDocument(t *testing.T) {
	docID := firestoreName()

	localFirestoreCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testAccFirestoreDestroy(t, docID),
		Steps: []resource.TestStep{
			{
				Config: testFirestoreConfig(docID),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_firestore_document.document", "document_id", docID),
					resource.TestCheckResourceAttr("google_firestore_field.field", "field", "title"),
					resource.TestCheckResourceAttrSet("google_firestore_document.document", "create_time"),
				),
			},
		},
	})
}

// testFirestoreConfig renders one document in the fixture collection.
func testFirestoreConfig(docID string) string {
	return fmt.Sprintf(`
resource "google_firestore_document" "document" {
 project     = %q
 collection  = "gcp-local-collection"
 document_id = %q
 fields      = jsonencode({title = {stringValue = "fire-and-fog"}})
}

resource "google_firestore_field" "field" {
 project  = %q
 database = "(default)"
 collection = "gcp-local-collection"
 field    = "title"
 ttl_config {}
}
`, projectID, docID, projectID)
}

// firestoreAPI reads one local Firestore path as the fixture principal.
func firestoreAPI(path string) (int, error) {
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, endpoint+path, nil)
	if err != nil {
		return 0, err
	}
	request.Header.Set("Authorization", "Bearer "+fixtureToken)
	response, err := httpClient.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<10))
	return response.StatusCode, nil
}

// testAccFirestoreDestroy requires the document to be gone.
func testAccFirestoreDestroy(t *testing.T, docID string) func(*terraform.State) error {
	return func(_ *terraform.State) error {
		code, err := firestoreAPI("/v1/projects/" + projectID + "/databases/(default)/documents/gcp-local-collection/" + docID)
		if err != nil {
			return err
		}
		if code != 404 {
			t.Errorf("document read after destroy = %d", code)
		}
		return nil
	}
}

// localSpannerCase drives one Spanner case through the adapter plumbing.
func localSpannerCase(t *testing.T, test resource.TestCase) {
	t.Helper()
	for i, step := range test.Steps {
		if step.Config == "" {
			continue
		}
		step.ConfigPlanChecks.PostApplyPostRefresh = append(step.ConfigPlanChecks.PostApplyPostRefresh, plancheck.ExpectEmptyPlan())
		step.Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 spanner_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "2s"
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/v1/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// spannerName mints one owned resource name part.
func spannerName() string {
	return "gcp-local-tf-" + fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
}

// TestLocalSpannerInstanceAndDatabase creates an instance, a database, and destroys both.
func TestLocalSpannerInstanceAndDatabase(t *testing.T) {
	name := spannerName()

	localSpannerCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testAccSpannerDestroy(t, name),
		Steps: []resource.TestStep{
			{
				Config: testSpannerConfig(name),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_spanner_instance.instance", "name", name),
					resource.TestCheckResourceAttr("google_spanner_database.database", "name", "events"),
				),
			},
		},
	})
}

// testSpannerConfig renders one instance plus a database.
func testSpannerConfig(name string) string {
	return fmt.Sprintf(`
resource "google_spanner_instance" "instance" {
 name           = "%s"
 config         = "regional-us-central1"
 display_name   = "acceptance"
 processing_units = 1000
 force_destroy  = true
}

resource "google_spanner_database" "database" {
 instance     = google_spanner_instance.instance.name
 name         = "events"
 ddl          = ["CREATE TABLE entries (id INT64) PRIMARY KEY(id)"]
 deletion_protection = false
}
`, name)
}

// testAccSpannerDestroy verifies both resources are gone.
func testAccSpannerDestroy(t *testing.T, name string) func(*terraform.State) error {
	t.Helper()
	return func(state *terraform.State) error {
		client := &http.Client{Timeout: 5 * time.Second}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet,
			fmt.Sprintf("%s/v1/projects/%s/instances/%s", endpoint, projectID, name), nil)
		if err != nil {
			return fmt.Errorf("build the instance probe: %w", err)
		}
		request.Header.Set("Authorization", "Bearer "+fixtureToken)
		response, err := client.Do(request)
		if err != nil {
			return fmt.Errorf("probe the instance: %w", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusNotFound {
			return fmt.Errorf("instance %s still exists (status %d)", name, response.StatusCode)
		}
		return nil
	}
}

// localSQLCase drives one Cloud SQL case through the adapter plumbing. An
// insert initializes and starts a real PostgreSQL server before it answers,
// so requests get a longer timeout than the other cases.
func localSQLCase(t *testing.T, test resource.TestCase) {
	t.Helper()
	for i, step := range test.Steps {
		if step.Config == "" {
			continue
		}
		step.ConfigPlanChecks.PostApplyPostRefresh = append(step.ConfigPlanChecks.PostApplyPostRefresh, plancheck.ExpectEmptyPlan())
		step.Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 sql_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "60s"
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/sql/v1beta4/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// TestLocalSQLDatabaseInstance creates a PostgreSQL instance with backups,
// TLS, an authorized network, and a flag, plus a database and a user;
// updates the TLS mode, networks, and backups in place; and destroys them.
func TestLocalSQLDatabaseInstance(t *testing.T) {
	name := spannerName()
	localSQLCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testSQLDestroy(name),
		Steps: []resource.TestStep{
			{
				Config: testSQLConfig(name, "ENCRYPTED_ONLY", 3, ""),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_sql_database_instance.db", "settings.0.backup_configuration.0.enabled", "true"),
					resource.TestCheckResourceAttr("google_sql_database_instance.db", "settings.0.backup_configuration.0.point_in_time_recovery_enabled", "true"),
					resource.TestCheckResourceAttr("google_sql_database_instance.db", "settings.0.ip_configuration.0.ssl_mode", "ENCRYPTED_ONLY"),
					resource.TestCheckResourceAttr("google_sql_database_instance.db", "settings.0.ip_configuration.0.authorized_networks.#", "1"),
					resource.TestCheckResourceAttr("google_sql_database_instance.db", "settings.0.database_flags.0.value", "60"),
					resource.TestCheckResourceAttrSet("google_sql_database_instance.db", "server_ca_cert.0.cert"),
					resource.TestCheckResourceAttr("google_sql_database.app", "name", "app"),
					resource.TestCheckResourceAttr("google_sql_user.app", "name", "app"),
				),
			},
			{
				Config: testSQLConfig(name, "TRUSTED_CLIENT_CERTIFICATE_REQUIRED", 5, `
  authorized_networks {
   name  = "docs"
   value = "203.0.113.0/24"
  }`),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_sql_database_instance.db", "settings.0.ip_configuration.0.ssl_mode", "TRUSTED_CLIENT_CERTIFICATE_REQUIRED"),
					resource.TestCheckResourceAttr("google_sql_database_instance.db", "settings.0.ip_configuration.0.authorized_networks.#", "2"),
					resource.TestCheckResourceAttr("google_sql_database_instance.db", "settings.0.backup_configuration.0.transaction_log_retention_days", "5"),
				),
			},
		},
	})
}

// testSQLConfig renders the instance, a database, and a user.
func testSQLConfig(name, sslMode string, logDays int, networks string) string {
	return fmt.Sprintf(`
resource "google_sql_database_instance" "db" {
 name                = "%s"
 database_version    = "POSTGRES_16"
 region              = "us-central1"
 deletion_protection = false
 settings {
  tier              = "db-f1-micro"
  edition           = "ENTERPRISE"
  availability_type = "ZONAL"
  backup_configuration {
   enabled                        = true
   start_time                     = "03:00"
   point_in_time_recovery_enabled = true
   transaction_log_retention_days = %d
   backup_retention_settings {
    retained_backups = 5
   }
  }
  ip_configuration {
   ipv4_enabled = true
   ssl_mode     = "%s"
   authorized_networks {
    name  = "loopback"
    value = "127.0.0.1/32"
   }%s
  }
  database_flags {
   name  = "max_connections"
   value = "60"
  }
 }
}

resource "google_sql_database" "app" {
 name     = "app"
 instance = google_sql_database_instance.db.name
}

resource "google_sql_user" "app" {
 name     = "app"
 instance = google_sql_database_instance.db.name
 password = "app-secret"
}
`, name, logDays, sslMode, networks)
}

// testSQLDestroy requires the instance to be gone.
func testSQLDestroy(name string) func(*terraform.State) error {
	return func(_ *terraform.State) error {
		code, err := bqAPI("/sql/v1beta4/projects/" + projectID + "/instances/" + name)
		if err != nil {
			return err
		}
		if code != http.StatusNotFound {
			return fmt.Errorf("instance %s still exists (status %d)", name, code)
		}
		return nil
	}
}
