package cloudsql

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/mysqlwire"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/pgwire"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
)

// Request shape checks for databases and users.
var (
	charsetPattern        = regexp.MustCompile(`^[A-Za-z0-9_]{1,32}$`)
	collationPattern      = regexp.MustCompile(`^[A-Za-z0-9_.@-]{1,64}$`)
	mysqlCollationPattern = regexp.MustCompile(`^[A-Za-z0-9_]{1,64}$`)
	mysqlHostPattern      = regexp.MustCompile(`^[A-Za-z0-9.%_:-]{1,255}$`)
	reservedNames         = map[string]bool{adminRole: true, superuserRole: true, "template0": true, "template1": true}
)

// objectName reports whether a database or role name is usable: PostgreSQL
// truncates identifiers beyond 63 bytes and no text value can carry NUL.
func objectName(name string) bool {
	return name != "" && len(name) <= 63 && !strings.ContainsRune(name, 0) && !reservedNames[name]
}

// mysqlName reports whether a MySQL schema (64 characters) or user (32
// characters) name is usable; MySQL itself rejects other invalid names.
func mysqlName(name string, limit int) bool {
	return name != "" && len(name) <= limit && !strings.ContainsRune(name, 0) && !reservedNames[name] && !strings.HasPrefix(name, "mysql.")
}

// databaseQuery selects the user-visible databases of a server.
const databaseQuery = "SELECT datname, pg_encoding_to_char(encoding), datcollate FROM pg_database WHERE NOT datistemplate AND datname <> " +
	"'" + adminRole + "'"

// mysqlDatabaseQuery selects every schema of a MySQL server, as Cloud SQL lists them.
const mysqlDatabaseQuery = "SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA"

// engineOf reports whether an existing instance runs MySQL.
func (p *Provider) engineOf(project, instance string) (bool, bool) {
	record, ok := p.lookup(project, instance)
	return isMySQL(record.Version), ok
}

// sqlOn runs administrative SQL, written for the engine the caller saw, on an
// existing instance's server. A missing instance, or one a concurrent delete
// and insert replaced with the other engine, renders notFound; a server error
// renders an API error.
func (p *Provider) sqlOn(ctx context.Context, project, instance string, mysql bool, sql string) ([][]string, *gateway.RESTResponse, error) {
	var rows [][]string
	missing := false
	err := p.use(ctx, project, func(ctx context.Context, fleet driver) error {
		// Check under the mutation gate: a concurrent delete may have won.
		if engine, ok := p.engineOf(project, instance); !ok || engine != mysql {
			missing = true
			return nil
		}
		var err error
		rows, err = fleet.exec(ctx, instance, "postgres", sql)
		return err
	})
	if err != nil {
		failure, err := backendFailure(err)
		return nil, failure, err
	}
	if missing {
		return nil, notFound(), nil
	}
	return rows, nil, nil
}

// databaseWire renders one sql#database row.
func databaseWire(project, instance string, row []string) map[string]any {
	return map[string]any{keyKind: "sql#database", keyName: row[0], "charset": row[1], "collation": row[2],
		pathInstance: instance, pathProject: project, "etag": databaseETag(instance, row[0]),
		keySelfLink: instanceLink(project, instance) + "/databases/" + row[0]}
}

// databaseETag is a stable tag for one database of one instance.
func databaseETag(instance, name string) string {
	sum := sha256.Sum256([]byte(instance + "/" + name))
	return hex.EncodeToString(sum[:8])
}

// databaseBody is the databases.insert subset the provider models.
type databaseBody struct {
	Name      string `json:"name"`
	Charset   string `json:"charset"`
	Collation string `json:"collation"`
}

// createDatabase renders the CREATE DATABASE statement for a validated body.
func (body databaseBody) createDatabase() (string, error) {
	if !objectName(body.Name) {
		return "", provider.Invalid("Invalid database name.")
	}
	if (body.Charset != "" && !charsetPattern.MatchString(body.Charset)) || (body.Collation != "" && !collationPattern.MatchString(body.Collation)) {
		return "", provider.Invalid("Invalid charset or collation.")
	}
	sql := "CREATE DATABASE " + pgwire.QuoteIdentifier(body.Name) + " OWNER " + superuserRole
	if body.Charset != "" || body.Collation != "" {
		sql += " TEMPLATE template0"
	}
	if body.Charset != "" {
		sql += " ENCODING " + pgwire.QuoteLiteral(body.Charset)
	}
	if body.Collation != "" {
		sql += " LC_COLLATE " + pgwire.QuoteLiteral(body.Collation) + " LC_CTYPE " + pgwire.QuoteLiteral(body.Collation)
	}
	return sql, nil
}

// createMySQLDatabase renders CREATE DATABASE for MySQL. An omitted charset or
// collation takes the server default (utf8mb4, utf8mb4_0900_ai_ci); MySQL
// refuses unknown or mismatched names.
func (body databaseBody) createMySQLDatabase() (string, error) {
	if !mysqlName(body.Name, 64) {
		return "", provider.Invalid("Invalid database name.")
	}
	if (body.Charset != "" && !charsetPattern.MatchString(body.Charset)) || (body.Collation != "" && !mysqlCollationPattern.MatchString(body.Collation)) {
		return "", provider.Invalid("Invalid charset or collation.")
	}
	sql := "CREATE DATABASE " + mysqlwire.QuoteIdentifier(body.Name)
	if body.Charset != "" {
		sql += " CHARACTER SET " + body.Charset
	}
	if body.Collation != "" {
		sql += " COLLATE " + body.Collation
	}
	return sql, nil
}

// insertDatabase creates a real database: owned by cloudsqlsuperuser on
// PostgreSQL, a schema on MySQL.
func (p *Provider) insertDatabase(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	var body databaseBody
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, provider.Invalid("Invalid JSON payload.")
	}
	instance, started := request.Path[pathInstance], p.clock()
	mysql, ok := p.engineOf(project, instance)
	if !ok {
		return notFound(), nil
	}
	create := body.createDatabase
	if mysql {
		create = body.createMySQLDatabase
	}
	sql, err := create()
	if err != nil {
		return nil, err
	}
	if _, failure, err := p.sqlOn(call.Context, project, instance, mysql, sql); failure != nil || err != nil {
		return failure, err
	}
	return p.complete(call, project, "CREATE_DATABASE", instance, started)
}

// databaseFilter returns the catalog query for one database, or "" when the
// name cannot exist.
func databaseFilter(mysql bool, name string) string {
	switch {
	case mysql && (mysqlName(name, 64) || mysqlSystemSchemas[name]):
		return mysqlDatabaseQuery + " WHERE SCHEMA_NAME = " + mysqlwire.QuoteLiteral(name)
	case !mysql && objectName(name):
		return databaseQuery + " AND datname = " + pgwire.QuoteLiteral(name)
	default:
		return ""
	}
}

// getDatabase reads one database from the server catalog.
func (p *Provider) getDatabase(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	instance, name := request.Path[pathInstance], request.Path[pathDatabase]
	mysql, ok := p.engineOf(project, instance)
	if !ok {
		return notFound(), nil
	}
	query := databaseFilter(mysql, name)
	if query == "" {
		return apiError(http.StatusNotFound, "NOT_FOUND", "", "Database not found."), nil
	}
	rows, failure, err := p.sqlOn(call.Context, project, instance, mysql, query)
	if failure != nil || err != nil {
		return failure, err
	}
	if len(rows) != 1 || len(rows[0]) != 3 {
		return apiError(http.StatusNotFound, "NOT_FOUND", "", "Database not found."), nil
	}
	return &gateway.RESTResponse{JSON: databaseWire(project, instance, rows[0])}, nil
}

// listDatabases lists the server's user-visible databases.
func (p *Provider) listDatabases(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	instance := request.Path[pathInstance]
	mysql, ok := p.engineOf(project, instance)
	if !ok {
		return notFound(), nil
	}
	query := databaseQuery + " ORDER BY datname"
	if mysql {
		query = mysqlDatabaseQuery + " ORDER BY SCHEMA_NAME"
	}
	rows, failure, err := p.sqlOn(call.Context, project, instance, mysql, query)
	if failure != nil || err != nil {
		return failure, err
	}
	response := map[string]any{keyKind: "sql#databasesList"}
	var items []any
	for _, row := range rows {
		if len(row) == 3 {
			items = append(items, databaseWire(project, instance, row))
		}
	}
	if len(items) > 0 {
		response["items"] = items
	}
	return &gateway.RESTResponse{JSON: response}, nil
}

// deleteDatabase drops a database; PostgreSQL refuses while sessions use it.
// MySQL system schemas are refused before the server sees them: dropping
// them would break the instance's administrative account.
func (p *Provider) deleteDatabase(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	instance, name, started := request.Path[pathInstance], request.Path[pathDatabase], p.clock()
	mysql, ok := p.engineOf(project, instance)
	if !ok {
		return notFound(), nil
	}
	if mysql && mysqlSystemSchemas[name] {
		return nil, provider.Invalid("gcp-local: the MySQL system database " + name + " cannot be deleted.")
	}
	if databaseFilter(mysql, name) == "" {
		return apiError(http.StatusNotFound, "NOT_FOUND", "", "Database not found."), nil
	}
	sql := "DROP DATABASE " + pgwire.QuoteIdentifier(name)
	if mysql {
		sql = "DROP DATABASE " + mysqlwire.QuoteIdentifier(name)
	}
	if _, failure, err := p.sqlOn(call.Context, project, instance, mysql, sql); failure != nil || err != nil {
		return failure, err
	}
	return p.complete(call, project, "DELETE_DATABASE", instance, started)
}

// userBody is the users.insert subset the provider models. host applies to
// MySQL only; Cloud SQL ignores it for PostgreSQL.
type userBody struct {
	Name             string          `json:"name"`
	Host             string          `json:"host"`
	Password         string          `json:"password"`
	Type             string          `json:"type"`
	PasswordPolicy   json.RawMessage `json:"passwordPolicy"`
	DualPasswordType string          `json:"dualPasswordType"`
}

// check refuses a users.insert body the provider cannot honor.
func (body userBody) check(mysql bool) error {
	switch {
	case body.Type != "" && body.Type != "BUILT_IN":
		return gap(fmt.Sprintf("user type %s (IAM database authentication)", body.Type))
	case len(body.PasswordPolicy) > 0 && string(body.PasswordPolicy) != "null":
		return gap("users.insert passwordPolicy")
	case body.DualPasswordType != "":
		return gap("users.insert dualPasswordType")
	case strings.ContainsRune(body.Password, 0) || !body.named(mysql):
		return provider.Invalid("Invalid user.")
	default:
		return nil
	}
}

// named reports whether the user name (and MySQL host) is usable.
func (body userBody) named(mysql bool) bool {
	if mysql {
		return mysqlName(body.Name, 32) && (body.Host == "" || mysqlHostPattern.MatchString(body.Host))
	}
	return objectName(body.Name) && !strings.HasPrefix(body.Name, "pg_")
}

// insertUser creates a built-in login user in cloudsqlsuperuser, as Cloud SQL
// does for users created through the API.
func (p *Provider) insertUser(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	var body userBody
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, provider.Invalid("Invalid user.")
	}
	instance, started := request.Path[pathInstance], p.clock()
	mysql, ok := p.engineOf(project, instance)
	if !ok {
		return notFound(), nil
	}
	if err := body.check(mysql); err != nil {
		return nil, err
	}
	var sql string
	if mysql {
		sql = mysqlCreateUser(body.Name, cmp.Or(body.Host, mysqlAnyHost), body.Password)
	} else {
		password := "NULL"
		if body.Password != "" {
			password = pgwire.QuoteLiteral(body.Password)
		}
		sql = "CREATE ROLE " + pgwire.QuoteIdentifier(body.Name) + " LOGIN CREATEDB CREATEROLE IN ROLE " + superuserRole + " PASSWORD " + password
	}
	if _, failure, err := p.sqlOn(call.Context, project, instance, mysql, sql); failure != nil || err != nil {
		return failure, err
	}
	return p.complete(call, project, "CREATE_USER", instance, started)
}

// listUsers lists the server's login users, hiding the internal accounts.
func (p *Provider) listUsers(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	instance := request.Path[pathInstance]
	mysql, ok := p.engineOf(project, instance)
	if !ok {
		return notFound(), nil
	}
	query := "SELECT rolname FROM pg_roles WHERE rolcanlogin AND rolname <> '" + adminRole + "' AND rolname NOT LIKE 'pg\\_%' ORDER BY rolname"
	if mysql {
		hidden := make([]string, 0, len(mysqlHidden))
		for _, name := range mysqlHidden {
			hidden = append(hidden, mysqlwire.QuoteLiteral(name))
		}
		query = "SELECT User, Host FROM mysql.user WHERE User NOT IN (" + strings.Join(hidden, ", ") + ") ORDER BY User, Host"
	}
	rows, failure, err := p.sqlOn(call.Context, project, instance, mysql, query)
	if failure != nil || err != nil {
		return failure, err
	}
	response := map[string]any{keyKind: "sql#usersList"}
	var items []any
	for _, row := range rows {
		item := map[string]any{keyKind: "sql#user", keyName: row[0], "instance": instance, pathProject: project}
		if mysql && len(row) == 2 {
			// Recorded (cloudsql-mysql-instance-admin): MySQL users also carry
			// etag, iamStatus, and passwordPolicy; only their presence is
			// recorded, so the values are local stand-ins.
			item["host"], item["etag"] = row[1], databaseETag(instance, row[0]+"@"+row[1])
			item["iamStatus"], item["passwordPolicy"] = "IAM_STATUS_UNSPECIFIED", map[string]any{}
		}
		items = append(items, item)
	}
	if len(items) > 0 {
		response["items"] = items
	}
	return &gateway.RESTResponse{JSON: response}, nil
}

// deleteUser drops a login user; PostgreSQL refuses while it owns objects.
// MySQL users are addressed by name and host (default %).
func (p *Provider) deleteUser(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	instance, name, started := request.Path[pathInstance], request.Query.Get("name"), p.clock()
	mysql, ok := p.engineOf(project, instance)
	if !ok {
		return notFound(), nil
	}
	host := cmp.Or(request.Query.Get("host"), mysqlAnyHost)
	sql := "DROP ROLE " + pgwire.QuoteIdentifier(name)
	switch {
	case mysql && (!mysqlName(name, 32) || !mysqlHostPattern.MatchString(host)):
		return nil, provider.Invalid("Invalid user name.")
	case mysql:
		sql = "DROP USER " + mysqlAccount(name, host)
	case !objectName(name) || strings.HasPrefix(name, "pg_"):
		return nil, provider.Invalid("Invalid user name.")
	}
	if _, failure, err := p.sqlOn(call.Context, project, instance, mysql, sql); failure != nil || err != nil {
		return failure, err
	}
	return p.complete(call, project, "DELETE_USER", instance, started)
}

// getOperation renders one recorded operation.
func (p *Provider) getOperation(_ gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	var found map[string]any
	_ = p.catalog.With(provider.ProjectScope(project), func(c *catalog) error {
		for _, operation := range c.operations {
			if operation.Name == request.Path[pathOperation] {
				found = operation.wire(project)
			}
		}
		return nil
	})
	if found == nil {
		return apiError(http.StatusNotFound, "NOT_FOUND", "", "The Cloud SQL operation does not exist."), nil
	}
	return &gateway.RESTResponse{JSON: found}, nil
}

// listOperations renders the project's operations, newest first, optionally
// for one instance.
func (p *Provider) listOperations(_ gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	if request.Query.Get("pageToken") != "" {
		return nil, gap("operations.list pageToken")
	}
	instance := request.Query.Get("instance")
	var items []any
	_ = p.catalog.With(provider.ProjectScope(project), func(c *catalog) error {
		for _, operation := range slices.Backward(c.operations) {
			if instance == "" || operation.Instance == instance {
				items = append(items, operation.wire(project))
			}
		}
		return nil
	})
	response := map[string]any{keyKind: "sql#operationsList"}
	if len(items) > 0 {
		response["items"] = items
	}
	return &gateway.RESTResponse{JSON: response}, nil
}
