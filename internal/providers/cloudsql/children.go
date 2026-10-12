package cloudsql

import (
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
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
)

// Request shape checks for databases and users.
var (
	charsetPattern   = regexp.MustCompile(`^[A-Za-z0-9_]{1,32}$`)
	collationPattern = regexp.MustCompile(`^[A-Za-z0-9_.@-]{1,64}$`)
	reservedNames    = map[string]bool{adminRole: true, superuserRole: true, "template0": true, "template1": true}
)

// objectName reports whether a database or role name is usable: PostgreSQL
// truncates identifiers beyond 63 bytes and no text value can carry NUL.
func objectName(name string) bool {
	return name != "" && len(name) <= 63 && !strings.ContainsRune(name, 0) && !reservedNames[name]
}

// databaseQuery selects the user-visible databases of a server.
const databaseQuery = "SELECT datname, pg_encoding_to_char(encoding), datcollate FROM pg_database WHERE NOT datistemplate AND datname <> " +
	"'" + adminRole + "'"

// sqlOn runs administrative SQL on an existing instance's server. A missing
// instance renders notFound; a PostgreSQL error renders an API error.
func (p *Provider) sqlOn(ctx context.Context, project, instance, sql string) ([][]string, *gateway.RESTResponse, error) {
	if _, ok := p.lookup(project, instance); !ok {
		return nil, notFound(), nil
	}
	var rows [][]string
	missing := false
	err := p.use(ctx, project, func(ctx context.Context, fleet driver) error {
		// Re-check under the mutation gate: a concurrent delete may have won.
		if _, ok := p.lookup(project, instance); !ok {
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
	sql := "CREATE DATABASE " + quoteIdentifier(body.Name) + " OWNER " + superuserRole
	if body.Charset != "" || body.Collation != "" {
		sql += " TEMPLATE template0"
	}
	if body.Charset != "" {
		sql += " ENCODING " + quoteLiteral(body.Charset)
	}
	if body.Collation != "" {
		sql += " LC_COLLATE " + quoteLiteral(body.Collation) + " LC_CTYPE " + quoteLiteral(body.Collation)
	}
	return sql, nil
}

// insertDatabase creates a real database owned by cloudsqlsuperuser.
func (p *Provider) insertDatabase(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	var body databaseBody
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, provider.Invalid("Invalid JSON payload.")
	}
	sql, err := body.createDatabase()
	if err != nil {
		return nil, err
	}
	instance, started := request.Path[pathInstance], p.clock()
	if _, failure, err := p.sqlOn(call.Context, project, instance, sql); failure != nil || err != nil {
		return failure, err
	}
	return p.complete(call, project, "CREATE_DATABASE", instance, started)
}

// getDatabase reads one database from the server catalog.
func (p *Provider) getDatabase(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	instance, name := request.Path[pathInstance], request.Path[pathDatabase]
	if !objectName(name) {
		return apiError(http.StatusNotFound, "NOT_FOUND", "", "Database not found."), nil
	}
	rows, failure, err := p.sqlOn(call.Context, project, instance, databaseQuery+" AND datname = "+quoteLiteral(name))
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
	rows, failure, err := p.sqlOn(call.Context, project, instance, databaseQuery+" ORDER BY datname")
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
func (p *Provider) deleteDatabase(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	instance, name, started := request.Path[pathInstance], request.Path[pathDatabase], p.clock()
	if !objectName(name) {
		return apiError(http.StatusNotFound, "NOT_FOUND", "", "Database not found."), nil
	}
	if _, failure, err := p.sqlOn(call.Context, project, instance, "DROP DATABASE "+quoteIdentifier(name)); failure != nil || err != nil {
		return failure, err
	}
	return p.complete(call, project, "DELETE_DATABASE", instance, started)
}

// insertUser creates a built-in login role in cloudsqlsuperuser, as Cloud SQL
// does for users created through the API.
func (p *Provider) insertUser(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	var body struct {
		Name     string `json:"name"`
		Password string `json:"password"`
		Type     string `json:"type"`
	}
	if json.Unmarshal(request.JSON, &body) != nil || !objectName(body.Name) || strings.HasPrefix(body.Name, "pg_") || strings.ContainsRune(body.Password, 0) {
		return nil, provider.Invalid("Invalid user.")
	}
	if body.Type != "" && body.Type != "BUILT_IN" {
		return nil, gap(fmt.Sprintf("user type %s (IAM database authentication)", body.Type))
	}
	password := "NULL"
	if body.Password != "" {
		password = quoteLiteral(body.Password)
	}
	instance, started := request.Path[pathInstance], p.clock()
	sql := "CREATE ROLE " + quoteIdentifier(body.Name) + " LOGIN CREATEDB CREATEROLE IN ROLE " + superuserRole + " PASSWORD " + password
	if _, failure, err := p.sqlOn(call.Context, project, instance, sql); failure != nil || err != nil {
		return failure, err
	}
	return p.complete(call, project, "CREATE_USER", instance, started)
}

// listUsers lists the server's login roles, hiding the internal administrator.
func (p *Provider) listUsers(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	instance := request.Path[pathInstance]
	rows, failure, err := p.sqlOn(call.Context, project, instance,
		"SELECT rolname FROM pg_roles WHERE rolcanlogin AND rolname <> '"+adminRole+"' AND rolname NOT LIKE 'pg\\_%' ORDER BY rolname")
	if failure != nil || err != nil {
		return failure, err
	}
	response := map[string]any{keyKind: "sql#usersList"}
	var items []any
	for _, row := range rows {
		items = append(items, map[string]any{keyKind: "sql#user", keyName: row[0], "instance": instance, pathProject: project})
	}
	if len(items) > 0 {
		response["items"] = items
	}
	return &gateway.RESTResponse{JSON: response}, nil
}

// deleteUser drops a login role; PostgreSQL refuses while it owns objects.
func (p *Provider) deleteUser(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	instance, name, started := request.Path[pathInstance], request.Query.Get("name"), p.clock()
	if !objectName(name) || strings.HasPrefix(name, "pg_") {
		return nil, provider.Invalid("Invalid user name.")
	}
	if _, failure, err := p.sqlOn(call.Context, project, instance, "DROP ROLE "+quoteIdentifier(name)); failure != nil || err != nil {
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
