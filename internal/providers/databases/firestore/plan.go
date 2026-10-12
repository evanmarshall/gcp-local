package firestore

import (
	"errors"
	"strings"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// requestPlans validates every resource name a request carries against the
// request project's (default) database and returns the permission plans it
// needs. Both transports call it before any backend work. A request whose
// names leave the project or database is rejected; named databases are an
// explicit coverage gap.
func requestPlans(project string, request proto.Message) ([]string, error) {
	for _, group := range []func(string, proto.Message) ([]string, error){documentPlans, databasePlans, queryPlans} {
		if plans, err := group(project, request); !errors.Is(err, errUnmatched) {
			return plans, err
		}
	}
	return nil, status.Error(codes.Unimplemented, "Firestore coverage gap: this method is not served")
}

// errUnmatched tells requestPlans that a plan group does not own the request type.
var errUnmatched = errors.New("request type not in this plan group")

// queryPlans plans queries, collection-ID listings, and Listen.
func queryPlans(project string, request proto.Message) ([]string, error) {
	switch r := request.(type) {
	case *pb.RunQueryRequest:
		return withNewTransaction([]string{methodRunQuery}, r.GetNewTransaction() != nil), checkParent(project, r.GetParent())
	case *pb.RunAggregationQueryRequest:
		return withNewTransaction([]string{methodRunAggregation}, r.GetNewTransaction() != nil), checkParent(project, r.GetParent())
	case *pb.ListCollectionIdsRequest:
		return []string{methodListCollectionIDs}, checkParent(project, r.GetParent())
	case *pb.ListenRequest:
		return listenPlans(project, r)
	default:
		return nil, errUnmatched
	}
}

// documentPlans plans the single-document and collection-listing methods.
func documentPlans(project string, request proto.Message) ([]string, error) {
	switch r := request.(type) {
	case *pb.GetDocumentRequest:
		return []string{methodDocumentsGet}, checkDocument(project, r.GetName())
	case *pb.ListDocumentsRequest:
		return []string{planDocumentsList}, checkCollection(project, r.GetParent(), r.GetCollectionId())
	case *pb.CreateDocumentRequest:
		if err := checkCollection(project, r.GetParent(), r.GetCollectionId()); err != nil {
			return nil, err
		}
		if id := r.GetDocumentId(); id != "" && !validSegment(id) {
			return nil, status.Errorf(codes.InvalidArgument, "Invalid document id %q", id)
		}
		return []string{methodDocumentsCreate}, nil
	case *pb.UpdateDocumentRequest:
		return []string{methodDocumentsUpdate}, checkDocument(project, r.GetDocument().GetName())
	case *pb.DeleteDocumentRequest:
		return []string{methodDocumentsDelete}, checkDocument(project, r.GetName())
	default:
		return nil, errUnmatched
	}
}

// databasePlans plans the database-scoped batch and transaction methods.
func databasePlans(project string, request proto.Message) ([]string, error) {
	switch r := request.(type) {
	case *pb.BatchGetDocumentsRequest:
		if err := checkDatabase(project, r.GetDatabase()); err != nil {
			return nil, err
		}
		for _, name := range r.GetDocuments() {
			if err := checkDocument(project, name); err != nil {
				return nil, err
			}
		}
		return withNewTransaction([]string{methodBatchGet}, r.GetNewTransaction() != nil), nil
	case *pb.BeginTransactionRequest:
		return []string{methodBeginTransaction}, checkDatabase(project, r.GetDatabase())
	case *pb.RollbackRequest:
		return []string{methodRollback}, checkDatabase(project, r.GetDatabase())
	case *pb.CommitRequest:
		return writePlans(project, r.GetDatabase(), r.GetWrites())
	case *pb.BatchWriteRequest:
		return writePlans(project, r.GetDatabase(), r.GetWrites())
	default:
		return nil, errUnmatched
	}
}

// withNewTransaction adds the transaction plan to a read that begins one.
func withNewTransaction(plans []string, begins bool) []string {
	if begins {
		return append(plans, methodBeginTransaction)
	}
	return plans
}

// writePlans returns one plan per distinct write kind in a commit or batch
// write, after checking every target document.
func writePlans(project, database string, writes []*pb.Write) ([]string, error) {
	if err := checkDatabase(project, database); err != nil {
		return nil, err
	}
	if len(writes) == 0 {
		return []string{planWriteNone}, nil
	}
	seen := map[string]bool{}
	var plans []string
	for _, write := range writes {
		plan, err := writePlan(project, write)
		if err != nil {
			return nil, err
		}
		if !seen[plan] {
			seen[plan] = true
			plans = append(plans, plan)
		}
	}
	return plans, nil
}

// writePlan classifies one write by its operation and precondition.
func writePlan(project string, write *pb.Write) (string, error) {
	var name string
	switch operation := write.GetOperation().(type) {
	case *pb.Write_Delete:
		return planWriteDelete, checkDocument(project, operation.Delete)
	case *pb.Write_Update:
		name = operation.Update.GetName()
	case *pb.Write_Transform:
		name = operation.Transform.GetDocument()
	default:
		return "", status.Error(codes.InvalidArgument, "A write needs an update, delete, or transform")
	}
	if err := checkDocument(project, name); err != nil {
		return "", err
	}
	switch condition := write.GetCurrentDocument().GetConditionType().(type) {
	case *pb.Precondition_Exists:
		if condition.Exists {
			return planWriteUpdate, nil
		}
		return planWriteCreate, nil
	case *pb.Precondition_UpdateTime:
		return planWriteUpdate, nil
	default:
		return planWriteUpsert, nil
	}
}

// listenPlans checks a Listen request's database and any added target.
func listenPlans(project string, request *pb.ListenRequest) ([]string, error) {
	if err := checkDatabase(project, request.GetDatabase()); err != nil {
		return nil, err
	}
	target := request.GetAddTarget()
	if target == nil {
		return []string{planListen}, nil
	}
	for _, name := range target.GetDocuments().GetDocuments() {
		if err := checkDocument(project, name); err != nil {
			return nil, err
		}
	}
	if query := target.GetQuery(); query != nil {
		if err := checkParent(project, query.GetParent()); err != nil {
			return nil, err
		}
	}
	return []string{planListen}, nil
}

// databaseName is the only database the provider serves.
func databaseName(project string) string {
	return "projects/" + project + "/databases/(default)"
}

// checkDatabase accepts only the request project's (default) database.
func checkDatabase(project, name string) error {
	if name == databaseName(project) {
		return nil
	}
	if rest, ok := strings.CutPrefix(name, "projects/"+project+"/databases/"); ok && rest != "" && !strings.Contains(rest, "/") {
		return status.Error(codes.Unimplemented, "Firestore coverage gap: only the (default) database is served")
	}
	return status.Errorf(codes.InvalidArgument, "Invalid database %q for project %q", name, project)
}

// documentSegments splits a name under the database's documents root into
// its collection/document segments; the root itself has none.
func documentSegments(project, name string) ([]string, error) {
	root := databaseName(project) + "/documents"
	if name == root {
		return nil, nil
	}
	rest, ok := strings.CutPrefix(name, root+"/")
	if !ok {
		if other, found := strings.CutPrefix(name, "projects/"+project+"/databases/"); found {
			if database, _, _ := strings.Cut(other, "/"); database != "" && database != "(default)" {
				return nil, status.Error(codes.Unimplemented, "Firestore coverage gap: only the (default) database is served")
			}
		}
		return nil, status.Errorf(codes.InvalidArgument, "Invalid document path %q for project %q", name, project)
	}
	segments := strings.Split(rest, "/")
	for _, segment := range segments {
		if !validSegment(segment) {
			return nil, status.Errorf(codes.InvalidArgument, "Invalid document path %q", name)
		}
	}
	return segments, nil
}

// checkDocument accepts a document name: an even, nonzero segment count.
func checkDocument(project, name string) error {
	segments, err := documentSegments(project, name)
	if err != nil {
		return err
	}
	if len(segments) == 0 || len(segments)%2 != 0 {
		return status.Errorf(codes.InvalidArgument, "Invalid document name %q", name)
	}
	return nil
}

// checkParent accepts a query or listing parent: the documents root or a
// document.
func checkParent(project, name string) error {
	segments, err := documentSegments(project, name)
	if err != nil {
		return err
	}
	if len(segments)%2 != 0 {
		return status.Errorf(codes.InvalidArgument, "Invalid parent %q", name)
	}
	return nil
}

// checkCollection accepts a parent plus one collection ID.
func checkCollection(project, parent, collection string) error {
	if err := checkParent(project, parent); err != nil {
		return err
	}
	if !validSegment(collection) {
		return status.Errorf(codes.InvalidArgument, "Invalid collection id %q", collection)
	}
	return nil
}

// validSegment checks one collection or document ID.
func validSegment(segment string) bool {
	return uid.MatchString(segment) && segment != "." && segment != ".."
}
