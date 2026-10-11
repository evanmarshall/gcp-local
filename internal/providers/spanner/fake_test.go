package spanner

import (
	"context"
	"strings"
	"sync"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// fakeDriver is a minimal in-memory stand-in for the emulator's admin RPCs,
// so unit tests never need the emulator binary.
type fakeDriver struct {
	mu                   sync.Mutex
	starts, stops, calls int
	instances            map[string]*instancepb.Instance
	databaseNames        map[string]bool
	fault                func(method string) error
	invoked              []proto.Message
}

func newFakeDriver() *fakeDriver {
	return &fakeDriver{instances: map[string]*instancepb.Instance{}, databaseNames: map[string]bool{}}
}

func (f *fakeDriver) Start(context.Context) error { f.mu.Lock(); f.starts++; f.mu.Unlock(); return nil }
func (*fakeDriver) Ready(context.Context) error   { return nil }
func (f *fakeDriver) Stop(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stops++
	f.instances, f.databaseNames = map[string]*instancepb.Instance{}, map[string]bool{}
	return nil
}
func (f *fakeDriver) Reset(ctx context.Context) error  { return f.Stop(ctx) }
func (f *fakeDriver) client() grpc.ClientConnInterface { return f }

func (*fakeDriver) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	panic("unexpected backend stream")
}

// counts returns the start and call counters.
func (f *fakeDriver) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.starts, f.calls
}

func (f *fakeDriver) Invoke(ctx context.Context, method string, request, response any, _ ...grpc.CallOption) error {
	if md, _ := metadata.FromOutgoingContext(ctx); len(md) != 0 {
		panic("caller metadata reached the backend")
	}
	in, inOK := request.(proto.Message)
	out, outOK := response.(proto.Message)
	if !inOK || !outOK {
		return status.Error(codes.Internal, "fake requires protobuf messages")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.invoked = append(f.invoked, proto.Clone(in))
	if f.fault != nil {
		if err := f.fault(method); err != nil {
			return err
		}
	}
	result, err := f.serve(in)
	if err != nil {
		return err
	}
	proto.Merge(out, result)
	return nil
}

// serve answers one admin RPC from the fake's state.
func (f *fakeDriver) serve(request proto.Message) (proto.Message, error) {
	switch request := request.(type) {
	case *instancepb.ListInstanceConfigsRequest:
		return &instancepb.ListInstanceConfigsResponse{}, nil
	case *instancepb.CreateInstanceRequest:
		name := request.GetInstance().GetName()
		if f.instances[name] != nil {
			return nil, status.Error(codes.AlreadyExists, "Instance already exists")
		}
		f.instances[name] = &instancepb.Instance{Name: name, Config: request.GetInstance().GetConfig(),
			ProcessingUnits: request.GetInstance().GetProcessingUnits(), State: instancepb.Instance_READY}
		return operation(name, f.instances[name])
	case *instancepb.GetInstanceRequest:
		return f.instance(request.GetName())
	case *instancepb.DeleteInstanceRequest:
		return f.deleteInstance(request.GetName())
	default:
		return f.serveDatabases(request)
	}
}

func (f *fakeDriver) deleteInstance(name string) (proto.Message, error) {
	if _, err := f.instance(name); err != nil {
		return nil, err
	}
	delete(f.instances, name)
	for database := range f.databaseNames {
		if strings.HasPrefix(database, name+"/") {
			delete(f.databaseNames, database)
		}
	}
	return &emptypb.Empty{}, nil
}

func (f *fakeDriver) serveDatabases(request proto.Message) (proto.Message, error) {
	switch request := request.(type) {
	case *databasepb.CreateDatabaseRequest:
		if _, err := f.instance(request.GetParent()); err != nil {
			return nil, err
		}
		match := createStatement.FindStringSubmatch(request.GetCreateStatement())
		name := request.GetParent() + "/databases/" + match[1]
		f.databaseNames[name] = true
		return operation(name, &databasepb.Database{Name: name, State: databasepb.Database_READY})
	case *databasepb.GetDatabaseRequest:
		if !f.databaseNames[request.GetName()] {
			return nil, status.Error(codes.NotFound, "Database not found")
		}
		return &databasepb.Database{Name: request.GetName(), State: databasepb.Database_READY}, nil
	case *databasepb.GetDatabaseDdlRequest:
		return &databasepb.GetDatabaseDdlResponse{}, nil
	case *databasepb.ListDatabasesRequest:
		response := &databasepb.ListDatabasesResponse{}
		for name := range f.databaseNames {
			if strings.HasPrefix(name, request.GetParent()+"/") {
				response.Databases = append(response.Databases, &databasepb.Database{Name: name})
			}
		}
		return response, nil
	default:
		return nil, status.Error(codes.Unimplemented, "fake does not serve this request")
	}
}

func (f *fakeDriver) instance(name string) (*instancepb.Instance, error) {
	instance := f.instances[name]
	if instance == nil {
		return nil, status.Error(codes.NotFound, "Instance not found")
	}
	return instance, nil
}

// operation wraps a result in a done long-running operation.
func operation(parent string, result proto.Message) (proto.Message, error) {
	packed, err := anypb.New(result)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "pack the operation result: %v", err)
	}
	return &longrunningpb.Operation{Name: parent + "/operations/_auto0", Done: true,
		Result: &longrunningpb.Operation_Response{Response: packed}}, nil
}
