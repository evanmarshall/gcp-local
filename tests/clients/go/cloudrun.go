package main

import (
	"context"

	runapi "cloud.google.com/go/run/apiv2"
	"cloud.google.com/go/run/apiv2/runpb"
)

// revisionImage is the image run_clients.py maps to a local revision process.
const revisionImage = "client-matrix/revision:1"

// runCloudRun deploys a service and waits on its operation, reads and lists
// it, and deletes it through the delete operation.
func runCloudRun(ctx context.Context, e *env) error {
	conn, err := e.grpcOption(e.owner)
	if err != nil {
		return err
	}
	client, err := runapi.NewServicesClient(ctx, conn)
	if err != nil {
		return step("new client", err)
	}
	defer client.Close()
	location := "projects/" + e.project + "/locations/us-central1"
	service := &runpb.Service{Template: &runpb.RevisionTemplate{Containers: []*runpb.Container{{Image: revisionImage}}}}
	create, err := client.CreateService(ctx, &runpb.CreateServiceRequest{Parent: location, ServiceId: "go-matrix", Service: service})
	if err != nil {
		return step("create service", err)
	}
	created, err := create.Wait(ctx)
	if err != nil {
		return step("wait for the create operation", err)
	}
	if _, err := client.GetService(ctx, &runpb.GetServiceRequest{Name: created.GetName()}); err != nil {
		return step("get service", err)
	}
	if err := listed(client.ListServices(ctx, &runpb.ListServicesRequest{Parent: location}).Next, created.GetName(), (*runpb.Service).GetName); err != nil {
		return step("list services", err)
	}
	deniedConn, err := e.grpcOption(e.denied)
	if err != nil {
		return err
	}
	denied, err := runapi.NewServicesClient(ctx, deniedConn)
	if err != nil {
		return step("new ungranted client", err)
	}
	defer denied.Close()
	_, err = denied.CreateService(ctx, &runpb.CreateServiceRequest{Parent: location, ServiceId: "go-denied", Service: service})
	if err := expectDenied("ungranted create service", err); err != nil {
		return err
	}
	deletion, err := client.DeleteService(ctx, &runpb.DeleteServiceRequest{Name: created.GetName()})
	if err != nil {
		return step("delete service", err)
	}
	_, err = deletion.Wait(ctx)
	return step("wait for the delete operation", err)
}
