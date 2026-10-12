package main

import (
	"encoding/json"
	"strings"

	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
)

// cloudSchedulerMethods is the Cloud Scheduler v1 job slice selected from the
// pinned discovery document, in route order. Selecting a method does not
// claim provider support.
var cloudSchedulerMethods = []string{
	"cloudscheduler.projects.locations.jobs.create",
	"cloudscheduler.projects.locations.jobs.list",
	"cloudscheduler.projects.locations.jobs.get",
	"cloudscheduler.projects.locations.jobs.patch",
	"cloudscheduler.projects.locations.jobs.delete",
	"cloudscheduler.projects.locations.jobs.pause",
	"cloudscheduler.projects.locations.jobs.resume",
	"cloudscheduler.projects.locations.jobs.run",
}

// cloudSchedulerVariables renames discovery path parameters to gateway path
// variables.
var cloudSchedulerVariables = strings.NewReplacer("{projectsId}", "{project}", "{locationsId}", "{location}", "{jobsId}", "{job}")

// cloudSchedulerRows reads the selected job routes from the pinned discovery
// document, checksum-verified against its manifest.
func cloudSchedulerRows() []route {
	raw := readDiscovery("api/discovery/cloudscheduler-v1.json", "api/discovery/cloudscheduler-v1.manifest.json", "Cloud Scheduler")
	var discovery struct {
		Resources map[string]struct {
			Resources map[string]struct {
				Resources map[string]struct {
					Methods map[string]struct {
						ID       string         `json:"id"`
						Verb     string         `json:"httpMethod"`
						FlatPath string         `json:"flatPath"`
						Request  map[string]any `json:"request"`
					} `json:"methods"`
				} `json:"resources"`
			} `json:"resources"`
		} `json:"resources"`
	}
	must(json.Unmarshal(raw, &discovery))
	jobs := discovery.Resources["projects"].Resources["locations"].Resources["jobs"].Methods
	rows := make([]route, 0, len(cloudSchedulerMethods))
	for _, id := range cloudSchedulerMethods {
		m, ok := jobs[strings.TrimPrefix(id, "cloudscheduler.projects.locations.jobs.")]
		if !ok || m.ID != id {
			panic("Cloud Scheduler discovery lacks " + id)
		}
		rows = append(rows, route{m.ID, m.Verb, "/" + cloudSchedulerVariables.Replace(m.FlatPath), len(m.Request) > 0, false})
	}
	return rows
}

// cloudSchedulerRoutes generates the Cloud Scheduler v1 route table slice.
func cloudSchedulerRoutes() []byte {
	return routeTable("CloudSchedulerRoutes", "is the Cloud Scheduler v1 job route table slice.", cloudSchedulerRows())
}

// cloudSchedulerRPC is the google.cloud.scheduler.v1.CloudScheduler surface
// of the Cloud Scheduler REST provider.
var cloudSchedulerRPC = transcodedService{
	service: schedulerpb.File_google_cloud_scheduler_v1_cloudscheduler_proto.Services().ByName("CloudScheduler"),
	alias:   "schedulerpb", importPath: "cloud.google.com/go/scheduler/apiv1/schedulerpb",
	variable: "CloudSchedulerMethods", bindings: "CloudSchedulerTranscoding",
	rest: map[string]string{
		"CreateJob": "cloudscheduler.projects.locations.jobs.create", "ListJobs": "cloudscheduler.projects.locations.jobs.list",
		"GetJob": "cloudscheduler.projects.locations.jobs.get", "UpdateJob": "cloudscheduler.projects.locations.jobs.patch",
		"DeleteJob": "cloudscheduler.projects.locations.jobs.delete", "PauseJob": "cloudscheduler.projects.locations.jobs.pause",
		"ResumeJob": "cloudscheduler.projects.locations.jobs.resume", "RunJob": "cloudscheduler.projects.locations.jobs.run",
	},
	routes: cloudSchedulerRows(),
}
