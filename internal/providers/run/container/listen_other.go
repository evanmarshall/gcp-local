//go:build !linux

package container

import (
	"context"
	"os/exec"
	"strconv"
	"time"
)

// groupListens reports whether a process in group pgid holds a listening TCP
// socket on port, asking lsof, which macOS ships. Without lsof nothing
// verifies, and the revision never becomes ready.
func groupListens(pgid, port int) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// -a joins the selections: this group, this TCP port, listening only.
	query := exec.CommandContext(ctx, "lsof", "-nP", "-a", "-g", strconv.Itoa(pgid), "-iTCP:"+strconv.Itoa(port), "-sTCP:LISTEN", "-t") //nolint:gosec // numeric arguments only
	output, err := query.Output()
	return err == nil && len(output) > 0
}
