package run

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// groupListens reports whether a process in group pgid holds a listening TCP
// socket on port. It matches the socket inodes of /proc/net/tcp{,6} against
// the descriptors of the group's processes.
func groupListens(pgid, port int) bool {
	inodes := listeningInodes(port)
	if len(inodes) == 0 {
		return false
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return false
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || processGroup(pid) != pgid {
			continue
		}
		if holdsSocket(pid, inodes) {
			return true
		}
	}
	return false
}

// listeningInodes returns the inodes of the sockets listening on port.
func listeningInodes(port int) map[string]bool {
	inodes := map[string]bool{}
	suffix := ":" + strings.ToUpper(strconv.FormatInt(int64(port), 16))
	for _, table := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		data, err := os.ReadFile(table) //nolint:gosec // fixed /proc/net socket tables, not caller input
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n")[1:] {
			// sl local_address rem_address st tx:rx tr:when retrnsmt uid timeout inode
			fields := strings.Fields(line)
			if len(fields) > 9 && fields[3] == "0A" && strings.HasSuffix(fields[1], suffix) {
				inodes[fields[9]] = true
			}
		}
	}
	return inodes
}

// processGroup returns a process's group ID, or -1 when it cannot be read.
func processGroup(pid int) int {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return -1
	}
	// The command name is parenthesized and may hold spaces; the fields after
	// it are state, ppid, then pgrp.
	end := bytes.LastIndexByte(data, ')')
	if end < 0 {
		return -1
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) < 3 {
		return -1
	}
	group, err := strconv.Atoi(fields[2])
	if err != nil {
		return -1
	}
	return group
}

// holdsSocket reports whether one process has a descriptor for any inode.
func holdsSocket(pid int, inodes map[string]bool) bool {
	directory := "/proc/" + strconv.Itoa(pid) + "/fd"
	descriptors, err := os.ReadDir(directory)
	if err != nil {
		return false
	}
	for _, descriptor := range descriptors {
		target, err := os.Readlink(filepath.Join(directory, descriptor.Name()))
		if err == nil && strings.HasPrefix(target, "socket:[") && inodes[strings.TrimSuffix(strings.TrimPrefix(target, "socket:["), "]")] {
			return true
		}
	}
	return false
}
