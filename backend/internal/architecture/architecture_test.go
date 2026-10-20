// Package architecture 用测试约束分层依赖方向。
package architecture

import (
	"os/exec"
	"strings"
	"testing"
)

const modulePrefix = "feedsystem_video_go/internal/"

var domainForbidden = []string{
	"application",
	"infrastructure",
	"interfaces",
	"middleware",
	"http",
	"config",
	"db",
	"account",
	"video",
	"feed",
	"social",
	"worker",
	"observability",
}

var applicationForbidden = []string{
	"infrastructure",
	"interfaces",
	"middleware",
	"http",
	"config",
	"db",
	"account",
	"video",
	"feed",
	"social",
	"worker",
	"observability",
}

func TestLayerDependencies(t *testing.T) {
	output, err := exec.Command(
		"go", "list",
		"-f", "{{.ImportPath}}|{{join .Imports \" \"}}",
		"feedsystem_video_go/internal/domain/...",
		"feedsystem_video_go/internal/application/...",
	).Output()
	if err != nil {
		t.Fatalf("go list failed: %v", err)
	}

	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		parts := strings.SplitN(line, "|", 2)
		if len(parts) != 2 {
			continue
		}
		pkg := parts[0]
		imports := strings.Fields(parts[1])

		var forbidden []string
		switch {
		case strings.HasPrefix(pkg, modulePrefix+"domain"):
			forbidden = domainForbidden
		case strings.HasPrefix(pkg, modulePrefix+"application"):
			forbidden = applicationForbidden
		default:
			continue
		}

		for _, forbiddenPkg := range forbidden {
			prefix := modulePrefix + forbiddenPkg
			for _, imported := range imports {
				if imported == prefix || strings.HasPrefix(imported, prefix+"/") {
					t.Errorf("%s must not import %s", pkg, imported)
				}
			}
		}
	}
}
