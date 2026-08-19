package eval

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"runtime/debug"
	"strings"
)

// SourceStateV2 separates build-time VCS evidence from the repository state
// observed when an evaluation is executed. A clean result is verified only
// when both revisions are known, equal, and unmodified.
type SourceStateV2 struct {
	BuildRevision   string `json:"build_revision"`
	RuntimeRevision string `json:"runtime_revision"`
	BuildModified   bool   `json:"build_modified"`
	RuntimeModified bool   `json:"runtime_modified"`
	Verified        bool   `json:"verified"`
	Clean           bool   `json:"clean"`
	InspectionError string `json:"inspection_error,omitempty"`
}

// SourceProofV2 records both runtime inspections used by a recorded run. A
// final clean state alone is insufficient because a dirty checkout could be
// restored while the benchmark is still running.
type SourceProofV2 struct {
	Before        SourceStateV2 `json:"before"`
	After         SourceStateV2 `json:"after"`
	Stable        bool          `json:"stable"`
	CleanRequired bool          `json:"clean_required"`
	CleanVerified bool          `json:"clean_verified"`
}

type buildSourceStateV2 struct {
	revision      string
	modified      bool
	revisionKnown bool
	modifiedKnown bool
}

type gitRunnerV2 interface {
	Output(context.Context, string, ...string) ([]byte, error)
}

type execGitRunnerV2 struct{}

func (execGitRunnerV2) Output(ctx context.Context, workingDirectory string, arguments ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, "git", arguments...)
	command.Dir = workingDirectory
	return command.Output()
}

// InspectSourceStateV2 inspects the build metadata and the Git checkout that
// contains workingDirectory. Inspection failures remain explicit metadata so
// ephemeral runs can proceed, while RequireCleanSourceV2 can enforce a gate.
func InspectSourceStateV2(ctx context.Context, workingDirectory string) SourceStateV2 {
	return inspectSourceStateV2(ctx, workingDirectory, readBuildSourceStateV2(), execGitRunnerV2{})
}

func inspectSourceStateV2(ctx context.Context, workingDirectory string, build buildSourceStateV2, git gitRunnerV2) SourceStateV2 {
	state := SourceStateV2{
		BuildRevision: "unknown",
		BuildModified: build.modified,
	}
	if build.revisionKnown && build.revision != "" {
		state.BuildRevision = build.revision
	}

	revisionOutput, err := git.Output(ctx, workingDirectory, "rev-parse", "HEAD")
	if err != nil {
		state.RuntimeRevision = "unknown"
		state.InspectionError = "runtime git revision unavailable"
		return state
	}
	state.RuntimeRevision = strings.TrimSpace(string(revisionOutput))
	if state.RuntimeRevision == "" {
		state.RuntimeRevision = "unknown"
		state.InspectionError = "runtime git revision is empty"
		return state
	}

	statusOutput, err := git.Output(ctx, workingDirectory, "status", "--porcelain", "--untracked-files=normal")
	if err != nil {
		state.InspectionError = "runtime git status unavailable"
		return state
	}
	state.RuntimeModified = strings.TrimSpace(string(statusOutput)) != ""
	state.Verified = build.revisionKnown && build.modifiedKnown && state.BuildRevision != "unknown"
	state.Clean = state.Verified &&
		state.BuildRevision == state.RuntimeRevision &&
		!state.BuildModified &&
		!state.RuntimeModified
	return state
}

func readBuildSourceStateV2() buildSourceStateV2 {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return buildSourceStateV2{}
	}
	state := buildSourceStateV2{}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			state.revision = setting.Value
			state.revisionKnown = setting.Value != ""
		case "vcs.modified":
			if setting.Value == "true" || setting.Value == "false" {
				state.modified = setting.Value == "true"
				state.modifiedKnown = true
			}
		}
	}
	return state
}

// RequireCleanSourceV2 turns source provenance into a hard acceptance gate.
func RequireCleanSourceV2(state SourceStateV2) error {
	strictlyClean := state.Verified &&
		state.InspectionError == "" &&
		state.BuildRevision != "" && state.BuildRevision != "unknown" &&
		state.RuntimeRevision != "" && state.RuntimeRevision != "unknown" &&
		state.BuildRevision == state.RuntimeRevision &&
		!state.BuildModified &&
		!state.RuntimeModified
	if state.Clean && strictlyClean {
		return nil
	}
	if state.InspectionError != "" {
		return fmt.Errorf("clean source revision is required: %s", state.InspectionError)
	}
	var reasons []string
	if !state.Verified {
		reasons = append(reasons, "build and runtime revisions were not verified")
	}
	if state.BuildRevision != state.RuntimeRevision {
		reasons = append(reasons, "build and runtime revisions differ")
	}
	if state.BuildModified {
		reasons = append(reasons, "binary was built from a modified checkout")
	}
	if state.RuntimeModified {
		reasons = append(reasons, "runtime checkout is modified")
	}
	if len(reasons) == 0 {
		return errors.New("clean source revision is required")
	}
	return fmt.Errorf("clean source revision is required: %s", strings.Join(reasons, "; "))
}
