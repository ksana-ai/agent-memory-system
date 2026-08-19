package eval

import (
	"context"
	"errors"
	"slices"
	"testing"
)

type fakeGitRunnerV2 struct {
	revision []byte
	status   []byte
	failAt   string
	calls    [][]string
}

func (runner *fakeGitRunnerV2) Output(_ context.Context, _ string, arguments ...string) ([]byte, error) {
	runner.calls = append(runner.calls, append([]string(nil), arguments...))
	if len(arguments) > 0 && arguments[0] == runner.failAt {
		return nil, errors.New("synthetic git failure")
	}
	if len(arguments) > 0 && arguments[0] == "rev-parse" {
		return runner.revision, nil
	}
	return runner.status, nil
}

func TestInspectSourceStateV2RequiresMatchingCleanBuildAndRuntime(t *testing.T) {
	runner := &fakeGitRunnerV2{revision: []byte("abc123\n")}
	state := inspectSourceStateV2(context.Background(), ".", buildSourceStateV2{
		revision:      "abc123",
		revisionKnown: true,
		modifiedKnown: true,
	}, runner)
	if !state.Verified || !state.Clean || state.BuildRevision != "abc123" || state.RuntimeRevision != "abc123" {
		t.Fatalf("unexpected source state: %#v", state)
	}
	if err := RequireCleanSourceV2(state); err != nil {
		t.Fatalf("require clean: %v", err)
	}
	wantCalls := [][]string{
		{"rev-parse", "HEAD"},
		{"status", "--porcelain", "--untracked-files=normal"},
	}
	if !slices.EqualFunc(runner.calls, wantCalls, func(left, right []string) bool {
		return slices.Equal(left, right)
	}) {
		t.Fatalf("git calls = %#v, want %#v", runner.calls, wantCalls)
	}
}

func TestInspectSourceStateV2ReportsDirtyAndMismatchedStates(t *testing.T) {
	runner := &fakeGitRunnerV2{revision: []byte("runtime\n"), status: []byte(" M file.go\n")}
	state := inspectSourceStateV2(context.Background(), ".", buildSourceStateV2{
		revision:      "build",
		modified:      true,
		revisionKnown: true,
		modifiedKnown: true,
	}, runner)
	if state.Clean || !state.Verified || !state.BuildModified || !state.RuntimeModified {
		t.Fatalf("unexpected source state: %#v", state)
	}
	if err := RequireCleanSourceV2(state); err == nil {
		t.Fatal("RequireCleanSourceV2 accepted dirty, mismatched source")
	}
}

func TestInspectSourceStateV2KeepsInspectionFailureExplicit(t *testing.T) {
	runner := &fakeGitRunnerV2{failAt: "rev-parse"}
	state := inspectSourceStateV2(context.Background(), ".", buildSourceStateV2{}, runner)
	if state.Clean || state.Verified || state.InspectionError == "" || state.RuntimeRevision != "unknown" {
		t.Fatalf("unexpected source state: %#v", state)
	}
	if err := RequireCleanSourceV2(state); err == nil {
		t.Fatal("RequireCleanSourceV2 accepted failed inspection")
	}
}

func TestSourceStateV2RequiresKnownModifiedFlagAndRejectsForgedCleanBit(t *testing.T) {
	runner := &fakeGitRunnerV2{revision: []byte("abc123\n")}
	missingModified := inspectSourceStateV2(context.Background(), ".", buildSourceStateV2{
		revision:      "abc123",
		revisionKnown: true,
	}, runner)
	if missingModified.Verified || missingModified.Clean {
		t.Fatalf("missing vcs.modified was treated as verified: %#v", missingModified)
	}

	forged := SourceStateV2{
		BuildRevision:   "build",
		RuntimeRevision: "runtime",
		BuildModified:   true,
		RuntimeModified: true,
		Verified:        false,
		Clean:           true,
		InspectionError: "synthetic failure",
	}
	if err := RequireCleanSourceV2(forged); err == nil {
		t.Fatal("RequireCleanSourceV2 trusted a forged clean bit")
	}
}
