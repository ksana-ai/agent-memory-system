package eval

import (
	"context"
	"testing"
)

func TestBuiltinArmFactoriesConstructIndependentRuntimes(t *testing.T) {
	factories := BuiltinArmFactories()
	if len(factories) != 2 {
		t.Fatalf("factory count = %d, want 2", len(factories))
	}
	for _, factory := range factories {
		descriptor := factory.Descriptor()
		if descriptor.ID == "" || descriptor.Version == "" || descriptor.JudgmentProfile == "" || descriptor.ResultKind == "" || len(descriptor.ConfigHash) != 64 {
			t.Fatalf("incomplete descriptor: %#v", descriptor)
		}
		first, err := factory.NewRuntime(context.Background())
		if err != nil {
			t.Fatalf("%s first runtime: %v", descriptor.ID, err)
		}
		second, err := factory.NewRuntime(context.Background())
		if err != nil {
			t.Fatalf("%s second runtime: %v", descriptor.ID, err)
		}
		if first.Store == second.Store || first.Retriever == second.Retriever {
			t.Fatalf("%s reused case runtime", descriptor.ID)
		}
	}
}

func TestBuiltinArmFactoryRejectsUnknownID(t *testing.T) {
	if _, err := BuiltinArmFactory("not-an-arm"); err == nil {
		t.Fatal("unknown arm error = nil")
	}
}
