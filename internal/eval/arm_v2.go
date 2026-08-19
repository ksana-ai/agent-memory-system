package eval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/kai443/go-agent-memory-system/internal/app"
	"github.com/kai443/go-agent-memory-system/internal/domain"
	"github.com/kai443/go-agent-memory-system/internal/retrieval"
	"github.com/kai443/go-agent-memory-system/internal/store"
	"github.com/kai443/go-agent-memory-system/internal/store/memstore"
)

const (
	ArmNoMemoryV1          = "no-memory-v1"
	ArmReviewedCardsBM25V1 = "reviewed-cards-bm25-v1"
)

// ArmDescriptor is the stable, machine-readable identity of a retrieval arm.
// ConfigHash prevents two materially different configurations from being
// reported under the same arm ID and version.
type ArmDescriptor struct {
	ID              string            `json:"id"`
	Version         string            `json:"version"`
	JudgmentProfile string            `json:"judgment_profile"`
	ResultKind      string            `json:"result_kind"`
	ConfigHash      string            `json:"config_hash"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

// ArmRuntime is deliberately constructed for one case only. The runner never
// shares it across a case or arm, which makes evaluation order irrelevant and
// prevents fixture state leaking between cases.
type ArmRuntime struct {
	Store     store.Store
	Retriever app.Retriever
	Cleanup   func(context.Context) error
}

// ArmFactory constructs an isolated retrieval runtime for a single case.
type ArmFactory interface {
	Descriptor() ArmDescriptor
	NewRuntime(context.Context) (ArmRuntime, error)
}

type armFactory struct {
	descriptor ArmDescriptor
	newRuntime func(context.Context) (ArmRuntime, error)
}

func (factory armFactory) Descriptor() ArmDescriptor {
	descriptor := factory.descriptor
	descriptor.Metadata = maps.Clone(factory.descriptor.Metadata)
	return descriptor
}

func (factory armFactory) NewRuntime(ctx context.Context) (ArmRuntime, error) {
	if err := ctx.Err(); err != nil {
		return ArmRuntime{}, err
	}
	runtime, err := factory.newRuntime(ctx)
	if err != nil {
		return ArmRuntime{}, err
	}
	if runtime.Store == nil || runtime.Retriever == nil {
		return ArmRuntime{}, fmt.Errorf("arm %q constructed an incomplete runtime", factory.descriptor.ID)
	}
	return runtime, nil
}

// BuiltinArmFactories returns the deterministic v2 baselines in stable order.
func BuiltinArmFactories() []ArmFactory {
	return []ArmFactory{newNoMemoryArm(), newReviewedCardsBM25Arm()}
}

func BuiltinArmFactory(id string) (ArmFactory, error) {
	for _, factory := range BuiltinArmFactories() {
		if factory.Descriptor().ID == id {
			return factory, nil
		}
	}
	return nil, fmt.Errorf("unknown evaluation arm %q", id)
}

func newNoMemoryArm() ArmFactory {
	config := struct {
		Storage   string `json:"storage"`
		Retriever string `json:"retriever"`
	}{Storage: "memstore", Retriever: "empty-v1"}
	return armFactory{
		descriptor: ArmDescriptor{
			ID:              ArmNoMemoryV1,
			Version:         "1",
			JudgmentProfile: "reviewed-memory-alias-v1",
			ResultKind:      "memory-card",
			ConfigHash:      hashArmConfig(config),
		},
		newRuntime: func(context.Context) (ArmRuntime, error) {
			return ArmRuntime{Store: memstore.New(), Retriever: &emptyRetriever{marker: 1}}, nil
		},
	}
}

func newReviewedCardsBM25Arm() ArmFactory {
	config := struct {
		Storage   string  `json:"storage"`
		Retriever string  `json:"retriever"`
		K1        float64 `json:"k1"`
		B         float64 `json:"b"`
	}{Storage: "memstore", Retriever: "memory-card-bm25-v1", K1: 1.2, B: 0.75}
	return armFactory{
		descriptor: ArmDescriptor{
			ID:              ArmReviewedCardsBM25V1,
			Version:         "1",
			JudgmentProfile: "reviewed-memory-alias-v1",
			ResultKind:      "memory-card",
			ConfigHash:      hashArmConfig(config),
		},
		newRuntime: func(context.Context) (ArmRuntime, error) {
			storage := memstore.New()
			retriever, err := retrieval.NewBM25(storage)
			if err != nil {
				return ArmRuntime{}, err
			}
			return ArmRuntime{Store: storage, Retriever: retriever}, nil
		},
	}
}

type emptyRetriever struct{ marker byte }

func (*emptyRetriever) Search(context.Context, string, string, string, int, time.Time) ([]domain.SearchHit, error) {
	return []domain.SearchHit{}, nil
}

func hashArmConfig(config any) string {
	encoded, err := json.Marshal(config)
	if err != nil {
		panic(fmt.Sprintf("marshal static arm config: %v", err))
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func validateArmFactories(factories []ArmFactory) error {
	if len(factories) == 0 {
		return fmt.Errorf("at least one evaluation arm is required")
	}
	ids := make([]string, 0, len(factories))
	seen := make(map[string]struct{}, len(factories))
	for _, factory := range factories {
		if factory == nil {
			return fmt.Errorf("evaluation arm factory must not be nil")
		}
		descriptor := factory.Descriptor()
		if descriptor.ID == "" || descriptor.Version == "" || descriptor.JudgmentProfile == "" || descriptor.ResultKind == "" || descriptor.ConfigHash == "" {
			return fmt.Errorf("evaluation arm descriptor is incomplete: %#v", descriptor)
		}
		if _, exists := seen[descriptor.ID]; exists {
			return fmt.Errorf("duplicate evaluation arm %q", descriptor.ID)
		}
		seen[descriptor.ID] = struct{}{}
		ids = append(ids, descriptor.ID)
	}
	if !sort.StringsAreSorted(ids) {
		// Caller order is output order; accepting arbitrary order makes manifests
		// harder to diff and can hide accidental changes to the requested arms.
		return fmt.Errorf("evaluation arms must be sorted by id")
	}
	return nil
}
