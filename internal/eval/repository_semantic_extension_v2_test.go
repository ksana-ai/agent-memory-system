package eval

import (
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

type semanticExtensionMemoryState struct {
	alias     string
	scope     string
	memory    MemorySpecV2
	createdAt time.Time
	active    bool
	actors    map[string]struct{}
	sessions  map[string]struct{}
}

func TestRepositorySemanticExtensionDatasetContract(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(filename), "..", "..", "datasets", "memory-semantic-extension-v1.json"))
	if err != nil {
		t.Fatalf("read repository semantic extension: %v", err)
	}
	dataset, err := LoadV2(data)
	if err != nil {
		t.Fatalf("strict-load repository semantic extension: %v", err)
	}
	if dataset.ID != "memory-semantic-extension-v1" || dataset.Version != "1.0.0" || len(dataset.Cases) != 30 || countQueriesV2(dataset) != 30 {
		t.Fatalf("unexpected semantic extension metadata: id=%q version=%q cases=%d queries=%d", dataset.ID, dataset.Version, len(dataset.Cases), countQueriesV2(dataset))
	}
	const wantDatasetSHA256 = "36b14f86c62c2636ff551df9090b31cdd63918613f47fa87aa8015088922f82c"
	if dataset.SHA256 != wantDatasetSHA256 {
		t.Fatalf("semantic extension SHA256=%s, want frozen %s", dataset.SHA256, wantDatasetSHA256)
	}

	familyTags := map[string]int{}
	languageTags := map[string]int{}
	primaryTags := map[string]int{}
	relevanceHistogram := map[int]int{}
	caseIDs := map[string]struct{}{}
	globalAliases := map[string]string{}
	queryIDs := map[string]string{}
	var searchable []struct {
		caseID string
		text   string
	}
	qualityQueries := 0
	policyQueries := 0
	requireEmptyCases := []string{}
	multiSourceTarget := false
	expirationCases := 0
	minQualityCorpus := 1 << 30
	minHardNegatives := 1 << 30
	minPolicyDistractors := 1 << 30

	for _, testCase := range dataset.Cases {
		if _, exists := caseIDs[testCase.ID]; exists {
			t.Fatalf("duplicate case id %q", testCase.ID)
		}
		caseIDs[testCase.ID] = struct{}{}

		if !hasTagV2(testCase.Tags, "semantic_extension") {
			t.Errorf("case %s is missing semantic_extension tag", testCase.ID)
		}
		familyCount := countKnownTagsV2(testCase.Tags, familyTags, []string{
			"direct", "multi_session_entity", "update_conflict", "language_hard", "lifecycle_non_recall", "scope_adversarial",
		})
		languageCount := countKnownTagsV2(testCase.Tags, languageTags, []string{"lang_en", "lang_zh", "lang_mixed"})
		primaryCount := countKnownTagsV2(testCase.Tags, primaryTags, []string{"primary_semantic", "primary_episodic", "primary_procedural"})
		if familyCount != 1 || languageCount != 1 || primaryCount != 1 {
			t.Errorf("case %s tag dimensions: family=%d language=%d primary=%d, want 1 each", testCase.ID, familyCount, languageCount, primaryCount)
		}
		if hasTagV2(testCase.Tags, "expiration") {
			expirationCases++
		}

		states := map[string]*semanticExtensionMemoryState{}
		activeByIdentity := map[string]string{}
		var firstMemory *MemorySpecV2
		var query *QueryV2
		for _, operation := range testCase.Timeline {
			switch value := operation.(type) {
			case *MemoryRememberV2:
				if firstMemory == nil {
					copy := value.Memory
					firstMemory = &copy
				}
				claimGlobalAliasV2(t, globalAliases, value.MemoryRef, testCase.ID)
				actors := map[string]struct{}{}
				sessions := map[string]struct{}{}
				for _, evidence := range value.Evidence {
					claimGlobalAliasV2(t, globalAliases, evidence.Alias, testCase.ID)
					actors[string(evidence.Actor)] = struct{}{}
					sessions[evidence.SessionID] = struct{}{}
					searchable = append(searchable, struct {
						caseID string
						text   string
					}{testCase.ID, evidence.Content})
				}
				state := &semanticExtensionMemoryState{
					alias: value.MemoryRef, scope: value.Scope, memory: value.Memory, createdAt: value.At, actors: actors, sessions: sessions,
				}
				states[value.MemoryRef] = state
				appendMemorySearchTextV2(&searchable, testCase.ID, value.Memory)
				if value.ReviewState == RememberApprovedV2 {
					identityKey := memoryIdentityKeyV2(value.Scope, normalizedMemoryIdentityV2(value.Memory))
					if previous := activeByIdentity[identityKey]; previous != "" {
						states[previous].active = false
					}
					state.active = true
					activeByIdentity[identityKey] = value.MemoryRef
				}

			case *ForgetUserV2:
				for _, state := range states {
					if state.scope == value.Scope {
						state.active = false
					}
				}
				for identity, alias := range activeByIdentity {
					if states[alias].scope == value.Scope {
						delete(activeByIdentity, identity)
					}
				}

			case *QueryV2:
				if query != nil {
					t.Fatalf("case %s has more than one query", testCase.ID)
				}
				query = value
				searchable = append(searchable, struct {
					caseID string
					text   string
				}{testCase.ID, value.Text})
				if previous, exists := queryIDs[value.ID]; exists {
					t.Fatalf("query id %q appears in cases %s and %s", value.ID, previous, testCase.ID)
				}
				queryIDs[value.ID] = testCase.ID

			default:
				t.Fatalf("case %s uses non-compact operation %T; the extension contract expects memory.remember, forget_user, and query only", testCase.ID, operation)
			}
		}
		if query == nil || query.Judgments.MemoryCards == nil {
			t.Fatalf("case %s has no memory-card query", testCase.ID)
		}
		if firstMemory == nil {
			t.Fatalf("case %s has no authored memory", testCase.ID)
		}
		wantPrimary := "primary_" + string(firstMemory.Kind)
		if !hasTagV2(testCase.Tags, wantPrimary) {
			t.Errorf("case %s primary tag does not match its first scenario memory kind %s", testCase.ID, firstMemory.Kind)
		}

		profile := query.Judgments.MemoryCards
		relevantCount := len(profile.Relevance)
		relevanceHistogram[relevantCount]++
		active := activeSemanticExtensionMemories(states, query.Scope, query.At)
		for _, forbidden := range profile.Forbidden {
			if active[forbidden] != nil {
				t.Errorf("case %s forbidden alias %s remains active in the query scope", testCase.ID, forbidden)
			}
			if hasTagV2(testCase.Tags, "scope_adversarial") {
				state := states[forbidden]
				if state == nil || !state.active || state.scope == query.Scope ||
					(state.memory.ExpiresAt != nil && !query.At.Before(*state.memory.ExpiresAt)) {
					t.Errorf("scope-adversarial case %s forbidden alias %s is not an active serviceable foreign-scope memory", testCase.ID, forbidden)
				}
			}
		}
		if hasTagV2(testCase.Tags, "expiration") {
			expiredForbidden := false
			for _, forbidden := range profile.Forbidden {
				state := states[forbidden]
				if state != nil && state.memory.ExpiresAt != nil && !query.At.Before(*state.memory.ExpiresAt) {
					expiredForbidden = true
				}
			}
			if !expiredForbidden {
				t.Errorf("expiration-tagged case %s has no genuinely expired forbidden memory at query time", testCase.ID)
			}
		}
		if relevantCount == 0 {
			policyQueries++
			if len(profile.Forbidden) == 0 {
				t.Errorf("policy-only case %s has no forbidden assertion", testCase.ID)
			}
			if profile.RequireEmpty {
				requireEmptyCases = append(requireEmptyCases, testCase.ID)
				if len(active) != 0 {
					t.Errorf("require-empty case %s has %d active query-scope memories", testCase.ID, len(active))
				}
			} else {
				if len(active) < minPolicyDistractors {
					minPolicyDistractors = len(active)
				}
				if len(active) < 10 {
					t.Errorf("non-vacuous policy case %s has %d legal active distractors, want at least 10", testCase.ID, len(active))
				}
			}
			continue
		}

		qualityQueries++
		if len(active) < minQualityCorpus {
			minQualityCorpus = len(active)
		}
		if len(active) < 12 {
			t.Errorf("quality case %s has %d active serviceable query-scope memories, want at least 12", testCase.ID, len(active))
		}
		grades := make([]int, 0, relevantCount)
		var target *semanticExtensionMemoryState
		for alias, grade := range profile.Relevance {
			grades = append(grades, grade)
			state := active[alias]
			if state == nil {
				t.Errorf("quality case %s relevant alias %s is not active at query time", testCase.ID, alias)
				continue
			}
			if grade == 3 {
				target = state
			}
		}
		sort.Ints(grades)
		wantGrades := map[int][]int{1: {3}, 2: {1, 3}, 3: {1, 2, 3}}[relevantCount]
		if !equalIntsV2(grades, wantGrades) {
			t.Errorf("quality case %s relevance grades=%v, want %v", testCase.ID, grades, wantGrades)
		}
		if target == nil {
			t.Errorf("quality case %s has no grade-3 primary relevant memory", testCase.ID)
			continue
		}
		wantPrimary = "primary_" + string(target.memory.Kind)
		if !hasTagV2(testCase.Tags, wantPrimary) {
			t.Errorf("quality case %s primary tag does not match grade-3 memory kind %s", testCase.ID, target.memory.Kind)
		}
		hardNegatives := 0
		for alias, state := range active {
			if _, relevant := profile.Relevance[alias]; relevant {
				continue
			}
			if sameHardNegativeGroupV2(target.memory, state.memory) &&
				!equalFoldTrimV2(target.memory.Key, state.memory.Key) &&
				!equalFoldTrimV2(target.memory.Value, state.memory.Value) &&
				!target.createdAt.Equal(state.createdAt) {
				hardNegatives++
			}
		}
		if hardNegatives < 5 {
			t.Errorf("quality case %s has %d strict same-entity/category/relationship hard negatives, want at least 5", testCase.ID, hardNegatives)
		}
		if hardNegatives < minHardNegatives {
			minHardNegatives = hardNegatives
		}
		if _, hasUser := target.actors["user"]; hasUser {
			_, hasAgent := target.actors["agent"]
			_, hasTool := target.actors["tool"]
			multiSourceTarget = multiSourceTarget || (hasAgent && hasTool)
		}
		if hasTagV2(testCase.Tags, "multi_session_entity") && len(target.sessions) < 2 {
			t.Errorf("multi-session case %s grade-3 target spans %d source sessions, want at least 2", testCase.ID, len(target.sessions))
		}
	}

	assertExactCountsV2(t, "family tags", familyTags, map[string]int{
		"direct": 6, "multi_session_entity": 6, "update_conflict": 6,
		"language_hard": 4, "lifecycle_non_recall": 4, "scope_adversarial": 4,
	})
	assertExactCountsV2(t, "language tags", languageTags, map[string]int{"lang_en": 10, "lang_zh": 10, "lang_mixed": 10})
	assertExactCountsV2(t, "primary kind tags", primaryTags, map[string]int{"primary_semantic": 10, "primary_episodic": 10, "primary_procedural": 10})
	assertExactCountsV2(t, "relevance histogram", relevanceHistogram, map[int]int{0: 6, 1: 12, 2: 8, 3: 4})
	if qualityQueries != 24 || policyQueries != 6 {
		t.Errorf("query split quality=%d policy=%d, want 24/6", qualityQueries, policyQueries)
	}
	if len(requireEmptyCases) != 1 || requireEmptyCases[0] != "k01" {
		t.Errorf("require_empty cases=%v, want [k01]", requireEmptyCases)
	}
	if expirationCases != 1 {
		t.Errorf("expiration-tagged cases=%d, want 1", expirationCases)
	}
	if !multiSourceTarget {
		t.Error("no grade-3 target has user, agent, and tool evidence")
	}
	t.Logf("static corpus minima: quality_active=%d strict_hard_negatives=%d nonempty_policy_distractors=%d", minQualityCorpus, minHardNegatives, minPolicyDistractors)

	wantCaseIDs := []string{
		"g01", "g02", "g03", "g04", "g05", "g06",
		"h01", "h02", "h03", "h04", "h05", "h06",
		"i01", "i02", "i03", "i04", "i05", "i06",
		"j01", "j02", "j03", "j04",
		"k01", "k02", "k03", "k04",
		"l01", "l02", "l03", "l04",
	}
	for _, caseID := range wantCaseIDs {
		if _, exists := caseIDs[caseID]; !exists {
			t.Errorf("missing pre-registered case %s", caseID)
		}
	}

	for alias, aliasCase := range globalAliases {
		for _, field := range searchable {
			if strings.Contains(strings.ToLower(field.text), strings.ToLower(alias)) {
				t.Errorf("opaque alias %q from case %s leaks into searchable text in case %s", alias, aliasCase, field.caseID)
			}
		}
	}
}

func activeSemanticExtensionMemories(states map[string]*semanticExtensionMemoryState, scope string, at time.Time) map[string]*semanticExtensionMemoryState {
	active := map[string]*semanticExtensionMemoryState{}
	for alias, state := range states {
		if !state.active || state.scope != scope {
			continue
		}
		if state.memory.ExpiresAt != nil && !at.Before(*state.memory.ExpiresAt) {
			continue
		}
		active[alias] = state
	}
	return active
}

func sameHardNegativeGroupV2(left, right MemorySpecV2) bool {
	return equalFoldTrimV2(left.Category, right.Category) &&
		equalFoldTrimV2(left.Person, right.Person) &&
		equalFoldTrimV2(left.Relationship, right.Relationship)
}

func equalFoldTrimV2(left, right string) bool {
	return strings.EqualFold(strings.TrimSpace(left), strings.TrimSpace(right))
}

func hasTagV2(tags []string, want string) bool {
	for _, tag := range tags {
		if tag == want {
			return true
		}
	}
	return false
}

func countKnownTagsV2(tags []string, counts map[string]int, known []string) int {
	total := 0
	for _, want := range known {
		if hasTagV2(tags, want) {
			counts[want]++
			total++
		}
	}
	return total
}

func claimGlobalAliasV2(t *testing.T, aliases map[string]string, alias, caseID string) {
	t.Helper()
	if previous, exists := aliases[alias]; exists {
		t.Fatalf("opaque alias %q appears in cases %s and %s", alias, previous, caseID)
	}
	aliases[alias] = caseID
}

func appendMemorySearchTextV2(destination *[]struct {
	caseID string
	text   string
}, caseID string, memory MemorySpecV2) {
	for _, text := range []string{memory.Category, memory.Key, memory.Value, memory.Person, memory.Relationship, memory.Backstory} {
		*destination = append(*destination, struct {
			caseID string
			text   string
		}{caseID, text})
	}
}

func equalIntsV2(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func assertExactCountsV2[K comparable](t *testing.T, name string, got, want map[K]int) {
	t.Helper()
	for key, count := range want {
		if got[key] != count {
			t.Errorf("%s[%v]=%d, want %d", name, key, got[key], count)
		}
	}
	for key, count := range got {
		if _, expected := want[key]; !expected && count != 0 {
			t.Errorf("unexpected %s[%v]=%d", name, key, count)
		}
	}
}
