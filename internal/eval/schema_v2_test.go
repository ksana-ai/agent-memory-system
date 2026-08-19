package eval

import (
	"strings"
	"testing"
)

const validDatasetV2 = `{
  "schema_version":"2",
  "id":"lifecycle-v2",
  "version":"2.0.0",
  "description":"strict multi-scope lifecycle fixture",
  "cases":[{
    "id":"update-delete",
    "description":"compact remember, update, scope control, and deletion",
    "tags":["multi_session","deletion","cross_tenant"],
    "scopes":[
      {"id":"subject","tenant_id":"tenant-a","user_id":"user-a"},
      {"id":"other","tenant_id":"tenant-b","user_id":"user-a"}
    ],
    "timeline":[
      {
        "op":"memory.remember","memory_ref":"mem_old","scope":"subject",
        "at":"2026-08-01T10:01:00Z","review_state":"approved",
        "memory":{"kind":"semantic","category":"travel","key":"seat_preference","value":"aisle","person":"self","relationship":"self","backstory":"earlier preference"},
        "evidence":[{"alias":"evt_old","session_id":"session-1","actor":"user","content":"I prefer aisle seats.","occurred_at":"2026-08-01T10:00:00Z"}]
      },
      {
        "op":"memory.remember","memory_ref":"mem_other","scope":"other",
        "at":"2026-08-01T10:02:00Z","review_state":"approved",
        "memory":{"kind":"semantic","category":"travel","key":"seat_preference","value":"middle","person":"self","relationship":"self"},
        "evidence":[{"alias":"evt_other","session_id":"session-other","actor":"user","content":"I prefer middle seats.","occurred_at":"2026-08-01T10:01:30Z"}]
      },
      {
        "op":"query","id":"before-update","scope":"subject","at":"2026-08-01T10:03:00Z","text":"preferred seat",
        "judgments":{
          "memory_cards":{"relevance":{"mem_old":3},"forbidden":["mem_other"]},
          "evidence_events":{"relevance":{"evt_old":3},"forbidden":["evt_other"]}
        }
      },
      {
        "op":"memory.remember","memory_ref":"mem_new","scope":"subject",
        "at":"2026-08-02T10:01:00Z","review_state":"approved",
        "memory":{"kind":"semantic","category":"travel","key":"seat_preference","value":"window","person":"self","relationship":"self"},
        "evidence":[{"alias":"evt_new","session_id":"session-2","actor":"user","content":"I now prefer window seats.","occurred_at":"2026-08-02T10:00:00Z"}]
      },
      {
        "op":"query","id":"after-update","scope":"subject","at":"2026-08-02T10:02:00Z","text":"preferred seat",
        "judgments":{"memory_cards":{"relevance":{"mem_new":3},"forbidden":["mem_old","mem_other"]}}
      },
      {"op":"forget_user","scope":"subject","at":"2026-08-03T10:00:00Z"},
      {
        "op":"query","id":"after-delete","scope":"subject","at":"2026-08-03T10:01:00Z","text":"preferred seat",
        "judgments":{
          "memory_cards":{"forbidden":["mem_old","mem_new"],"require_empty":true},
          "evidence_events":{"forbidden":["evt_old","evt_new"],"require_empty":true}
        }
      }
    ]
  }]
}`

const validDetailedDatasetV2 = `{
  "schema_version":"2",
  "id":"detailed-v2",
  "version":"2.0.0",
  "description":"detailed lifecycle fixture",
  "cases":[{
    "id":"detailed",
    "scopes":[{"id":"subject","tenant_id":"tenant-a","user_id":"user-a"}],
    "timeline":[
      {"op":"evidence.append","as":"evt_detail","scope":"subject","session_id":"session-1","at":"2026-08-01T10:00:00Z","actor":"user","content":"I like tea."},
      {"op":"candidate.propose","as":"cand_detail","scope":"subject","at":"2026-08-01T10:01:00Z","source_event_ids":["evt_detail"],"memory":{"kind":"semantic","category":"food","key":"drink","value":"tea"},"extractor":"fixture","extractor_version":"v2"},
      {"op":"candidate.review","candidate":"cand_detail","scope":"subject","at":"2026-08-01T10:02:00Z","decision":"approve","memory_as":"mem_detail","reviewer_id":"reviewer","reason":"supported"},
      {"op":"query","id":"detail-query","scope":"subject","at":"2026-08-01T10:03:00Z","text":"preferred drink","judgments":{"memory_cards":{"relevance":{"mem_detail":3}},"evidence_events":{"relevance":{"evt_detail":2}}}}
    ]
  }]
}`

func TestLoadV2AcceptsCompactMultiScopeLifecycle(t *testing.T) {
	dataset, err := LoadV2([]byte(validDatasetV2))
	if err != nil {
		t.Fatalf("LoadV2(): %v", err)
	}
	if dataset.SchemaVersion != "2" || dataset.ID != "lifecycle-v2" || len(dataset.Cases) != 1 {
		t.Fatalf("unexpected dataset: %#v", dataset)
	}
	if len(dataset.SHA256) != 64 {
		t.Fatalf("SHA256 length = %d, want 64", len(dataset.SHA256))
	}
	if _, ok := dataset.Cases[0].Timeline[0].(*MemoryRememberV2); !ok {
		t.Fatalf("timeline[0] type = %T, want *MemoryRememberV2", dataset.Cases[0].Timeline[0])
	}
	if _, ok := dataset.Cases[0].Timeline[5].(*ForgetUserV2); !ok {
		t.Fatalf("timeline[5] type = %T, want *ForgetUserV2", dataset.Cases[0].Timeline[5])
	}
	if err := dataset.VerifyIntegrity(); err != nil {
		t.Fatalf("VerifyIntegrity(): %v", err)
	}
}

func TestLoadV2AcceptsDetailedLifecycleOperations(t *testing.T) {
	dataset, err := LoadV2([]byte(validDetailedDatasetV2))
	if err != nil {
		t.Fatalf("LoadV2(): %v", err)
	}
	wantTypes := []any{
		(*EvidenceAppendV2)(nil),
		(*CandidateProposeV2)(nil),
		(*CandidateReviewV2)(nil),
		(*QueryV2)(nil),
	}
	for index, want := range wantTypes {
		switch want.(type) {
		case *EvidenceAppendV2:
			if _, ok := dataset.Cases[0].Timeline[index].(*EvidenceAppendV2); !ok {
				t.Fatalf("timeline[%d] type = %T", index, dataset.Cases[0].Timeline[index])
			}
		case *CandidateProposeV2:
			if _, ok := dataset.Cases[0].Timeline[index].(*CandidateProposeV2); !ok {
				t.Fatalf("timeline[%d] type = %T", index, dataset.Cases[0].Timeline[index])
			}
		case *CandidateReviewV2:
			if _, ok := dataset.Cases[0].Timeline[index].(*CandidateReviewV2); !ok {
				t.Fatalf("timeline[%d] type = %T", index, dataset.Cases[0].Timeline[index])
			}
		case *QueryV2:
			if _, ok := dataset.Cases[0].Timeline[index].(*QueryV2); !ok {
				t.Fatalf("timeline[%d] type = %T", index, dataset.Cases[0].Timeline[index])
			}
		}
	}
}

func TestLoadV2RejectsUnknownFieldsAtEveryLayer(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{name: "dataset", data: strings.Replace(validDetailedDatasetV2, `"description":"detailed lifecycle fixture",`, `"description":"detailed lifecycle fixture","unknown":true,`, 1)},
		{name: "case", data: strings.Replace(validDetailedDatasetV2, `"id":"detailed",`, `"id":"detailed","unknown":true,`, 1)},
		{name: "operation", data: strings.Replace(validDetailedDatasetV2, `"content":"I like tea."`, `"content":"I like tea.","unknown":true`, 1)},
		{name: "memory", data: strings.Replace(validDetailedDatasetV2, `"value":"tea"`, `"value":"tea","unknown":true`, 1)},
		{name: "judgment", data: strings.Replace(validDetailedDatasetV2, `"relevance":{"mem_detail":3}`, `"relevance":{"mem_detail":3},"unknown":true`, 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := LoadV2([]byte(test.data)); err == nil {
				t.Fatal("LoadV2() error = nil, want unknown-field error")
			}
		})
	}
}

func TestLoadV2RejectsInvalidReferencesTimeAndJudgments(t *testing.T) {
	tests := []struct {
		name string
		data string
		want string
	}{
		{
			name: "reference before use",
			data: strings.Replace(validDetailedDatasetV2, `"source_event_ids":["evt_detail"]`, `"source_event_ids":["missing"]`, 1),
			want: "referenced before use",
		},
		{
			name: "decreasing timeline time",
			data: strings.Replace(validDetailedDatasetV2, `"at":"2026-08-01T10:03:00Z","text"`, `"at":"2026-08-01T09:59:00Z","text"`, 1),
			want: "precedes",
		},
		{
			name: "relevance below scale",
			data: strings.Replace(validDetailedDatasetV2, `"mem_detail":3`, `"mem_detail":0`, 1),
			want: "want 1..3",
		},
		{
			name: "relevance above scale",
			data: strings.Replace(validDetailedDatasetV2, `"mem_detail":3`, `"mem_detail":4`, 1),
			want: "want 1..3",
		},
		{
			name: "require empty with relevance",
			data: strings.Replace(validDetailedDatasetV2, `"relevance":{"mem_detail":3}`, `"relevance":{"mem_detail":3},"require_empty":true`, 1),
			want: "cannot be combined",
		},
		{
			name: "relevant and forbidden overlap",
			data: strings.Replace(validDetailedDatasetV2, `"relevance":{"mem_detail":3}`, `"relevance":{"mem_detail":3},"forbidden":["mem_detail"]`, 1),
			want: "both relevant and forbidden",
		},
		{
			name: "memory card profile is required",
			data: strings.Replace(validDetailedDatasetV2, `"memory_cards":{"relevance":{"mem_detail":3}},`, ``, 1),
			want: "needs a memory_cards",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := LoadV2([]byte(test.data))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("LoadV2() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestLoadV2RejectsPositiveGoldForWrongScopeOrInactiveMemory(t *testing.T) {
	tests := []struct {
		name string
		data string
		want string
	}{
		{
			name: "other scope is not positive gold",
			data: strings.Replace(validDatasetV2, `"relevance":{"mem_old":3},"forbidden":["mem_other"]`, `"relevance":{"mem_other":3},"forbidden":["mem_old"]`, 1),
			want: "not query scope",
		},
		{
			name: "superseded memory is not positive gold",
			data: strings.Replace(validDatasetV2, `"relevance":{"mem_new":3},"forbidden":["mem_old","mem_other"]`, `"relevance":{"mem_old":3},"forbidden":["mem_new","mem_other"]`, 1),
			want: "not an active memory",
		},
		{
			name: "deleted memory is not positive gold",
			data: strings.Replace(validDatasetV2, `"memory_cards":{"forbidden":["mem_old","mem_new"],"require_empty":true}`, `"memory_cards":{"relevance":{"mem_new":3}}`, 1),
			want: "was deleted",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := LoadV2([]byte(test.data))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("LoadV2() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestLoadV2RejectsInvalidCompactRemember(t *testing.T) {
	tests := []struct {
		name string
		data string
		want string
	}{
		{
			name: "no evidence",
			data: strings.Replace(validDatasetV2, `"evidence":[{"alias":"evt_old","session_id":"session-1","actor":"user","content":"I prefer aisle seats.","occurred_at":"2026-08-01T10:00:00Z"}]`, `"evidence":[]`, 1),
			want: "at least one evidence",
		},
		{
			name: "evidence after remember",
			data: strings.Replace(validDatasetV2, `"occurred_at":"2026-08-01T10:00:00Z"`, `"occurred_at":"2026-08-01T10:02:00Z"`, 1),
			want: "is after remember",
		},
		{
			name: "invalid review state",
			data: strings.Replace(validDatasetV2, `"review_state":"approved"`, `"review_state":"unknown"`, 1),
			want: "approved, rejected, or pending",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := LoadV2([]byte(test.data))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("LoadV2() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestLoadV2AcceptsPendingAndRejectedRememberAsForbiddenOnly(t *testing.T) {
	for _, reviewState := range []string{"pending", "rejected"} {
		t.Run(reviewState, func(t *testing.T) {
			data := strings.Replace(validDatasetV2, `"review_state":"approved"`, `"review_state":"`+reviewState+`"`, 1)
			data = strings.Replace(
				data,
				`"memory_cards":{"relevance":{"mem_old":3},"forbidden":["mem_other"]}`,
				`"memory_cards":{"forbidden":["mem_old","mem_other"],"require_empty":true}`,
				1,
			)
			if _, err := LoadV2([]byte(data)); err != nil {
				t.Fatalf("LoadV2() %s remember: %v", reviewState, err)
			}
		})
	}
}

func TestLoadV2RejectsPendingRememberAsPositiveGold(t *testing.T) {
	data := strings.Replace(validDatasetV2, `"review_state":"approved"`, `"review_state":"pending"`, 1)
	_, err := LoadV2([]byte(data))
	if err == nil || !strings.Contains(err.Error(), "not an active memory") {
		t.Fatalf("LoadV2() error = %v, want inactive-memory error", err)
	}
}

func TestDatasetV2VerifyIntegrityRejectsSemanticAndDigestMutation(t *testing.T) {
	dataset, err := LoadV2([]byte(validDatasetV2))
	if err != nil {
		t.Fatalf("LoadV2(): %v", err)
	}
	query := dataset.Cases[0].Timeline[2].(*QueryV2)
	query.Text = "mutated query"
	if err := dataset.VerifyIntegrity(); err == nil || !strings.Contains(err.Error(), "changed after loading") {
		t.Fatalf("VerifyIntegrity() error = %v, want mutation error", err)
	}

	dataset, err = LoadV2([]byte(validDatasetV2))
	if err != nil {
		t.Fatalf("LoadV2() again: %v", err)
	}
	dataset.SHA256 = strings.Repeat("0", 64)
	if err := dataset.VerifyIntegrity(); err == nil || !strings.Contains(err.Error(), "changed after loading") {
		t.Fatalf("VerifyIntegrity() digest error = %v, want mutation error", err)
	}
}

func TestDatasetV2VerifyIntegrityRequiresLoadV2(t *testing.T) {
	loaded, err := LoadV2([]byte(validDetailedDatasetV2))
	if err != nil {
		t.Fatalf("LoadV2(): %v", err)
	}
	loaded.fingerprint = ""
	if err := loaded.VerifyIntegrity(); err == nil || !strings.Contains(err.Error(), "LoadV2") {
		t.Fatalf("VerifyIntegrity() error = %v, want loader requirement", err)
	}
}
