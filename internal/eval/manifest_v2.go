package eval

import "time"

const ManifestSchemaVersionV2 = "2"

type ManifestV2 struct {
	SchemaVersion string            `json:"schema_version"`
	GeneratedAt   time.Time         `json:"generated_at"`
	Source        SourceProofV2     `json:"source"`
	Acceptance    AcceptanceV2      `json:"acceptance"`
	Dataset       DatasetMetadataV2 `json:"dataset"`
	System        SystemMetadataV2  `json:"system"`
	Arms          []ArmResultV2     `json:"arms"`
}

type AcceptanceV2 struct {
	PolicyPassRequired bool `json:"policy_pass_required"`
	PolicyPassVerified bool `json:"policy_pass_verified"`
}

type DatasetMetadataV2 struct {
	ID      string `json:"id"`
	Version string `json:"version"`
	SHA256  string `json:"sha256"`
	Cases   int    `json:"cases"`
	Queries int    `json:"queries"`
}

type SystemMetadataV2 struct {
	RunnerVersion           string `json:"runner_version"`
	RecallK                 int    `json:"recall_k"`
	NDCGK                   int    `json:"ndcg_k"`
	RetrievalDepth          int    `json:"retrieval_depth"`
	WarmupRuns              int    `json:"warmup_runs"`
	MeasuredRuns            int    `json:"measured_runs"`
	QueryTimeoutNanoseconds int64  `json:"query_timeout_nanoseconds"`
	ConfigHash              string `json:"config_hash"`
}

type ArmResultV2 struct {
	Descriptor ArmDescriptor   `json:"descriptor"`
	Aggregate  ArmAggregateV2  `json:"aggregate"`
	Queries    []QueryResultV2 `json:"queries"`
}

type ArmAggregateV2 struct {
	QueryCount                int     `json:"query_count"`
	QualityQueryCount         int     `json:"quality_query_count"`
	NonRecallQueryCount       int     `json:"non_recall_query_count"`
	RecallAtK                 float64 `json:"recall_at_k"`
	MRR                       float64 `json:"mrr"`
	NDCGAtK                   float64 `json:"ndcg_at_k"`
	PassRate                  float64 `json:"pass_rate"`
	LatencyP50Nanoseconds     int64   `json:"latency_p50_nanoseconds"`
	LatencyP95Nanoseconds     int64   `json:"latency_p95_nanoseconds"`
	LatencyMaxNanoseconds     int64   `json:"latency_max_nanoseconds"`
	LatencySampleCount        int     `json:"latency_sample_count"`
	ForbiddenHits             int     `json:"forbidden_hits"`
	RequireEmptyFailures      int     `json:"require_empty_failures"`
	ScopeViolations           int     `json:"scope_violations"`
	NonActiveHits             int     `json:"nonactive_hits"`
	ExpiredHits               int     `json:"expired_hits"`
	UnknownHits               int     `json:"unknown_hits"`
	DuplicateHits             int     `json:"duplicate_hits"`
	OverLimitHits             int     `json:"over_limit_hits"`
	UnknownSourceIDs          int     `json:"unknown_source_ids"`
	MissingSources            int     `json:"missing_sources"`
	ReorderedSources          int     `json:"reordered_sources"`
	SourceScopeViolations     int     `json:"source_scope_violations"`
	MemoryPayloadViolations   int     `json:"memory_payload_violations"`
	EvidencePayloadViolations int     `json:"evidence_payload_violations"`
	QueryExecutionFailures    int     `json:"query_execution_failures"`
	PolicyPassed              bool    `json:"policy_passed"`
	PolicyPassRate            float64 `json:"policy_pass_rate"`
	NonRecallPassRate         float64 `json:"non_recall_pass_rate"`
	QualityResultSHA256       string  `json:"quality_result_sha256"`
}

type QueryResultV2 struct {
	CaseID               string          `json:"case_id"`
	QueryID              string          `json:"query_id"`
	Scope                string          `json:"scope"`
	RetrievalDepth       int             `json:"retrieval_depth"`
	Hits                 []HitResultV2   `json:"hits"`
	DurationsNanoseconds []int64         `json:"durations_nanoseconds"`
	Quality              *QueryQualityV2 `json:"quality,omitempty"`
	Policy               QueryPolicyV2   `json:"policy"`
	ExecutionError       string          `json:"execution_error,omitempty"`
}

type HitResultV2 struct {
	Rank                int      `json:"rank"`
	Alias               string   `json:"alias,omitempty"`
	MemoryID            string   `json:"memory_id"`
	TenantID            string   `json:"tenant_id"`
	UserID              string   `json:"user_id"`
	Status              string   `json:"status"`
	Score               float64  `json:"score"`
	SourceAliases       []string `json:"source_aliases,omitempty"`
	SourcePayloadSHA256 []string `json:"source_payload_sha256,omitempty"`
	PayloadSHA256       string   `json:"payload_sha256"`
}

type QueryQualityV2 struct {
	RelevantCount int     `json:"relevant_count"`
	RecallAtK     float64 `json:"recall_at_k"`
	MRR           float64 `json:"mrr"`
	NDCGAtK       float64 `json:"ndcg_at_k"`
	Passed        bool    `json:"passed"`
}

type QueryPolicyV2 struct {
	ForbiddenHits             int  `json:"forbidden_hits"`
	RequireEmptyFailure       bool `json:"require_empty_failure"`
	ScopeViolations           int  `json:"scope_violations"`
	NonActiveHits             int  `json:"nonactive_hits"`
	ExpiredHits               int  `json:"expired_hits"`
	UnknownHits               int  `json:"unknown_hits"`
	DuplicateHits             int  `json:"duplicate_hits"`
	OverLimitHits             int  `json:"over_limit_hits"`
	UnknownSourceIDs          int  `json:"unknown_source_ids"`
	MissingSources            int  `json:"missing_sources"`
	ReorderedSources          int  `json:"reordered_sources"`
	SourceScopeViolations     int  `json:"source_scope_violations"`
	MemoryPayloadViolations   int  `json:"memory_payload_violations"`
	EvidencePayloadViolations int  `json:"evidence_payload_violations"`
	Passed                    bool `json:"passed"`
}
