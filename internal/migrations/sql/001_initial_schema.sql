CREATE EXTENSION IF NOT EXISTS vector;

CREATE SCHEMA IF NOT EXISTS agent_memory;

CREATE TABLE agent_memory.user_scope_state (
    tenant_id text NOT NULL,
    user_id text NOT NULL,
    context_revision bigint NOT NULL DEFAULT 0,
    last_deleted_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, user_id),
    CONSTRAINT user_scope_state_tenant_id_not_blank CHECK (btrim(tenant_id) <> ''),
    CONSTRAINT user_scope_state_user_id_not_blank CHECK (btrim(user_id) <> ''),
    CONSTRAINT user_scope_state_revision_nonnegative CHECK (context_revision >= 0)
);

CREATE TABLE agent_memory.evidence_events (
    tenant_id text NOT NULL,
    user_id text NOT NULL,
    id text NOT NULL,
    session_id text NOT NULL,
    actor text NOT NULL,
    content text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at timestamptz NOT NULL,
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, user_id, id),
    CONSTRAINT evidence_events_scope_fk
        FOREIGN KEY (tenant_id, user_id)
        REFERENCES agent_memory.user_scope_state (tenant_id, user_id),
    CONSTRAINT evidence_events_id_not_blank CHECK (btrim(id) <> ''),
    CONSTRAINT evidence_events_session_id_not_blank CHECK (btrim(session_id) <> ''),
    CONSTRAINT evidence_events_actor_valid CHECK (actor IN ('user', 'agent', 'tool')),
    CONSTRAINT evidence_events_content_not_blank CHECK (btrim(content) <> ''),
    CONSTRAINT evidence_events_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX evidence_events_scope_session_time_idx
    ON agent_memory.evidence_events (tenant_id, user_id, session_id, occurred_at, id);

CREATE TABLE agent_memory.memory_candidates (
    tenant_id text NOT NULL,
    user_id text NOT NULL,
    id text NOT NULL,
    kind text NOT NULL,
    category text NOT NULL,
    memory_key text NOT NULL,
    value text NOT NULL,
    person text NOT NULL DEFAULT '',
    relationship text NOT NULL DEFAULT '',
    backstory text NOT NULL DEFAULT '',
    extractor text NOT NULL,
    extractor_version text NOT NULL,
    status text NOT NULL,
    review_decision text,
    reviewer_id text,
    review_reason text,
    reviewed_at timestamptz,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, user_id, id),
    CONSTRAINT memory_candidates_scope_fk
        FOREIGN KEY (tenant_id, user_id)
        REFERENCES agent_memory.user_scope_state (tenant_id, user_id),
    CONSTRAINT memory_candidates_id_not_blank CHECK (btrim(id) <> ''),
    CONSTRAINT memory_candidates_kind_valid CHECK (kind IN ('episodic', 'semantic', 'procedural')),
    CONSTRAINT memory_candidates_category_not_blank CHECK (btrim(category) <> ''),
    CONSTRAINT memory_candidates_key_not_blank CHECK (btrim(memory_key) <> ''),
    CONSTRAINT memory_candidates_value_not_blank CHECK (btrim(value) <> ''),
    CONSTRAINT memory_candidates_extractor_not_blank CHECK (btrim(extractor) <> ''),
    CONSTRAINT memory_candidates_extractor_version_not_blank CHECK (btrim(extractor_version) <> ''),
    CONSTRAINT memory_candidates_status_valid CHECK (status IN ('pending', 'approved', 'rejected')),
    CONSTRAINT memory_candidates_review_valid CHECK (
        (
            status = 'pending'
            AND review_decision IS NULL
            AND reviewer_id IS NULL
            AND review_reason IS NULL
            AND reviewed_at IS NULL
        )
        OR (
            status = 'approved'
            AND review_decision = 'approve'
            AND reviewer_id IS NOT NULL
            AND btrim(reviewer_id) <> ''
            AND review_reason IS NOT NULL
            AND btrim(review_reason) <> ''
            AND reviewed_at IS NOT NULL
        )
        OR (
            status = 'rejected'
            AND review_decision = 'reject'
            AND reviewer_id IS NOT NULL
            AND btrim(reviewer_id) <> ''
            AND review_reason IS NOT NULL
            AND btrim(review_reason) <> ''
            AND reviewed_at IS NOT NULL
        )
    ),
    CONSTRAINT memory_candidates_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX memory_candidates_scope_status_created_idx
    ON agent_memory.memory_candidates (tenant_id, user_id, status, created_at, id);

CREATE TABLE agent_memory.candidate_source_events (
    tenant_id text NOT NULL,
    user_id text NOT NULL,
    candidate_id text NOT NULL,
    evidence_event_id text NOT NULL,
    source_order integer NOT NULL,
    PRIMARY KEY (tenant_id, user_id, candidate_id, evidence_event_id),
    CONSTRAINT candidate_source_events_candidate_order_unique
        UNIQUE (tenant_id, user_id, candidate_id, source_order),
    CONSTRAINT candidate_source_events_candidate_fk
        FOREIGN KEY (tenant_id, user_id, candidate_id)
        REFERENCES agent_memory.memory_candidates (tenant_id, user_id, id)
        ON DELETE CASCADE,
    CONSTRAINT candidate_source_events_evidence_fk
        FOREIGN KEY (tenant_id, user_id, evidence_event_id)
        REFERENCES agent_memory.evidence_events (tenant_id, user_id, id),
    CONSTRAINT candidate_source_events_source_order_nonnegative CHECK (source_order >= 0)
);

CREATE INDEX candidate_source_events_evidence_idx
    ON agent_memory.candidate_source_events (tenant_id, user_id, evidence_event_id);

CREATE TABLE agent_memory.memory_identity_chains (
    tenant_id text NOT NULL,
    user_id text NOT NULL,
    identity_key text COLLATE "C" NOT NULL,
    kind text COLLATE "C" NOT NULL,
    category text COLLATE "C" NOT NULL,
    memory_key text COLLATE "C" NOT NULL,
    person text COLLATE "C" NOT NULL DEFAULT '',
    relationship text COLLATE "C" NOT NULL DEFAULT '',
    latest_version integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, user_id, identity_key),
    CONSTRAINT memory_identity_chains_scope_fk
        FOREIGN KEY (tenant_id, user_id)
        REFERENCES agent_memory.user_scope_state (tenant_id, user_id),
    CONSTRAINT memory_identity_chains_natural_identity_unique
        UNIQUE (tenant_id, user_id, kind, category, memory_key, person, relationship),
    CONSTRAINT memory_identity_chains_identity_key_not_blank CHECK (btrim(identity_key) <> ''),
    CONSTRAINT memory_identity_chains_kind_valid CHECK (kind IN ('episodic', 'semantic', 'procedural')),
    CONSTRAINT memory_identity_chains_category_not_blank CHECK (btrim(category) <> ''),
    CONSTRAINT memory_identity_chains_key_not_blank CHECK (btrim(memory_key) <> ''),
    CONSTRAINT memory_identity_chains_latest_version_nonnegative CHECK (latest_version >= 0)
);

CREATE TABLE agent_memory.memory_cards (
    tenant_id text NOT NULL,
    user_id text NOT NULL,
    id text NOT NULL,
    candidate_id text NOT NULL,
    identity_key text COLLATE "C" NOT NULL,
    kind text NOT NULL,
    category text NOT NULL,
    memory_key text NOT NULL,
    value text NOT NULL,
    person text NOT NULL DEFAULT '',
    relationship text NOT NULL DEFAULT '',
    backstory text NOT NULL DEFAULT '',
    version integer NOT NULL,
    status text NOT NULL,
    created_at timestamptz NOT NULL,
    superseded_at timestamptz,
    PRIMARY KEY (tenant_id, user_id, id),
    CONSTRAINT memory_cards_candidate_unique UNIQUE (tenant_id, user_id, candidate_id),
    CONSTRAINT memory_cards_identity_version_unique
        UNIQUE (tenant_id, user_id, identity_key, version),
    CONSTRAINT memory_cards_candidate_fk
        FOREIGN KEY (tenant_id, user_id, candidate_id)
        REFERENCES agent_memory.memory_candidates (tenant_id, user_id, id),
    CONSTRAINT memory_cards_identity_chain_fk
        FOREIGN KEY (tenant_id, user_id, identity_key)
        REFERENCES agent_memory.memory_identity_chains (tenant_id, user_id, identity_key),
    CONSTRAINT memory_cards_id_not_blank CHECK (btrim(id) <> ''),
    CONSTRAINT memory_cards_kind_valid CHECK (kind IN ('episodic', 'semantic', 'procedural')),
    CONSTRAINT memory_cards_category_not_blank CHECK (btrim(category) <> ''),
    CONSTRAINT memory_cards_key_not_blank CHECK (btrim(memory_key) <> ''),
    CONSTRAINT memory_cards_value_not_blank CHECK (btrim(value) <> ''),
    CONSTRAINT memory_cards_version_positive CHECK (version > 0),
    CONSTRAINT memory_cards_status_valid CHECK (status IN ('active', 'superseded')),
    CONSTRAINT memory_cards_superseded_at_valid CHECK (
        (status = 'active' AND superseded_at IS NULL)
        OR (status = 'superseded' AND superseded_at IS NOT NULL AND superseded_at >= created_at)
    )
);

CREATE UNIQUE INDEX memory_cards_one_active_identity_idx
    ON agent_memory.memory_cards (tenant_id, user_id, identity_key)
    WHERE status = 'active';

CREATE INDEX memory_cards_scope_active_created_idx
    ON agent_memory.memory_cards (tenant_id, user_id, created_at DESC, id)
    WHERE status = 'active';

---- create above / drop below ----

DROP SCHEMA IF EXISTS agent_memory CASCADE;
