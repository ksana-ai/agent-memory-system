CREATE TABLE agent_memory.embedding_projection_promotions (
    operation_id text COLLATE "C" PRIMARY KEY,
    from_embedding_space text COLLATE "C",
    to_embedding_space text COLLATE "C" NOT NULL,
    allow_empty boolean NOT NULL,
    live_scope_count bigint NOT NULL,
    live_card_count bigint NOT NULL,
    covered_card_count bigint NOT NULL,
    previous_generation bigint NOT NULL,
    generation bigint NOT NULL,
    cutoff_at timestamptz NOT NULL,
    promoted_at timestamptz NOT NULL,
    CONSTRAINT embedding_projection_promotions_from_space_fk
        FOREIGN KEY (from_embedding_space)
        REFERENCES agent_memory.embedding_spaces (id)
        ON DELETE RESTRICT,
    CONSTRAINT embedding_projection_promotions_to_space_fk
        FOREIGN KEY (to_embedding_space)
        REFERENCES agent_memory.embedding_spaces (id)
        ON DELETE RESTRICT,
    CONSTRAINT embedding_projection_promotions_operation_id_not_blank
        CHECK (operation_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'),
    CONSTRAINT embedding_projection_promotions_from_space_not_blank
        CHECK (from_embedding_space IS NULL OR btrim(from_embedding_space) <> ''),
    CONSTRAINT embedding_projection_promotions_to_space_not_blank
        CHECK (btrim(to_embedding_space) <> ''),
    CONSTRAINT embedding_projection_promotions_distinct_spaces
        CHECK (from_embedding_space IS NULL OR from_embedding_space <> to_embedding_space),
    CONSTRAINT embedding_projection_promotions_counts_valid
        CHECK (
            live_scope_count >= 0
            AND live_card_count >= 0
            AND covered_card_count >= 0
            AND covered_card_count <= live_card_count
            AND live_scope_count <= live_card_count
        ),
    CONSTRAINT embedding_projection_promotions_coverage_complete
        CHECK (covered_card_count = live_card_count),
    CONSTRAINT embedding_projection_promotions_empty_authorized
        CHECK (allow_empty OR live_card_count > 0),
    CONSTRAINT embedding_projection_promotions_generation_valid
        CHECK (
            previous_generation >= 0
            AND generation = previous_generation + 1
        ),
    CONSTRAINT embedding_projection_promotions_timestamps_valid
        CHECK (promoted_at >= cutoff_at)
);

ALTER TABLE agent_memory.embedding_projection_targets
    ADD CONSTRAINT embedding_projection_targets_serving_enqueues
    CHECK (state <> 'serving' OR enqueue_new);

---- create above / drop below ----

DROP TABLE IF EXISTS agent_memory.embedding_projection_promotions;

ALTER TABLE agent_memory.embedding_projection_targets
    DROP CONSTRAINT IF EXISTS embedding_projection_targets_serving_enqueues;
