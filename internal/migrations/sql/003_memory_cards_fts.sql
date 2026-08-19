ALTER TABLE agent_memory.memory_cards
    ADD COLUMN search_document tsvector
    GENERATED ALWAYS AS (
        setweight(
            to_tsvector('pg_catalog.simple'::regconfig, coalesce(memory_key, '')),
            'A'
        )
        || setweight(
            to_tsvector('pg_catalog.simple'::regconfig, coalesce(value, '')),
            'B'
        )
        || setweight(
            to_tsvector(
                'pg_catalog.simple'::regconfig,
                coalesce(category, '') || ' '
                    || coalesce(person, '') || ' '
                    || coalesce(relationship, '')
            ),
            'C'
        )
        || setweight(
            to_tsvector('pg_catalog.simple'::regconfig, coalesce(backstory, '')),
            'D'
        )
    ) STORED;

CREATE INDEX memory_cards_active_search_document_gin_idx
    ON agent_memory.memory_cards
    USING gin (search_document)
    WHERE status = 'active';

---- create above / drop below ----

DROP INDEX IF EXISTS agent_memory.memory_cards_active_search_document_gin_idx;

ALTER TABLE agent_memory.memory_cards
    DROP COLUMN IF EXISTS search_document;
