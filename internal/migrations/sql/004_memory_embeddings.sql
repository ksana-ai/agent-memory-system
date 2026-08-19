CREATE TABLE agent_memory.embedding_spaces (
    id text COLLATE "C" PRIMARY KEY,
    provider text COLLATE "C" NOT NULL,
    model text COLLATE "C" NOT NULL,
    dimension integer NOT NULL,
    document_version text COLLATE "C" NOT NULL,
    query_version text COLLATE "C" NOT NULL,
    model_fingerprint text COLLATE "C" NOT NULL,
    created_at timestamptz NOT NULL,
    CONSTRAINT embedding_spaces_id_not_blank CHECK (btrim(id) <> ''),
    CONSTRAINT embedding_spaces_provider_not_blank CHECK (btrim(provider) <> ''),
    CONSTRAINT embedding_spaces_model_not_blank CHECK (btrim(model) <> ''),
    CONSTRAINT embedding_spaces_dimension_pinned CHECK (dimension = 1024),
    CONSTRAINT embedding_spaces_document_version_not_blank CHECK (btrim(document_version) <> ''),
    CONSTRAINT embedding_spaces_query_version_not_blank CHECK (btrim(query_version) <> ''),
    CONSTRAINT embedding_spaces_model_fingerprint_valid CHECK (model_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT embedding_spaces_configuration_unique
        UNIQUE (id, provider, model, document_version, query_version, model_fingerprint)
);

CREATE TABLE agent_memory.memory_embeddings (
    tenant_id text NOT NULL,
    user_id text NOT NULL,
    memory_id text NOT NULL,
    embedding_space text COLLATE "C" NOT NULL,
    provider text COLLATE "C" NOT NULL,
    model text COLLATE "C" NOT NULL,
    document_version text COLLATE "C" NOT NULL,
    query_version text COLLATE "C" NOT NULL,
    model_fingerprint text COLLATE "C" NOT NULL,
    content_sha256 text COLLATE "C" NOT NULL,
    embedding vector(1024) NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, user_id, memory_id, embedding_space),
    CONSTRAINT memory_embeddings_card_fk
        FOREIGN KEY (tenant_id, user_id, memory_id)
        REFERENCES agent_memory.memory_cards (tenant_id, user_id, id)
        ON DELETE CASCADE,
    CONSTRAINT memory_embeddings_space_fk
        FOREIGN KEY (
            embedding_space, provider, model, document_version, query_version, model_fingerprint
        )
        REFERENCES agent_memory.embedding_spaces (
            id, provider, model, document_version, query_version, model_fingerprint
        ),
    CONSTRAINT memory_embeddings_memory_id_not_blank CHECK (btrim(memory_id) <> ''),
    CONSTRAINT memory_embeddings_space_not_blank CHECK (btrim(embedding_space) <> ''),
    CONSTRAINT memory_embeddings_provider_not_blank CHECK (btrim(provider) <> ''),
    CONSTRAINT memory_embeddings_model_not_blank CHECK (btrim(model) <> ''),
    CONSTRAINT memory_embeddings_document_version_not_blank CHECK (btrim(document_version) <> ''),
    CONSTRAINT memory_embeddings_query_version_not_blank CHECK (btrim(query_version) <> ''),
    CONSTRAINT memory_embeddings_model_fingerprint_valid CHECK (model_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT memory_embeddings_content_sha256_valid CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT memory_embeddings_vector_nonzero CHECK (vector_norm(embedding) > 0)
);

CREATE INDEX memory_embeddings_scope_space_idx
    ON agent_memory.memory_embeddings (tenant_id, user_id, embedding_space, memory_id);

---- create above / drop below ----

DROP TABLE IF EXISTS agent_memory.memory_embeddings;

DROP TABLE IF EXISTS agent_memory.embedding_spaces;
