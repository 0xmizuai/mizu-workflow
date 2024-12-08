-- Datasets table
CREATE TABLE IF NOT EXISTS datasets (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    language VARCHAR(10) DEFAULT 'en',
    data_type VARCHAR(50) NOT NULL,
    r2_key TEXT NOT NULL,
    md5 CHAR(32) NOT NULL,
    num_of_records INTEGER,
    decompressed_byte_size BIGINT,
    byte_size BIGINT NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, language)
);

-- Indexes for datasets
CREATE INDEX idx_datasets_name ON datasets(name);
CREATE INDEX idx_datasets_language ON datasets(language);
CREATE INDEX idx_datasets_data_type ON datasets(data_type);
CREATE INDEX idx_datasets_name_language ON datasets(name, language);

-- Additional indexes for new fields
CREATE INDEX idx_datasets_md5 ON datasets(md5);
CREATE INDEX idx_datasets_processed_at ON datasets(processed_at);

-- Queries table (updated)
CREATE TABLE IF NOT EXISTS queries (
    id SERIAL PRIMARY KEY,
    dataset_id INTEGER NOT NULL REFERENCES datasets(id),
    query_text TEXT NOT NULL,
    publisher VARCHAR(255) NOT NULL,
    language VARCHAR(10) DEFAULT 'en',
    progress BIGINT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_dataset
        FOREIGN KEY(dataset_id)
        REFERENCES datasets(id)
        ON DELETE RESTRICT
);

-- Updated indexes for queries
CREATE INDEX idx_queries_dataset_id ON queries(dataset_id);
CREATE INDEX idx_queries_publisher ON queries(publisher);
CREATE INDEX idx_queries_language ON queries(language);
CREATE INDEX idx_queries_publisher_created ON queries(publisher, created_at DESC);

CREATE TABLE IF NOT EXISTS query_results (
    id SERIAL PRIMARY KEY,
    query_id INTEGER NOT NULL REFERENCES queries(id),
    url TEXT NOT NULL,
    warc_id TEXT NOT NULL,
    text TEXT,
    crawled_at TIMESTAMP WITH TIME ZONE,
    processed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_query
        FOREIGN KEY(query_id)
        REFERENCES queries(id)
        ON DELETE CASCADE
);

CREATE INDEX idx_query_results_query_id ON query_results(query_id);
