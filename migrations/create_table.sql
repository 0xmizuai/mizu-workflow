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
    source TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(md5)
);

-- Indexes for datasets
CREATE INDEX idx_datasets_name ON datasets(name);
CREATE INDEX idx_datasets_language ON datasets(language);
CREATE INDEX idx_datasets_name_language ON datasets(name, language);
CREATE INDEX idx_datasets_md5 ON datasets(md5);
CREATE INDEX idx_datasets_source ON datasets(source);

-- Queries table (updated)
CREATE TABLE IF NOT EXISTS queries (
    id SERIAL PRIMARY KEY,
    dataset VARCHAR(255) NOT NULL,
    language VARCHAR(10),
    query_text TEXT NOT NULL,
    publisher VARCHAR(255) NOT NULL,
    total_published INTEGER DEFAULT 0,
    total_processed INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'publishing' CHECK (status IN ('publishing', 'processing', 'processed')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
);

-- Updated indexes for queries
CREATE INDEX idx_queries_publisher ON queries(publisher);
CREATE INDEX idx_queries_dataset_language ON queries(dataset, language);
CREATE INDEX idx_queries_publisher_created ON queries(publisher, created_at DESC);
CREATE INDEX idx_queries_status ON queries(status);

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
