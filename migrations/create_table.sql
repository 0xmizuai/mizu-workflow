-- Datasets table
CREATE TABLE IF NOT EXISTS datasets (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    language VARCHAR(10) NOT NULL DEFAULT 'unknown',
    data_type VARCHAR(50) NOT NULL,
    md5 CHAR(32) NOT NULL,
    num_of_records INTEGER DEFAULT 0,
    decompressed_byte_size BIGINT DEFAULT 0,
    byte_size BIGINT DEFAULT 0,
    source TEXT DEFAULT '',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(md5)
);

-- Indexes for datasets
CREATE INDEX idx_datasets_name ON datasets(name);
CREATE INDEX idx_datasets_language ON datasets(language);
CREATE INDEX idx_datasets_name_language ON datasets(name, language);
CREATE INDEX idx_datasets_md5 ON datasets(md5);
CREATE INDEX idx_datasets_byte_size ON datasets(byte_size);
CREATE INDEX idx_datasets_created_at ON datasets(created_at);


-- Queries table (updated)
CREATE TABLE IF NOT EXISTS queries (
    id SERIAL PRIMARY KEY,
    dataset VARCHAR(255) NOT NULL,
    language VARCHAR(10),
    query_text TEXT NOT NULL,
    model VARCHAR(255) NOT NULL,
    owner VARCHAR(255) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'publishing', 'published', 'processed')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Updated indexes for queries
CREATE INDEX idx_queries_owner ON queries(owner);
CREATE INDEX idx_queries_dataset_language ON queries(dataset, language);
CREATE INDEX idx_queries_owner_created ON queries(owner, created_at DESC);
CREATE INDEX idx_queries_status ON queries(status);

CREATE TABLE IF NOT EXISTS query_results (
    id SERIAL PRIMARY KEY,
    query_id INTEGER NOT NULL REFERENCES queries(id),
    data_id INTEGER NOT NULL,
    job_id VARCHAR(255) NOT NULL,
    result JSONB,
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'processed', 'error')),
    finished_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(job_id),
    CONSTRAINT fk_query
        FOREIGN KEY(query_id)
        REFERENCES queries(id)
        ON DELETE CASCADE
);

CREATE INDEX idx_query_results_query_id ON query_results(query_id);
CREATE INDEX idx_query_results_job_id ON query_results(job_id);
CREATE INDEX idx_query_results_status ON query_results(status);
CREATE INDEX idx_query_results_created_at ON query_results(created_at);

-- Dataset statistics table
CREATE TABLE IF NOT EXISTS dataset_stats (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    language VARCHAR(10) NOT NULL,
    data_type VARCHAR(50) NOT NULL,
    total_objects INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(language, data_type, name)
);

-- Indexes for dataset_stats
CREATE INDEX idx_dataset_stats_name ON dataset_stats(name);
CREATE INDEX idx_dataset_stats_language ON dataset_stats(language);
CREATE INDEX idx_dataset_stats_name_language ON dataset_stats(name, language);
CREATE INDEX idx_dataset_stats_created_at ON dataset_stats(created_at);
