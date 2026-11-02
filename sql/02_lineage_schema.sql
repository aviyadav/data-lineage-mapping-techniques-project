CREATE SCHEMA IF NOT EXISTS lineage;

CREATE TABLE IF NOT EXISTS lineage.table_lineage (
    id BIGSERIAL PRIMARY KEY,
    source_table TEXT NOT NULL,
    target_table TEXT NOT NULL,
    transformation_type TEXT,
    confidence REAL DEFAULT 0.9,
    source_file TEXT,
    run_id UUID,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_table_lineage_source
ON lineage.table_lineage(source_table);

CREATE INDEX IF NOT EXISTS idx_table_lineage_target
ON lineage.table_lineage(target_table);


CREATE TABLE IF NOT EXISTS lineage.column_lineage (
    id BIGSERIAL PRIMARY KEY,
    source_table TEXT NOT NULL,
    source_column TEXT NOT NULL,
    target_table TEXT NOT NULL,
    target_column TEXT NOT NULL,
    transformation_type TEXT,
    confidence REAL DEFAULT 0.9,
    sensitive BOOLEAN DEFAULT FALSE,
    source_file TEXT,
    run_id UUID,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_column_lineage_target
ON lineage.column_lineage(target_table, target_column);


CREATE TABLE IF NOT EXISTS lineage.lineage_events (
    event_id UUID PRIMARY KEY,
    run_id UUID,
    event_time TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    job_namespace TEXT,
    job_name TEXT,
    producer TEXT,
    inputs JSONB,
    outputs JSONB,
    facets JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS lineage.runs (
    run_id UUID PRIMARY KEY,
    job_name TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    status TEXT NOT NULL,
    inputs JSONB,
    outputs JSONB,
    metrics JSONB
);


CREATE TABLE IF NOT EXISTS lineage.data_contracts (
    dataset_name TEXT NOT NULL,
    version TEXT NOT NULL,
    schema_definition JSONB NOT NULL,
    lineage JSONB NOT NULL,
    sla JSONB,
    owner TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (dataset_name, version)
);


CREATE TABLE IF NOT EXISTS lineage.contract_violations (
    id BIGSERIAL PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    version TEXT NOT NULL,
    violation_type TEXT NOT NULL,
    details JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS lineage.impact_scores (
    table_name TEXT PRIMARY KEY,
    impact_score NUMERIC(6,2) NOT NULL,
    priority_tier TEXT NOT NULL,
    sla_response TEXT NOT NULL,
    downstream_count INTEGER NOT NULL,
    criticality_level TEXT NOT NULL,
    blast_radius JSONB,
    calculated_at TIMESTAMPTZ DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS lineage.quality_checks (
    id BIGSERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    column_name TEXT,
    check_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    passed BOOLEAN NOT NULL,
    message TEXT,
    executed_at TIMESTAMPTZ DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS lineage.quality_incidents (
    id BIGSERIAL PRIMARY KEY,
    source_table TEXT NOT NULL,
    check_name TEXT NOT NULL,
    severity TEXT NOT NULL,
    contaminated_downstream JSONB,
    propagation_path JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS lineage.lineage_versions (
    version_id BIGSERIAL PRIMARY KEY,
    snapshot_time TIMESTAMPTZ NOT NULL,
    commit_sha TEXT,
    pipeline_version TEXT,
    graph JSONB NOT NULL,
    schema_versions JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS lineage.dataset_ownership (
    dataset_name TEXT PRIMARY KEY,
    owner_email TEXT NOT NULL,
    team TEXT NOT NULL,
    steward_email TEXT,
    last_verified TIMESTAMPTZ NOT NULL,
    documentation_url TEXT,
    lineage_completeness NUMERIC(4,3) DEFAULT 1.0,
    criticality TEXT DEFAULT 'medium',
    recovery_hours NUMERIC(10,2) DEFAULT 1.0
);


CREATE TABLE IF NOT EXISTS lineage.governance_audit_log (
    id BIGSERIAL PRIMARY KEY,
    audit_time TIMESTAMPTZ DEFAULT NOW(),
    issues_found INTEGER NOT NULL,
    details JSONB,
    compliance_rate NUMERIC(6,4)
);