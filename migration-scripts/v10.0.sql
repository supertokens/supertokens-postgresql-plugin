-- Core 10.0: bulk import tables + session_info (user_id, app_id) index.

CREATE TABLE IF NOT EXISTS bulk_import_users (
    id CHAR(36),
    app_id VARCHAR(64) NOT NULL DEFAULT 'public',
    primary_user_id VARCHAR(36),
    raw_data TEXT NOT NULL,
    status VARCHAR(128) DEFAULT 'NEW',
    error_msg TEXT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    CONSTRAINT bulk_import_users_pkey PRIMARY KEY(app_id, id),
    CONSTRAINT bulk_import_users__app_id_fkey FOREIGN KEY(app_id) REFERENCES apps(app_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS bulk_import_users_status_updated_at_index ON bulk_import_users (app_id, status, updated_at);

CREATE INDEX IF NOT EXISTS bulk_import_users_pagination_index1 ON bulk_import_users (app_id, status, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS bulk_import_users_pagination_index2 ON bulk_import_users (app_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS session_info_user_id_app_id_index ON session_info (user_id, app_id);
