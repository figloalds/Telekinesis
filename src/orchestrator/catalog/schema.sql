-- Selected current catalog layout. No old-catalog migration.
CREATE TABLE folder_intents(state TEXT PRIMARY KEY,sync_enabled INTEGER NOT NULL,mount_enabled INTEGER NOT NULL,updated_ms INTEGER NOT NULL);

CREATE TABLE mount_gates(state TEXT PRIMARY KEY,source_installation TEXT,source_endpoint TEXT NOT NULL,created_ms INTEGER NOT NULL,override_ms INTEGER,satisfied_ms INTEGER);

CREATE TABLE peer_credentials(installation TEXT NOT NULL,side TEXT NOT NULL,secret_hash TEXT NOT NULL,created_ms INTEGER NOT NULL,revoked_ms INTEGER,PRIMARY KEY(installation,side));

CREATE TABLE peer_meta(installation TEXT PRIMARY KEY,last_seen_ms INTEGER,display_name TEXT,incompatible TEXT,revoked_ms INTEGER);

CREATE TABLE peers(endpoint TEXT PRIMARY KEY,code TEXT NOT NULL,installation TEXT NOT NULL,connections INTEGER NOT NULL,revision TEXT NOT NULL);

CREATE TABLE projects(state TEXT PRIMARY KEY,label TEXT NOT NULL,repo TEXT NOT NULL UNIQUE,device TEXT NOT NULL,directory TEXT NOT NULL,mount TEXT,desired INTEGER NOT NULL);

CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
