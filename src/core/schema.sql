-- Selected current metadata schema. Initialize atomically; do not replay migrations.
CREATE TABLE branches(id TEXT PRIMARY KEY, name TEXT NOT NULL, shared INTEGER NOT NULL);

CREATE TABLE checkpoints(id TEXT PRIMARY KEY, branch TEXT NOT NULL, object TEXT NOT NULL, message TEXT NOT NULL);

CREATE TABLE config(key TEXT PRIMARY KEY, value TEXT NOT NULL);

CREATE TABLE content_delete_intents(request TEXT PRIMARY KEY NOT NULL CHECK(length(request)=36), branch TEXT NOT NULL, entity TEXT NOT NULL, operation TEXT NOT NULL, record BLOB NOT NULL);

CREATE INDEX content_delete_entity ON content_delete_intents(branch,entity,request);

CREATE TABLE directory_delete_intents(request TEXT PRIMARY KEY NOT NULL CHECK(length(request)=36), branch TEXT NOT NULL, entity TEXT NOT NULL, record BLOB NOT NULL CHECK(length(record)<=65536));

CREATE INDEX directory_delete_entity ON directory_delete_intents(branch,entity,request);

CREATE TABLE conflicts(id TEXT PRIMARY KEY, branch TEXT NOT NULL, payload TEXT NOT NULL, resolved_by TEXT);

CREATE TABLE content_alias_cursor(kind INTEGER NOT NULL,digest TEXT NOT NULL,
        before_id INTEGER NOT NULL,PRIMARY KEY(kind,digest));

CREATE TABLE content_manifest_edges(
        parent TEXT NOT NULL,ordinal INTEGER NOT NULL,kind INTEGER NOT NULL,digest TEXT NOT NULL,
        len INTEGER NOT NULL,level INTEGER NOT NULL,PRIMARY KEY(parent,ordinal));

CREATE TABLE content_object_jobs(
        kind INTEGER NOT NULL,digest TEXT NOT NULL,verified INTEGER NOT NULL DEFAULT 0,
        expanded INTEGER NOT NULL DEFAULT 0,retry_at_ms INTEGER NOT NULL DEFAULT 0,error TEXT,
        witness TEXT NOT NULL,PRIMARY KEY(kind,digest));

CREATE TABLE content_page_aliases(
        id INTEGER PRIMARY KEY,kind INTEGER NOT NULL,digest TEXT NOT NULL,reference TEXT NOT NULL,
        generation INTEGER NOT NULL,offset INTEGER NOT NULL,
        UNIQUE(kind,digest,reference,generation,offset));

CREATE TABLE content_public_receipts(kind INTEGER NOT NULL,digest TEXT NOT NULL,
            PRIMARY KEY(kind,digest));

CREATE TABLE content_root_metadata(
            reference TEXT PRIMARY KEY,event TEXT NOT NULL,entity TEXT NOT NULL,
            branch TEXT NOT NULL,root BLOB NOT NULL CHECK(length(root)=48),
            generation INTEGER NOT NULL CHECK(generation>0),granted INTEGER NOT NULL,
            shared INTEGER NOT NULL,publishable INTEGER NOT NULL);

CREATE TABLE content_root_outbox(
            seq INTEGER PRIMARY KEY AUTOINCREMENT,repository TEXT NOT NULL,branch TEXT NOT NULL,
            reference TEXT NOT NULL,generation INTEGER NOT NULL CHECK(generation>0),
            root BLOB NOT NULL CHECK(length(root)=48),action TEXT NOT NULL CHECK(action IN ('grant','withdraw')),
            shared INTEGER NOT NULL,publishable INTEGER NOT NULL,UNIQUE(reference,generation));

CREATE TABLE content_save_create_changes(
            mutation TEXT PRIMARY KEY CHECK(length(mutation)=36),
            record BLOB NOT NULL CHECK(length(record)<=65536));

CREATE TABLE content_save_entities(
            branch TEXT NOT NULL,entity TEXT NOT NULL,generation INTEGER NOT NULL
            CHECK(typeof(generation)='integer' AND generation>0),
            owner TEXT CHECK(owner IS NULL OR length(owner)=36),PRIMARY KEY(branch,entity));

CREATE TABLE content_save_history(
            operation TEXT NOT NULL,checkpoint TEXT NOT NULL,generation INTEGER NOT NULL CHECK(generation>0),
            capture BLOB NOT NULL CHECK(length(capture)<=65536),
            selected BLOB NOT NULL CHECK(length(selected)<=65536),receipt BLOB NOT NULL CHECK(length(receipt)<=65536),
            PRIMARY KEY(operation,checkpoint),UNIQUE(operation,generation));

CREATE TABLE content_save_metadata_changes(
            mutation TEXT PRIMARY KEY CHECK(length(mutation)=36),
            record BLOB NOT NULL CHECK(length(record)<=65536));

CREATE TABLE content_save_namespace_changes(
            mutation TEXT PRIMARY KEY CHECK(length(mutation)=36),
            record BLOB NOT NULL CHECK(length(record)<=65536));

CREATE TABLE content_save_namespace_continuations(
            operation TEXT NOT NULL,generation INTEGER NOT NULL CHECK(generation>0),
            mutation TEXT NOT NULL CHECK(length(mutation)=36),
            previous BLOB NOT NULL CHECK(length(previous)<=65536),
            successor BLOB NOT NULL CHECK(length(successor)<=65536),
            selected BLOB CHECK(length(selected)<=65536),
            PRIMARY KEY(operation,generation),UNIQUE(mutation));

CREATE TABLE content_save_operations(
            operation TEXT PRIMARY KEY,capture BLOB NOT NULL CHECK(length(capture)<=65536),
            selected BLOB CHECK(length(selected)<=65536),receipt BLOB CHECK(length(receipt)<=65536));

CREATE TABLE content_save_successors(
            operation TEXT NOT NULL,checkpoint TEXT NOT NULL,capture BLOB NOT NULL CHECK(length(capture)<=65536),
            PRIMARY KEY(operation,checkpoint));

CREATE TABLE content_source_backoff(kind INTEGER NOT NULL,digest TEXT NOT NULL,
            source TEXT NOT NULL,attempts INTEGER NOT NULL,retry_at_ms INTEGER NOT NULL,
            PRIMARY KEY(kind,digest,source));

CREATE TABLE content_transfer_cursor(source TEXT NOT NULL,lane_index INTEGER NOT NULL,
        lane_count INTEGER NOT NULL,kind INTEGER NOT NULL,after_digest TEXT NOT NULL,
        PRIMARY KEY(source,lane_index,lane_count,kind));

CREATE TABLE current_meta(branch TEXT PRIMARY KEY,payload TEXT NOT NULL);

CREATE TABLE current_parts(branch TEXT NOT NULL,entity TEXT NOT NULL,entry TEXT NOT NULL,heads TEXT NOT NULL,PRIMARY KEY(branch,entity));

CREATE TABLE current_state(branch TEXT PRIMARY KEY, payload TEXT NOT NULL);

CREATE TABLE event_frontier(branch TEXT NOT NULL,event TEXT NOT NULL,PRIMARY KEY(branch,event));

CREATE TABLE event_objects(hash TEXT NOT NULL,event TEXT NOT NULL,shared INTEGER NOT NULL,PRIMARY KEY(hash,event));

CREATE TABLE events(id TEXT PRIMARY KEY, branch TEXT NOT NULL, payload TEXT NOT NULL);

CREATE TABLE feed_in(peer TEXT PRIMARY KEY,epoch TEXT NOT NULL,seq INTEGER NOT NULL,id TEXT NOT NULL,
                tip INTEGER NOT NULL,updated_ms INTEGER NOT NULL,pending_active INTEGER NOT NULL DEFAULT 0,
                blocked INTEGER NOT NULL DEFAULT 0,quarantined INTEGER NOT NULL DEFAULT 0);

CREATE TABLE feed_journal(seq INTEGER PRIMARY KEY,event TEXT NOT NULL);

CREATE TABLE feed_needs(peer TEXT NOT NULL,event TEXT NOT NULL,reason TEXT NOT NULL,
                requested_ms INTEGER NOT NULL,missing_at INTEGER,seq INTEGER NOT NULL DEFAULT 0,digest TEXT,
                PRIMARY KEY(peer,event));

CREATE TABLE feed_out(peer TEXT NOT NULL,via TEXT NOT NULL,epoch TEXT NOT NULL,seq INTEGER NOT NULL,
                id TEXT NOT NULL,reported_ms INTEGER NOT NULL,PRIMARY KEY(peer,via));

CREATE TABLE feed_rejected(peer TEXT NOT NULL,event TEXT NOT NULL,seq INTEGER NOT NULL,
                code TEXT NOT NULL,message TEXT NOT NULL,payload TEXT,at_ms INTEGER NOT NULL,PRIMARY KEY(peer,event));

CREATE TABLE feed_remaining(peer TEXT PRIMARY KEY,epoch TEXT NOT NULL,seq INTEGER NOT NULL,
                tip INTEGER NOT NULL,remaining INTEGER NOT NULL);

CREATE TABLE incoming_events(id TEXT PRIMARY KEY,payload TEXT NOT NULL,error TEXT, state TEXT, received_ms INTEGER, attempts INTEGER, retry_at_ms INTEGER, code TEXT, error_class TEXT, blocked_on TEXT);

CREATE TABLE incoming_sources(event TEXT NOT NULL,peer TEXT NOT NULL,seq INTEGER NOT NULL,PRIMARY KEY(event,peer));

CREATE TABLE incoming_waits(child TEXT NOT NULL,parent TEXT NOT NULL,PRIMARY KEY(child,parent));

CREATE TABLE journal_branch(seq INTEGER PRIMARY KEY,branch TEXT NOT NULL);

CREATE TABLE journal_sequence(seq INTEGER PRIMARY KEY AUTOINCREMENT,event TEXT NOT NULL UNIQUE);

CREATE TABLE object_admission_cursor(consumer TEXT PRIMARY KEY,cursor INTEGER NOT NULL);

CREATE TABLE object_availability(hash TEXT PRIMARY KEY,size INTEGER NOT NULL,state INTEGER NOT NULL,mtime_ns INTEGER NOT NULL,verified_ms INTEGER NOT NULL);

CREATE TABLE object_error_state(hash TEXT PRIMARY KEY,code TEXT NOT NULL,class TEXT NOT NULL,attempts INTEGER NOT NULL,retry_at_ms INTEGER NOT NULL,last_ms INTEGER NOT NULL);

CREATE TABLE object_errors(hash TEXT PRIMARY KEY,error TEXT NOT NULL,at INTEGER NOT NULL);

CREATE TABLE object_references(hash TEXT PRIMARY KEY);

CREATE TABLE object_requests(hash TEXT PRIMARY KEY,requested_ms INTEGER NOT NULL);

CREATE TABLE object_root_admission(
            reference TEXT PRIMARY KEY,branch TEXT NOT NULL,root BLOB NOT NULL CHECK(length(root)=48),
            generation INTEGER NOT NULL,granted INTEGER NOT NULL,shared INTEGER NOT NULL,publishable INTEGER NOT NULL);

CREATE TABLE object_sizes(hash TEXT PRIMARY KEY,size INTEGER NOT NULL);

CREATE TABLE outbox(event TEXT PRIMARY KEY, acknowledged INTEGER NOT NULL DEFAULT 0);

CREATE TABLE peer_known(peer TEXT NOT NULL,event TEXT NOT NULL,PRIMARY KEY(peer,event));

CREATE TABLE peer_objects(peer TEXT NOT NULL,hash TEXT NOT NULL,PRIMARY KEY(peer,hash));

CREATE TABLE peer_requests(peer TEXT NOT NULL,hash TEXT NOT NULL,PRIMARY KEY(peer,hash));

CREATE TABLE pending_saves(entity TEXT PRIMARY KEY,payload TEXT NOT NULL);

CREATE TABLE projection_version(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL);

CREATE TABLE published_objects(hash TEXT PRIMARY KEY);

CREATE TABLE receipts(id TEXT PRIMARY KEY, payload_hash TEXT NOT NULL, result TEXT NOT NULL);

CREATE TABLE received_shared_objects(hash TEXT PRIMARY KEY);

CREATE TABLE snapshot_conflicts(branch TEXT NOT NULL,id TEXT NOT NULL,payload TEXT NOT NULL,resolved_by TEXT,PRIMARY KEY(branch,id));

CREATE TABLE snapshot_dirty(branch TEXT NOT NULL,kind INTEGER NOT NULL,key TEXT NOT NULL,PRIMARY KEY(branch,kind,key));

CREATE TABLE snapshot_entries(branch TEXT NOT NULL,entity TEXT NOT NULL,entry TEXT NOT NULL,heads TEXT NOT NULL,PRIMARY KEY(branch,entity));

CREATE TABLE spool_state(hash TEXT PRIMARY KEY,durable INTEGER NOT NULL,total INTEGER NOT NULL,updated_ms INTEGER NOT NULL);

CREATE TABLE staged_objects(hash TEXT NOT NULL,event TEXT NOT NULL,PRIMARY KEY(hash,event));

CREATE TABLE startup_snapshots(branch TEXT PRIMARY KEY,cursor INTEGER NOT NULL,payload TEXT NOT NULL,checksum TEXT NOT NULL);

CREATE INDEX content_alias_page ON content_page_aliases(kind,digest,id);

CREATE INDEX content_jobs_missing ON content_object_jobs(verified,retry_at_ms,kind,digest);

CREATE INDEX content_jobs_pages ON content_object_jobs(kind,expanded,retry_at_ms,digest);

CREATE INDEX content_jobs_scan ON content_object_jobs(verified,kind,digest);

CREATE INDEX content_jobs_structure_scan ON content_object_jobs(kind,expanded,digest);

CREATE INDEX content_roots_by_event ON content_root_metadata(event);

CREATE INDEX event_objects_by_event ON event_objects(event);

CREATE INDEX feed_needs_event ON feed_needs(event);

CREATE INDEX incoming_events_blocked_on ON incoming_events(blocked_on) WHERE blocked_on IS NOT NULL;

CREATE INDEX incoming_events_state ON incoming_events(state,received_ms);

CREATE INDEX incoming_sources_peer ON incoming_sources(peer);

CREATE INDEX incoming_waits_parent ON incoming_waits(parent);

CREATE INDEX journal_branch_order ON journal_branch(branch,seq);

CREATE INDEX object_availability_by_state ON object_availability(state,verified_ms);

CREATE INDEX staged_objects_by_event ON staged_objects(event);

CREATE TRIGGER event_objects_delete AFTER DELETE ON events BEGIN
        DELETE FROM event_objects WHERE event=OLD.id;
    END;

CREATE TRIGGER event_objects_insert AFTER INSERT ON events BEGIN
        INSERT OR IGNORE INTO event_objects(hash,event,shared)
            SELECT json_extract(c.value,'$.value'),NEW.id,coalesce(json_extract(NEW.payload,'$.branch.shared'),0)
            FROM json_each(CASE WHEN json_valid(NEW.payload) THEN NEW.payload ELSE '{}' END,'$.changes') c
            WHERE json_extract(c.value,'$.field')='content' AND json_type(c.value,'$.value')='text';
    END;

CREATE TRIGGER event_objects_update AFTER UPDATE OF id,payload ON events BEGIN
        DELETE FROM event_objects WHERE event=OLD.id;
        INSERT OR IGNORE INTO event_objects(hash,event,shared)
            SELECT json_extract(c.value,'$.value'),NEW.id,coalesce(json_extract(NEW.payload,'$.branch.shared'),0)
            FROM json_each(CASE WHEN json_valid(NEW.payload) THEN NEW.payload ELSE '{}' END,'$.changes') c
            WHERE json_extract(c.value,'$.field')='content' AND json_type(c.value,'$.value')='text';
    END;

CREATE TRIGGER feed_epoch_delete AFTER DELETE ON events BEGIN
                INSERT OR REPLACE INTO config(key,value) VALUES('journal_epoch',lower(hex(randomblob(16))));
            END;

CREATE TRIGGER feed_epoch_update AFTER UPDATE ON events BEGIN
                INSERT OR REPLACE INTO config(key,value) VALUES('journal_epoch',lower(hex(randomblob(16))));
            END;

CREATE TRIGGER feed_journal_branch AFTER UPDATE OF shared ON branches
                WHEN OLD.shared IS NOT NEW.shared BEGIN
                DELETE FROM feed_journal WHERE seq IN (SELECT s.seq FROM journal_sequence s
                    JOIN events e ON e.id=s.event WHERE e.branch=NEW.id);
                INSERT OR REPLACE INTO feed_journal(seq,event) SELECT s.seq,s.event FROM journal_sequence s
                    JOIN events e ON e.id=s.event WHERE e.branch=NEW.id AND NEW.shared=1;
            END;

CREATE TRIGGER feed_journal_delete AFTER DELETE ON journal_sequence BEGIN
                DELETE FROM feed_journal WHERE seq=OLD.seq;
            END;

CREATE TRIGGER feed_journal_insert AFTER INSERT ON journal_sequence BEGIN
                INSERT OR REPLACE INTO feed_journal(seq,event) SELECT NEW.seq,NEW.event FROM events e
                    JOIN branches b ON b.id=e.branch WHERE e.id=NEW.event AND b.shared=1;
            END;

CREATE TRIGGER feed_journal_update AFTER UPDATE ON journal_sequence BEGIN
                DELETE FROM feed_journal WHERE seq=OLD.seq;
                INSERT OR REPLACE INTO feed_journal(seq,event) SELECT NEW.seq,NEW.event FROM events e
                    JOIN branches b ON b.id=e.branch WHERE e.id=NEW.event AND b.shared=1;
            END;

CREATE TRIGGER feed_outbox_known AFTER INSERT ON outbox
                WHEN NEW.acknowledged=0 AND EXISTS(SELECT 1 FROM peer_known k WHERE k.peer IN (SELECT peer FROM feed_in) AND k.event=NEW.event) BEGIN
                UPDATE outbox SET acknowledged=1 WHERE event=NEW.event;
            END;

CREATE TRIGGER incoming_events_cleanup AFTER DELETE ON incoming_events BEGIN
                DELETE FROM incoming_waits WHERE child=OLD.id;
                DELETE FROM incoming_sources WHERE event=OLD.id;
            END;

CREATE TRIGGER journal_branch_delete AFTER DELETE ON journal_sequence BEGIN
            DELETE FROM journal_branch WHERE seq=OLD.seq;
        END;

CREATE TRIGGER journal_branch_insert AFTER INSERT ON journal_sequence BEGIN
            INSERT OR REPLACE INTO journal_branch(seq,branch) SELECT NEW.seq,e.branch FROM events e WHERE e.id=NEW.event;
        END;

CREATE TRIGGER journal_branch_update AFTER UPDATE ON journal_sequence BEGIN
            DELETE FROM journal_branch WHERE seq=OLD.seq;
            INSERT OR REPLACE INTO journal_branch(seq,branch) SELECT NEW.seq,e.branch FROM events e WHERE e.id=NEW.event;
        END;

CREATE TRIGGER projection_version_meta_delete AFTER DELETE ON current_meta BEGIN
            UPDATE projection_version SET version=version+1 WHERE id=1;
        END;

CREATE TRIGGER projection_version_meta_insert AFTER INSERT ON current_meta BEGIN
            UPDATE projection_version SET version=version+1 WHERE id=1;
        END;

CREATE TRIGGER projection_version_meta_update AFTER UPDATE ON current_meta BEGIN
            UPDATE projection_version SET version=version+1 WHERE id=1;
        END;

CREATE TRIGGER projection_version_parts_delete AFTER DELETE ON current_parts BEGIN
            UPDATE projection_version SET version=version+1 WHERE id=1;
        END;

CREATE TRIGGER projection_version_parts_insert AFTER INSERT ON current_parts BEGIN
            UPDATE projection_version SET version=version+1 WHERE id=1;
        END;

CREATE TRIGGER projection_version_parts_update AFTER UPDATE ON current_parts BEGIN
            UPDATE projection_version SET version=version+1 WHERE id=1;
        END;

CREATE TRIGGER projection_version_state_delete AFTER DELETE ON current_state BEGIN
            UPDATE projection_version SET version=version+1 WHERE id=1;
        END;

CREATE TRIGGER projection_version_state_insert AFTER INSERT ON current_state BEGIN
            UPDATE projection_version SET version=version+1 WHERE id=1;
        END;

CREATE TRIGGER projection_version_state_update AFTER UPDATE ON current_state BEGIN
            UPDATE projection_version SET version=version+1 WHERE id=1;
        END;

CREATE TRIGGER snapshot_dirty_conflicts_delete AFTER DELETE ON conflicts BEGIN
            INSERT INTO snapshot_dirty(branch,kind,key) SELECT OLD.branch,1,OLD.id
                WHERE NOT EXISTS(SELECT 1 FROM snapshot_dirty WHERE branch=OLD.branch AND kind=1 AND key=OLD.id);
        END;

CREATE TRIGGER snapshot_dirty_conflicts_insert AFTER INSERT ON conflicts BEGIN
            INSERT INTO snapshot_dirty(branch,kind,key) SELECT NEW.branch,1,NEW.id
                WHERE NOT EXISTS(SELECT 1 FROM snapshot_dirty WHERE branch=NEW.branch AND kind=1 AND key=NEW.id);
        END;

CREATE TRIGGER snapshot_dirty_conflicts_update AFTER UPDATE ON conflicts BEGIN
            INSERT INTO snapshot_dirty(branch,kind,key) SELECT NEW.branch,1,NEW.id
                WHERE NOT EXISTS(SELECT 1 FROM snapshot_dirty WHERE branch=NEW.branch AND kind=1 AND key=NEW.id);
        END;

CREATE TRIGGER snapshot_dirty_parts_insert AFTER INSERT ON current_parts BEGIN
            INSERT INTO snapshot_dirty(branch,kind,key) SELECT NEW.branch,0,NEW.entity
                WHERE NOT EXISTS(SELECT 1 FROM snapshot_dirty WHERE branch=NEW.branch AND kind=0 AND key=NEW.entity);
        END;

CREATE TRIGGER snapshot_dirty_parts_update AFTER UPDATE ON current_parts BEGIN
            INSERT INTO snapshot_dirty(branch,kind,key) SELECT NEW.branch,0,NEW.entity
                WHERE NOT EXISTS(SELECT 1 FROM snapshot_dirty WHERE branch=NEW.branch AND kind=0 AND key=NEW.entity);
        END;

CREATE TRIGGER snapshot_event_delete AFTER DELETE ON events BEGIN
            DELETE FROM startup_snapshots;
            DELETE FROM journal_sequence WHERE event=OLD.id;
        END;

CREATE TRIGGER snapshot_event_insert AFTER INSERT ON events BEGIN
            INSERT INTO journal_sequence(event) VALUES(NEW.id);
        END;

CREATE TRIGGER snapshot_event_update AFTER UPDATE ON events BEGIN
            DELETE FROM startup_snapshots;
            UPDATE journal_sequence SET event=NEW.id WHERE event=OLD.id;
        END;

CREATE TRIGGER staged_objects_delete AFTER DELETE ON incoming_events BEGIN
        DELETE FROM staged_objects WHERE event=OLD.id;
    END;

CREATE TRIGGER staged_objects_insert AFTER INSERT ON incoming_events BEGIN
        INSERT OR IGNORE INTO staged_objects(hash,event)
            SELECT json_extract(c.value,'$.value'),NEW.id
            FROM json_each(CASE WHEN json_valid(NEW.payload) THEN NEW.payload ELSE '{}' END,'$.changes') c
            WHERE json_extract(c.value,'$.field')='content' AND json_type(c.value,'$.value')='text';
    END;

CREATE TRIGGER staged_objects_update AFTER UPDATE OF id,payload ON incoming_events BEGIN
        DELETE FROM staged_objects WHERE event=OLD.id;
        INSERT OR IGNORE INTO staged_objects(hash,event)
            SELECT json_extract(c.value,'$.value'),NEW.id
            FROM json_each(CASE WHEN json_valid(NEW.payload) THEN NEW.payload ELSE '{}' END,'$.changes') c
            WHERE json_extract(c.value,'$.field')='content' AND json_type(c.value,'$.value')='text';
    END;
