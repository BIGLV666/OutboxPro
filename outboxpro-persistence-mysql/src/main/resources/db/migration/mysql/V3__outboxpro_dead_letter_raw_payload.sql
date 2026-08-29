ALTER TABLE outboxpro_dead_letter
    MODIFY COLUMN payload_json LONGTEXT NOT NULL;
