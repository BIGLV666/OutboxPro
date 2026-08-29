CREATE TABLE IF NOT EXISTS outboxpro_dead_letter (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(200),
    consumer_name VARCHAR(200) NOT NULL,
    queue_name VARCHAR(255) NOT NULL,
    original_exchange VARCHAR(255) NOT NULL,
    original_routing_key VARCHAR(255) NOT NULL,
    payload_json LONGTEXT NOT NULL,
    attempt_count INT NOT NULL,
    reason_code VARCHAR(64) NOT NULL,
    reason_retryable BOOLEAN NOT NULL,
    reason_retry_exhausted BOOLEAN NOT NULL,
    exception_type VARCHAR(300),
    exception_message VARCHAR(2000),
    status VARCHAR(32) NOT NULL,
    dispatch_owner VARCHAR(100),
    dispatch_until DATETIME(6),
    replay_count INT NOT NULL DEFAULT 0,
    replay_owner VARCHAR(100),
    replayed_time DATETIME(6),
    last_replay_operator VARCHAR(200),
    last_replay_reason VARCHAR(1000),
    last_replay_error VARCHAR(2000),
    created_time DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_time DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    version BIGINT NOT NULL DEFAULT 0,
    UNIQUE KEY uk_dead_letter_event_consumer (event_id, consumer_name),
    KEY idx_dead_letter_replay (status, replay_count, id),
    KEY idx_dead_letter_event (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS outboxpro_dead_letter_counter (
    counter_bucket INT NOT NULL PRIMARY KEY,
    pending_count BIGINT NOT NULL DEFAULT 0,
    updated_time DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    version BIGINT NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

