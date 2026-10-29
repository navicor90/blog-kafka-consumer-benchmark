CREATE TABLE sensor (
    id             BIGINT PRIMARY KEY,
    temperature    DOUBLE PRECISION,
    decommissioned BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at     TIMESTAMPTZ
);

-- Last temperature produced per sensor, written by the producer to verify correctness
CREATE TABLE expected_state (
    id          BIGINT PRIMARY KEY,
    temperature DOUBLE PRECISION NOT NULL
);
