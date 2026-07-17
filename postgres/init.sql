CREATE TABLE historical_load (
    event_id       BIGINT PRIMARY KEY,
    timestamp      TIMESTAMP NOT NULL,
    value          DOUBLE PRECISION NOT NULL,
    plug_id        INTEGER NOT NULL,
    household_id   INTEGER NOT NULL,
    house_id       INTEGER NOT NULL
);

CREATE INDEX idx_timestamp
ON historical_load(timestamp);