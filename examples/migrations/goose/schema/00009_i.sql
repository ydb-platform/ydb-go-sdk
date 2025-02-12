-- +goose Up
CREATE TABLE `goose/orders` (
    id String,
    doc String,
    PRIMARY KEY(id)
);

CREATE TABLE `goose/events` (
    id String,
    name Utf8,
    service String,
    channel Int64,
    created Timestamp,
    state Json,
    INDEX sample_index GLOBAL ON (service, channel, created),
    PRIMARY KEY(id, name)
);

-- +goose Down
DROP TABLE `goose/events`;
DROP TABLE `goose/orders`;
