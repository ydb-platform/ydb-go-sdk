-- +goose Up
CREATE TABLE `goose/records` (
    id Utf8,
    name Utf8,
    str String,
    num Uint64,
    PRIMARY KEY(id, name)
);

ALTER TABLE `goose/records` ADD CHANGEFEED `changefeed` WITH (
    FORMAT = 'JSON',
    MODE = 'NEW_AND_OLD_IMAGES'
);

-- ALTER TOPIC `goose/records/changefeed` ADD CONSUMER `sample-consumer`;

-- +goose Down
-- ALTER TOPIC `goose/records/changefeed` DROP CONSUMER `sample-consumer`;
ALTER TABLE `goose/records` DROP CHANGEFEED `changefeed`;
DROP TABLE `goose/records`;