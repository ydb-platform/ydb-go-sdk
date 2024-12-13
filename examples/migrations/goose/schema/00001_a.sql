-- +goose Up
-- +goose StatementBegin
CREATE TABLE `goose/owners` (
    owner_id Uint64,
    owner_name Utf8,
    owner_type Utf8,
    PRIMARY KEY (owner_id)
);
CREATE TABLE `goose/repos` (
    repo_id Uint64,
    repo_owner_id Uint64,
    repo_full_name Utf8,
    PRIMARY KEY (repo_id)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE `goose/repos`;
DROP TABLE `goose/owners`;
-- +goose StatementEnd
