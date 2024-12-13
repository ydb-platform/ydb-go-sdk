
-- +goose Up
-- +goose StatementBegin
INSERT INTO `goose/owners` (owner_id, owner_name, owner_type)
VALUES (3, 'james', 'user'), (4, 'pressly', 'organization');
INSERT INTO `goose/repos` (repo_id, repo_full_name, repo_owner_id)
VALUES (1, 'james/rover', 3), (2, 'pressly/goose', 4);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DELETE FROM `goose/owners` WHERE (owner_id = 3 OR owner_id = 4);
DELETE FROM `goose/repos` WHERE (repo_id = 1 OR repo_id = 2);
-- +goose StatementEnd
