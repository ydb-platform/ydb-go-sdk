-- +goose Up
-- +goose StatementBegin
INSERT INTO `goose/owners` (owner_id, owner_name, owner_type)
VALUES (1, 'lucas', 'user'), (2, 'space', 'organization');
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DELETE FROM `goose/owners`;
-- +goose StatementEnd
