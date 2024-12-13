-- +goose Up
-- +goose StatementBegin
ALTER TABLE `goose/stargazers` DROP COLUMN stargazer_location;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE `goose/stargazers` ADD COLUMN stargazer_location Utf8;
-- +goose StatementEnd
