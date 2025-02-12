-- +goose Up
CREATE TABLE `goose/tbl_with_pk_uuid` (
    id Uuid,
    value Double,
    PRIMARY KEY(id)
);
UPSERT INTO `goose/tbl_with_pk_uuid` (id, value) VALUES (CAST('00000000-0000-0000-0000-000000000001' AS Uuid), 1.0);

-- +goose Down
DELETE FROM `goose/tbl_with_pk_uuid` WHERE id=CAST('00000000-0000-0000-0000-000000000001' AS Uuid);
DROP TABLE `goose/tbl_with_pk_uuid`;