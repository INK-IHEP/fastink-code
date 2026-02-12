-- start transaction
START TRANSACTION;
-- check table structure
SELECT COLUMN_NAME,
    IS_NULLABLE,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'inkdb'
    AND TABLE_NAME = 'users'
    AND COLUMN_NAME = 'email';
-- alter email column to allow NULL
ALTER TABLE inkdb.users
MODIFY COLUMN email VARCHAR(255) NULL DEFAULT NULL COMMENT 'email';
ALTER TABLE inkdb.users DROP CONSTRAINT USERS_EMAIL_NN;
-- check table structure result
SELECT COLUMN_NAME,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'inkdb'
    AND TABLE_NAME = 'users'
    AND COLUMN_NAME = 'email';
-- commit if everything is fine
COMMIT;