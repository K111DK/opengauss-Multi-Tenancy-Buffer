\! gs_ktool -d all
\! gs_ktool -g

CREATE SCHEMA ce_drop_schema;
SET SEARCH_PATH TO ce_drop_schema;
CREATE CLIENT MASTER KEY cmk1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY cek1 WITH VALUES (CLIENT_MASTER_KEY = cmk1, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
DROP SCHEMA ce_drop_schema CASCADE;
SHOW SEARCH_PATH;
DROP CLIENT MASTER KEY cmk1 CASCADE;
RESET search_path;
SHOW SEARCH_PATH;
CREATE CLIENT MASTER KEY cmk1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
DROP CLIENT MASTER KEY cmk1 CASCADE;

\! gs_ktool -d all