DROP TABLE IF EXISTS pharmacyclaims_common_model;
CREATE TABLE pharmacyclaims_common_model AS
SELECT * FROM non_accredo_claims
UNION ALL
SELECT * FROM accredo_claims;
