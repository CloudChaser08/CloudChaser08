DROP TABLE IF EXISTS split_indices;
CREATE TABLE split_indices (n integer ENCODE raw) DISTSTYLE ALL;
INSERT INTO split_indices VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12);
