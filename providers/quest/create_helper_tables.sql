/*
 * '50' is assumed to be the absolute max count of diagnoses we would ever see on a single row
 */

DROP TABLE IF EXISTS diagnosis_exploder;
CREATE TABLE diagnosis_exploder
    (n integer ENCODE raw)  
    DISTSTYLE ALL ;
INSERT INTO diagnosis_exploder VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15), (16), (17), (18), (19), (20), (21), (22), (23), (24), (25), (26), (27), (28), (29), (30), (31), (32), (33), (34), (35), (36), (37), (38), (39), (40), (41), (42), (43), (44), (45), (46), (47), (48), (49), (50);
