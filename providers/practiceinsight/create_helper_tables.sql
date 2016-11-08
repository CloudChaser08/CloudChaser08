/*
 * '12' is assumed to be the absolute max count of diagnoses we would ever see on a single row
 */

DROP TABLE IF EXISTS diagnosis_exploder;
CREATE TABLE diagnosis_exploder
    (n integer ENCODE raw)  
    DISTSTYLE ALL ;
INSERT INTO diagnosis_exploder VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12);
