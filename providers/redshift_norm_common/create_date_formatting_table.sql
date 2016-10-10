CREATE TABLE dates (date text encode lzo, formatted text encode lzo) DISTSTYLE ALL;
INSERT INTO dates SELECT replace((:min_valid_date::date + n - 1)::date::text, '-', ''), (:min_valid_date::date + n - 1)::date::text FROM tmp WHERE (:min_valid_date::date + n - 1) <= getdate()::date;
