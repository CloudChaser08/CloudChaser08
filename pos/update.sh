#!/bin/sh

echo "Loading Place of Service..."
psql -U $1 -d hvdb -c "COPY place_of_service (id, pos_group, description, start_date, end_date, other) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',')"



