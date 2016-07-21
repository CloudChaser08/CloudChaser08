
all: drop create prep import

drop:
	psql -f drop_table.sql

create:
	psql -f create_table.sql

transform:
	psql -f transform.sql
