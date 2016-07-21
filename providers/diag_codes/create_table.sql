

CREATE TABLE diag_codes_test (
  code_type  varchar(16) not null,
  order_num  integer,
  diag_code  varchar(32) not null,
  short_desc varchar,
  long_desc  varchar,
  start_date timestamp default now(),
  end_date   timestamp
);

