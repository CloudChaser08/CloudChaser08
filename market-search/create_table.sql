
CREATE TABLE patients (
  hvid INTEGER NOT NULL,
  gender VARCHAR(1),
  zip3 VARCHAR(3),
  year_of_birth INTEGER,
primary key(hvid)
);

CREATE TABLE market_search (
  feed INTEGER NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  hvid INTEGER NOT NULL,
  foreign key(hvid) references patients(hvid) )
distkey(hvid)
sortkey(value);

