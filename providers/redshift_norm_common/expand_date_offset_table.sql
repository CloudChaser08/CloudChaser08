SELECT row_number() OVER (order by true) n INTO tmp2 FROM tmp CROSS JOIN (SELECT * FROM tmp) ot;
DROP TABLE tmp;
ALTER TABLE tmp2 RENAME TO tmp;
