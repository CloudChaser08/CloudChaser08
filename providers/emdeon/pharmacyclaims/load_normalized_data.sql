-- Load normalized data into table for cleaning out reversals
copy :table from :input_path credentials :credentials BZIP2 EMPTYASNULL FILLRECORD MAXERROR 20;
