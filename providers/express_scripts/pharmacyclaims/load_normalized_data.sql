-- Load normalized data into table for cleaning out reversals
copy normalized_claims from :input_path credentials :credentials BZIP2 EMPTYASNULL FILLRECORD MAXERROR 20;
