-- Create a table for the transaction data
DROP TABLE IF EXISTS emdeon_dx_raw;
CREATE TABLE emdeon_dx_raw (column1 text ENCODE lzo,
column2 text ENCODE lzo,
column3 text ENCODE lzo,
column4 text ENCODE lzo,
column5 text ENCODE lzo,
column6 text ENCODE lzo,
column7 text ENCODE lzo,
column8 text ENCODE lzo,
column9 text ENCODE lzo,
column10 text ENCODE lzo,
column11 text ENCODE lzo,
column12 text ENCODE lzo,
column13 text ENCODE lzo,
column14 text ENCODE lzo,
column15 text ENCODE lzo,
column16 text ENCODE lzo,
column17 text ENCODE lzo,
column18 text ENCODE lzo,
column19 text ENCODE lzo,
column20 text ENCODE lzo,
column21 text ENCODE lzo,
column22 text ENCODE lzo,
column23 text ENCODE lzo,
column24 text ENCODE lzo,
column25 text ENCODE lzo,
column26 text ENCODE lzo,
column27 text ENCODE lzo,
column28 text ENCODE lzo,
column29 text ENCODE lzo,
column30 text ENCODE lzo,
column31 text ENCODE lzo,
column32 text ENCODE lzo,
column33 text ENCODE lzo,
column34 text ENCODE lzo,
column35 text ENCODE lzo,
column36 text ENCODE lzo,
column37 text ENCODE lzo,
column38 text ENCODE lzo,
column39 text ENCODE lzo,
column40 text ENCODE lzo,
column41 text ENCODE lzo,
column42 text ENCODE lzo,
column43 text ENCODE lzo,
column44 text ENCODE lzo,
column45 text ENCODE lzo,
column46 text ENCODE lzo,
column47 text ENCODE lzo,
column48 text ENCODE lzo,
column49 text ENCODE lzo,
column50 text ENCODE lzo,
column51 text ENCODE lzo,
column52 text ENCODE lzo,
column53 text ENCODE lzo,
column54 text ENCODE lzo,
column55 text ENCODE lzo,
column56 text ENCODE lzo,
column57 text ENCODE lzo,
column58 text ENCODE lzo,
column59 text ENCODE lzo,
column60 text ENCODE lzo,
column61 text ENCODE lzo,
column62 text ENCODE lzo,
column63 text ENCODE lzo,
column64 text ENCODE lzo,
column65 text ENCODE lzo,
column66 text ENCODE lzo,
column67 text ENCODE lzo,
column68 text ENCODE lzo,
column69 text ENCODE lzo,
column70 text ENCODE lzo,
column71 text ENCODE lzo,
column72 text ENCODE lzo,
column73 text ENCODE lzo,
column74 text ENCODE lzo,
column75 text ENCODE lzo,
column76 text ENCODE lzo,
column77 text ENCODE lzo,
column78 text ENCODE lzo,
column79 text ENCODE lzo,
column80 text ENCODE lzo,
column81 text ENCODE lzo,
column82 text ENCODE lzo,
column83 text ENCODE lzo,
column84 text ENCODE lzo,
column85 text ENCODE lzo,
column86 text ENCODE lzo,
column87 text ENCODE lzo,
column88 text ENCODE lzo,
column89 text ENCODE lzo,
column90 text ENCODE lzo,
column91 text ENCODE lzo,
column92 text ENCODE lzo,
column93 text ENCODE lzo,
column94 text ENCODE lzo,
column95 text ENCODE lzo,
column96 text ENCODE lzo,
column97 text ENCODE lzo,
column98 text ENCODE lzo,
column99 text ENCODE lzo,
column100 text ENCODE lzo,
column101 text ENCODE lzo,
column102 text ENCODE lzo,
column103 text ENCODE lzo,
column104 text ENCODE lzo,
column105 text ENCODE lzo,
column106 text ENCODE lzo,
column107 text ENCODE lzo,
column108 text ENCODE lzo,
column109 text ENCODE lzo,
column110 text ENCODE lzo,
column111 text ENCODE lzo,
column112 text ENCODE lzo,
column113 text ENCODE lzo,
column114 text ENCODE lzo,
column115 text ENCODE lzo,
column116 text ENCODE lzo,
column117 text ENCODE lzo,
column118 text ENCODE lzo,
column119 text ENCODE lzo,
column120 text ENCODE lzo,
column121 text ENCODE lzo,
column122 text ENCODE lzo,
column123 text ENCODE lzo,
column124 text ENCODE lzo,
column125 text ENCODE lzo,
column126 text ENCODE lzo,
column127 text ENCODE lzo,
column128 text ENCODE lzo,
column129 text ENCODE lzo,
column130 text ENCODE lzo,
column131 text ENCODE lzo,
column132 text ENCODE lzo,
column133 text ENCODE lzo,
column134 text ENCODE lzo,
column135 text ENCODE lzo,
column136 text ENCODE lzo,
column137 text ENCODE lzo,
column138 text ENCODE lzo,
column139 text ENCODE lzo,
column140 text ENCODE lzo,
column141 text ENCODE lzo,
column142 text ENCODE lzo,
column143 text ENCODE lzo,
column144 text ENCODE lzo,
column145 text ENCODE lzo,
column146 text ENCODE lzo,
column147 text ENCODE lzo,
column148 text ENCODE lzo,
column149 text ENCODE lzo,
column150 text ENCODE lzo,
column151 text ENCODE lzo,
column152 text ENCODE lzo,
column153 text ENCODE lzo,
column154 text ENCODE lzo,
column155 text ENCODE lzo,
column156 text ENCODE lzo,
column157 text ENCODE lzo,
column158 text ENCODE lzo,
column159 text ENCODE lzo,
column160 text ENCODE lzo,
column161 text ENCODE lzo,
column162 text ENCODE lzo,
column163 text ENCODE lzo,
column164 text ENCODE lzo,
column165 text ENCODE lzo,
column166 text ENCODE lzo,
column167 text ENCODE lzo,
column168 text ENCODE lzo,
column169 text ENCODE lzo,
column170 text ENCODE lzo,
column171 text ENCODE lzo)
DISTKEY(column1) SORTKEY(column2);

-- Load transaction data into table
copy emdeon_dx_raw from :input_path credentials :credentials BZIP2 EMPTYASNULL FILLRECORD;