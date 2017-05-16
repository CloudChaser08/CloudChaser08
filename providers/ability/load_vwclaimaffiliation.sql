DROP TABLE IF EXISTS transactional_claimaffiliation_dupes CASCADE;
CREATE TABLE transactional_claimaffiliation_dupes (
        claimid      text ENCODE lzo,
        type         text ENCODE lzo,
        npi          text ENCODE lzo,
        fullname     text ENCODE lzo,
        firstname    text ENCODE lzo,
        middlename   text ENCODE lzo,
        lastname     text ENCODE lzo,
        taxonomy     text ENCODE lzo,
        orgnpi       text ENCODE lzo,
        orgname      text ENCODE lzo,
        addr1        text ENCODE lzo,
        addr2        text ENCODE lzo,
        city         text ENCODE lzo,
        state        text ENCODE lzo,
        zip          text ENCODE lzo,
        processdate  text ENCODE lzo
        ) DISTKEY(claimid) SORTKEY(claimid);

COPY transactional_claimaffiliation_dupes FROM :claimaffiliation_path CREDENTIALS :credentials EMPTYASNULL NULL AS 'NULL' ACCEPTINVCHARS IGNOREHEADER 1 MAXERROR 100 DELIMITER '|' BZIP2;

DROP TABLE IF EXISTS transactional_claimaffiliation CASCADE;
CREATE TABLE transactional_claimaffiliation (
        claimid      text ENCODE lzo,
        type         text ENCODE lzo,
        npi          text ENCODE lzo,
        fullname     text ENCODE lzo,
        firstname    text ENCODE lzo,
        middlename   text ENCODE lzo,
        lastname     text ENCODE lzo,
        taxonomy     text ENCODE lzo,
        orgnpi       text ENCODE lzo,
        orgname      text ENCODE lzo,
        addr1        text ENCODE lzo,
        addr2        text ENCODE lzo,
        city         text ENCODE lzo,
        state        text ENCODE lzo,
        zip          text ENCODE lzo,
        processdate  text ENCODE lzo
        ) DISTKEY(claimid) SORTKEY(claimid);

INSERT INTO transactional_claimaffiliation (SELECT DISTINCT * FROM transactional_claimaffiliation_dupes);
DROP TABLE transactional_claimaffiliation_dupes CASCADE;
