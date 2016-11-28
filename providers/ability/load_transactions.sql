DROP TABLE IF EXISTS transactional_header;
CREATE TABLE transactional_header (
        claimid                    text ENCODE lzo,
        type                       text ENCODE lzo,
        status                     text ENCODE lzo,
        location                   text ENCODE lzo,
        pregnancyindicator         text ENCODE lzo,
        relatedcause               text ENCODE lzo,
        maritalstatus              text ENCODE lzo,
        mammographycertification   text ENCODE lzo,
        clia                       text ENCODE lzo,
        epsdt                      text ENCODE lzo,
        claimfrequencycode         text ENCODE lzo,
        medicareassignment         text ENCODE lzo,
        institutionaltype          text ENCODE lzo,
        totalcharge                text ENCODE lzo,
        patientpaid                text ENCODE lzo,
        drgcode                    text ENCODE lzo,
        onsetdate                  text ENCODE lzo,
        admissiondate              text ENCODE lzo,
        admissiontype              text ENCODE lzo,
        admissionsource            text ENCODE lzo,
        dischargestatus            text ENCODE lzo,
        admissiondiagnosis         text ENCODE lzo,
        dischargedate              text ENCODE lzo,
        startdate                  text ENCODE lzo,
        enddate                    text ENCODE lzo,
        claimfilingdate            text ENCODE lzo,
        claimsubmittersidentifier  text ENCODE lzo,
        lengthofstay               text ENCODE lzo,
        processdate                text ENCODE lzo,
        claimid2                   text ENCODE lzo,
        hvjoinkey                  text ENCODE lzo
        ) DISTKEY(claimid) SORTKEY(claimid);

COPY transactional_header FROM :header_path CREDENTIALS :credentials EMPTYASNULL ACCEPTINVCHARS DELIMITER '|';

DROP TABLE IF EXISTS transactional_serviceline;
CREATE TABLE transactional_serviceline (
        servicelineid             text ENCODE lzo,
        claimid                   text ENCODE lzo,
        servicelocationtaxid      text ENCODE lzo,
        renderingpractionertaxid  text ENCODE lzo,
        referringpractionertaxid  text ENCODE lzo,
        placeofservice            text ENCODE lzo,
        facilitytype              text ENCODE lzo,
        procedurecode             text ENCODE lzo,
        amount                    text ENCODE lzo,
        qualifier                 text ENCODE lzo,
        modifier1                 text ENCODE lzo,
        modifier2                 text ENCODE lzo,
        modifier3                 text ENCODE lzo,
        modifier4                 text ENCODE lzo,
        description               text ENCODE lzo,
        linecharge                text ENCODE lzo,
        paid                      text ENCODE lzo,
        revenuecode               text ENCODE lzo,
        diagnosiscodepointer1     text ENCODE lzo,
        diagnosiscodepointer2     text ENCODE lzo,
        diagnosiscodepointer3     text ENCODE lzo,
        diagnosiscodepointer4     text ENCODE lzo,
        servicestart              text ENCODE lzo,
        serviceend                text ENCODE lzo,
        mammographycertification  text ENCODE lzo,
        clia                      text ENCODE lzo,
        emergency                 text ENCODE lzo,
        epsdt                     text ENCODE lzo,
        drugcode                  text ENCODE lzo,
        drugprice                 text ENCODE lzo,
        drugquantity              text ENCODE lzo,
        drugunit                  text ENCODE lzo,
        lineitemcontrolnumber     text ENCODE lzo,
        sequencenumber            text ENCODE lzo,
        processdate               text ENCODE lzo
        ) DISTKEY(claimid) SORTKEY(claimid);

COPY transactional_serviceline FROM :serviceline_path CREDENTIALS :credentials EMPTYASNULL ACCEPTINVCHARS IGNOREHEADER 1 DELIMITER '|';

DROP TABLE IF EXISTS transactional_servicelineaffiliation;
CREATE TABLE transactional_servicelineaffiliation (
        servicelineid  text ENCODE lzo,
        type           text ENCODE lzo,
        npi            text ENCODE lzo,
        fullname       text ENCODE lzo,
        firstname      text ENCODE lzo,
        middlename     text ENCODE lzo,
        lastname       text ENCODE lzo,
        taxonomy       text ENCODE lzo,
        orgnpi         text ENCODE lzo,
        orgname        text ENCODE lzo,
        addr1          text ENCODE lzo,
        addr2          text ENCODE lzo,
        city           text ENCODE lzo,
        state          text ENCODE lzo,
        zip            text ENCODE lzo,
        processdate    text ENCODE lzo,
        claimid        text ENCODE lzo
        ) DISTKEY(claimid) SORTKEY(claimid);

COPY transactional_servicelineaffiliation FROM :servicelineaffiliation_path CREDENTIALS :credentials EMPTYASNULL ACCEPTINVCHARS IGNOREHEADER 1 DELIMITER '|';

DROP TABLE IF EXISTS transactional_diagnosis;
CREATE TABLE transactional_diagnosis (
        claimid             text ENCODE lzo,
        type                text ENCODE lzo,
        diagnosiscode       text ENCODE lzo,
        presentonadmission  text ENCODE lzo,
        sequencenumber      text ENCODE lzo,
        processdate         text ENCODE lzo
        ) DISTKEY(claimid) SORTKEY(claimid);

COPY transactional_diagnosis FROM :diagnosis_path CREDENTIALS :credentials EMPTYASNULL ACCEPTINVCHARS IGNOREHEADER 1 DELIMITER '|';

DROP TABLE IF EXISTS transactional_procedure;
CREATE TABLE transactional_procedure (
        claimid             text ENCODE lzo,
        type                text ENCODE lzo,
        procedurecode       text ENCODE lzo,
        proceduredate       text ENCODE lzo,
        sequencenumber      text ENCODE lzo,
        processdate         text ENCODE lzo
        ) DISTKEY(claimid) SORTKEY(claimid);

COPY transactional_procedure FROM :procedure_path CREDENTIALS :credentials EMPTYASNULL ACCEPTINVCHARS IGNOREHEADER 1 DELIMITER '|';

DROP TABLE IF EXISTS transactional_billing;
CREATE TABLE transactional_billing (
        claimid      text ENCODE lzo,
        npi          text ENCODE lzo,
        taxid        text ENCODE lzo,
        orgname      text ENCODE lzo,
        fullname     text ENCODE lzo,
        firstname    text ENCODE lzo,
        middlename   text ENCODE lzo,
        lastname     text ENCODE lzo,
        addr1        text ENCODE lzo,
        addr2        text ENCODE lzo,
        city         text ENCODE lzo,
        state        text ENCODE lzo,
        zip          text ENCODE lzo,
        taxonomy     text ENCODE lzo,
        stlic        text ENCODE lzo,
        ssn          text ENCODE lzo,
        upin         text ENCODE lzo,
        processdate  text ENCODE lzo
        ) DISTKEY(claimid) SORTKEY(claimid);

COPY transactional_billing FROM :billing_path CREDENTIALS :credentials EMPTYASNULL ACCEPTINVCHARS IGNOREHEADER 1 DELIMITER '|';

DROP TABLE IF EXISTS transactional_payer;
CREATE TABLE transactional_payer (
        claimid                text ENCODE lzo,
        sourcepayerid          text ENCODE lzo,
        payerid                text ENCODE lzo,
        claimfileindicator     text ENCODE lzo,
        name                   text ENCODE lzo,
        addr1                  text ENCODE lzo,
        addr2                  text ENCODE lzo,
        city                   text ENCODE lzo,
        state                  text ENCODE lzo,
        zip                    text ENCODE lzo,
        payerclassificationid  text ENCODE lzo,
        payerclassification    text ENCODE lzo,
        destinationpayer       text ENCODE lzo,
        sequencenumber         text ENCODE lzo,
        processdate            text ENCODE lzo,
        hvjoinkey              text ENCODE lzo
        ) DISTKEY(claimid) SORTKEY(claimid);

COPY transactional_payer FROM :payer_path CREDENTIALS :credentials EMPTYASNULL ACCEPTINVCHARS DELIMITER '|';

