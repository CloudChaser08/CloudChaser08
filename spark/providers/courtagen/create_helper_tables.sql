/*
 * There should be no more than 8 diagnoses in every row
 */
DROP TABLE IF EXISTS diagnosis_exploder;
CREATE TABLE diagnosis_exploder
    (n int)  
;
INSERT INTO diagnosis_exploder VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

/*
 * Some providers give us the full state name, but we only want the abbreviation
 */
DROP TABLE IF EXISTS state_abbr;
CREATE TABLE state_abbr (
    state string,
    abbr  string
);

INSERT INTO state_abbr VALUES 
("ALABAMA", "AL"),
("ALASKA", "AK"),
("ARIZONA", "AZ"),
("ARKANSAS", "AR"),
("CALIFORNIA", "CA"),
("COLORADO", "CO"),
("CONNECTICUT", "CT"),
("DELAWARE", "DE"),
("DISTRICT OF COLUMBIA", "DC"),
("FLORIDA", "FL"),
("GEORGIA", "GA"),
("HAWAII", "HI"),
("IDAHO", "ID"),
("ILLINOIS", "IL"),
("INDIANA", "IN"),
("IOWA", "IA"),
("KANSAS", "KS"),
("KENTUCKY", "KY"),
("LOUISIANA", "LA"),
("MAINE", "ME"),
("MARYLAND", "MD"),
("MASSACHUSETTS", "MA"),
("MICHIGAN", "MI"),
("MINNESOTA", "MN"),
("MISSISSIPPI", "MS"),
("MISSOURI", "MO"),
("MONTANA", "MT"),
("NEBRASKA", "NE"),
("NEVADA", "NV"),
("NEW HAMPSHIRE", "NH"),
("NEW JERSEY", "NJ"),
("NEW MEXICO", "NM"),
("NEW YORK", "NY"),
("NORTH CAROLINA", "NC"),
("NORTH DAKOTA", "ND"),
("OHIO", "OH"),
("OKLAHOMA", "OK"),
("OREGON", "OR"),
("PENNSYLVANIA", "PA"),
("RHODE ISLAND", "RI"),
("SOUTH CAROLINA", "SC"),
("SOUTH DAKOTA", "SD"),
("TENNESSEE", "TN"),
("TEXAS", "TX"),
("UTAH", "UT"),
("VERMONT", "VT"),
("VIRGINIA", "VA"),
("WASHINGTON", "WA"),
("WEST VIRGINIA", "WV"),
("WISCONSIN", "WI"),
("WYOMING", "WY");

/*
 * Result summary table
 */

DROP TABLE IF EXISTS summ;
CREATE TABLE summ (
    value string,
    name  string,
    desc  string
);

INSERT INTO summ VALUES
('-1', 'Inconclusive',    "The patient’s sample was sequenced, but a clinical report was not generated. (E.g. Research sample, insurance denial, etc.)"),
('0',  'Inconclusive',    "The patient’s sample was sequenced, but a clinical report was not generated. (E.g. Research sample, insurance denial, etc.)"),
('1',  'Negative',        "No variants were identified that are causal. (This category is no longer used as of Feb. 1, 2016.)"),
('2',  'Likely Negative', "No variants were identified in the panel genes OR No variants were identified that are likely causal. Most reports will have no front-paged variants, but likely negative reports may have carrier status or risk factor variants front-paged."),
('3',  'Uncertain',       "Variant(s) of uncertain significance were identified. These variants cannot be excluded based on clinical correlation, but there are no genes with a strong correlation to the patient’s phenotype."),
('4',  'Likely Positive', "A pathogenic/likely pathogenic variant was identified. These reports may be upgraded to positive after parental testing if the variant is de novo (for autosomal dominant conditions) or if the variants are in trans (for autosomal recessive conditions)."),
('5',  'Positive',        "A pathogenic/likely pathogenic variant was identified that is de novo (or in trans for >1 variant) and/or has a well-established association to the patient’s phenotype. Many reports will not be scored positive until parental testing is performed.");
