DROP TABLE IF EXISTS matching_payload_fixed;
CREATE TABLE matching_payload_fixed AS
SELECT
    mat.personId as personId,
    COALESCE(par.parentId, mat.hvid) as hvid,
    mat.gender,
    mat.state,
    mat.yearOfBirth
FROM matching_payload mat
    LEFT JOIN parent_child_map par USING (hvid)
    DISTRIBUTE BY personId
    ;

DROP TABLE matching_payload;
ALTER TABLE matching_payload_fixed RENAME TO matching_payload;