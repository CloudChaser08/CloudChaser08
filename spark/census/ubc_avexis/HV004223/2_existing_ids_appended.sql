SELECT
    ri.*,
    hai.hvid
FROM reconstructed_input ri
    LEFT JOIN historically_assigned_ids hai ON
        ri.privateidone = hai.privateidone AND
        ri.firstservicedate = hai.firstservicedate AND
        ri.gender = hai.gender AND
        ri.privateidone is not null AND
        ri.firstservicedate is not null AND
        ri.gender is not null
