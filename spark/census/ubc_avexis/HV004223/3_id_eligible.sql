SELECT *
FROM existing_ids_appended
WHERE
    hvid is null AND
    firstservicedate is not null AND
    gender is not null AND
    privateidone is not null
