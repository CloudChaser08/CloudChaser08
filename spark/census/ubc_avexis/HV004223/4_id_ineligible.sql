SELECT *
FROM existing_ids_appended
WHERE
    hvid is null AND (
        firstservicedate is null OR
        gender is null OR
        privateidone is null
    )
