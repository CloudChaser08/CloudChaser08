----------------------------------------------------------
-- qc masterset: QC: Numbers must match
----------------------------------------------------------
select
    a.*,
    b.new_master
from
    (
        select
            'Total adults' as src,
            count(distinct adult_hvid) as backup
        from _mom_masterset
  UNION
        select
            'Total babies' as src,
            count(distinct child_hvid) as backup
        from _mom_masterset
        where child_hvid is not null
  UNION
        select
            'Total HVIDs' as src,
            count(distinct hvid) as backup
        from
            (
                select
                    adult_hvid as hvid
                from _mom_masterset
            UNION
                select
                    child_hvid as hvid
                from _mom_masterset
                where child_hvid is not null
            )
  ) a
  left outer join
    (
        select
            'Total adults' as src,
            count(distinct adult_hvid) as new_master
        from mom_masterset_prod
    UNION
        select
            'Total babies' as src,
            count(distinct child_hvid) as new_master
        from mom_masterset_prod
        where child_hvid is not null
    UNION
        select
            'Total HVIDs' as src,
            count(distinct hvid) as new_master
        from
        (
                select
                    adult_hvid as hvid
                from mom_masterset_prod
            UNION
                select
                    child_hvid as hvid
                from mom_masterset_prod
                where child_hvid is not null
        )
  ) b on a.src=b.src

