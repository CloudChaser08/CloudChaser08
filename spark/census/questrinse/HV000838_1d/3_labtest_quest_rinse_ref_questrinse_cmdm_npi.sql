select ind_npi,
   ind_full_nm,
   ind_spclty,
   ind_spclty_cd,
   ind_spclty_desc,
   ind_spclty_tp_secondary,
   ind_spclty_cd_secondary,
   ind_spclty_desc_secondary,
   ind_spclty_tp_tertiary,
   ind_spclty_cd_tertiary,
   ind_spclty_desc_tertiary
from (
SELECT
   ind_npi,
   ind_full_nm,
   ind_spclty,
   ind_spclty_cd,
   ind_spclty_desc,
   ind_spclty_tp_secondary,
   ind_spclty_cd_secondary,
   ind_spclty_desc_secondary,
   ind_spclty_tp_tertiary,
   ind_spclty_cd_tertiary,
   ind_spclty_desc_tertiary,
   ROW_NUMBER() OVER ( PARTITION BY
                ind_npi
                ORDER BY
                  ind_npi,
                  ind_spclty_cd DESC
                ) AS row_num
FROM
    (
      SELECT
        ind_npi,
        ind_full_nm,
        ind_spclty,
        ind_spclty_cd,
        ind_spclty_desc,
        ind_spclty_tp_secondary,
        ind_spclty_cd_secondary,
        ind_spclty_desc_secondary,
        ind_spclty_tp_tertiary,
        ind_spclty_cd_tertiary,
        ind_spclty_desc_tertiary
        FROM ref_questrinse_cmdm
        WHERE ind_npi is not null
        GROUP bY
          ind_npi,
          ind_full_nm,
          ind_spclty,
          ind_spclty_cd,
          ind_spclty_desc,
          ind_spclty_tp_secondary,
          ind_spclty_cd_secondary,
          ind_spclty_desc_secondary,
          ind_spclty_tp_tertiary,
          ind_spclty_cd_tertiary,
          ind_spclty_desc_tertiary
        )
)
WHERE row_num  = 1