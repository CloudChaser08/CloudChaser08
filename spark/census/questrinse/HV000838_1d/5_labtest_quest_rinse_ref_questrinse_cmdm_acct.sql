SELECT  mstr.act_idw_lab_code,
        mstr.act_client_no,
        mstr.act_spclty_tp,
        mstr.act_spclty_cd,
        mstr.act_spclty_desc,
        mstr.act_addr_tp
FROM ref_questrinse_cmdm mstr
INNER JOIN labtest_quest_rinse_ref_questrinse_cmdm_acct_pre key_rows
ON  mstr.act_idw_lab_code = key_rows.act_idw_lab_code
AND mstr.act_client_no = key_rows.act_client_no
WHERE UPPER(mstr.act_addr_tp) = 'DELIVER RESULTS'
GROUP BY
     mstr.act_idw_lab_code,
     mstr.act_client_no,
     mstr.act_spclty_tp,
     mstr.act_spclty_cd,
     mstr.act_spclty_desc,
     mstr.act_addr_tp
