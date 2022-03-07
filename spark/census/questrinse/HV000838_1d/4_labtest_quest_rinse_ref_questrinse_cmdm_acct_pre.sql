SELECT
        x.act_idw_lab_code,
        x.act_client_no
FROM (
         SELECT
          a.act_idw_lab_code,
          a.act_client_no,
          a.act_spclty_tp,
          a.act_spclty_cd,
          a.act_spclty_desc,
          a.act_addr_tp
        FROM  ref_questrinse_cmdm a
        WHERE UPPER(a.act_addr_tp) = 'DELIVER RESULTS'
        GROUP BY
          a.act_idw_lab_code,
          a.act_client_no,
          a.act_spclty_tp,
          a.act_spclty_cd,
          a.act_spclty_desc,
          a.act_addr_tp
     ) x
GROUP BY
x.act_idw_lab_code,
x.act_client_no
HAVING count(*) = 1 -- exclude the rows that have more than a pair of act_idw_lab_code/act_client_no
