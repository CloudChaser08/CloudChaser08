SELECT
    pay1.hvid,
    pay1.personid,
    pay1.threedigitzip,
    pay1.yearofbirth,
    pay1.gender,
    pay1.age,
    pay1.state
 FROM 8451_grocery_marketplace_pay_temp1 pay1
INNER JOIN
(
    SELECT
        personid,
        COUNT(*) AS row_cnt
     FROM 8451_grocery_marketplace_pay_temp1
    GROUP BY 1
    HAVING row_cnt = 1
) pay2
   ON pay1.personid = pay2.personid
