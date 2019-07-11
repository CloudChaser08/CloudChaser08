SELECT
    pay1.hvid,
    pay1.personid,
    pay1.threedigitzip,
    pay1.yearofbirth,
    pay1.gender,
    pay1.age,
    pay1.state,
    pay1.best_row_num
 FROM 8451_grocery_marketplace_pay_temp1 pay1
/* Select the best row for each personid, based on how much info is present. */
INNER JOIN
(
    SELECT
        personid,
        MIN(best_row_num) AS best_row_num
     FROM 8451_grocery_marketplace_pay_temp1
    GROUP BY 1
) pay2
   ON pay1.personid = pay2.personid
  AND pay1.best_row_num = pay2.best_row_num
/* Only select personids we haven't loaded yet. */
WHERE NOT EXISTS
    (
        SELECT 1
         FROM 8451_grocery_marketplace_pay_temp2 pay3
        WHERE pay1.personid = pay3.personid
    )
