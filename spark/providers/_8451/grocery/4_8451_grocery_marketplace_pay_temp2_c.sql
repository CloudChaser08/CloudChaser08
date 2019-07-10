SELECT * 
FROM 8451_grocery_marketplace_pay_temp2_cache pay1
WHERE EXISTS
/* Find personid values that have another row with
   only NULL info on it. */
    (
        SELECT 1
         FROM 8451_grocery_marketplace_pay_temp1 pay2
        WHERE pay1.personid = pay2.personid
          AND
            (
                pay1.hvid IS NOT NULL
            AND pay2.hvid IS NULL
            )
          AND
            (
                pay1.threedigitzip IS NOT NULL
            AND pay2.threedigitzip IS NULL
            )
          AND
            (
                pay1.yearofbirth IS NOT NULL
            AND pay2.yearofbirth IS NULL
            )
          AND
            (
                pay1.gender IS NOT NULL
            AND pay2.gender IS NULL
            )
          AND
            (
                pay1.age IS NOT NULL
            AND pay2.age IS NULL
            )
          AND
            (
                pay1.state IS NOT NULL
            AND pay2.state IS NULL
            )
    )
