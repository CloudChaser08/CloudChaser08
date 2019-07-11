SELECT * 
FROM 8451_grocery_marketplace_pay_temp2_cache pay1
WHERE EXISTS
/* Find personid values that have another row with
   different info (not NULL) on it. */
    (
        SELECT 1
         FROM 8451_grocery_marketplace_pay_temp1 pay2
        WHERE pay1.personid = pay2.personid
          AND
            (
                (
                    pay1.hvid IS NOT NULL
                AND pay2.hvid IS NOT NULL
                AND COALESCE(pay1.hvid, '') <> COALESCE(pay2.hvid, '')
                )
             OR
                (
                    pay1.threedigitzip IS NOT NULL
                AND pay2.threedigitzip IS NOT NULL
                AND COALESCE(pay1.threedigitzip, '') <> COALESCE(pay2.threedigitzip, '')
                )
             OR
                (
                    pay1.yearofbirth IS NOT NULL
                AND pay2.yearofbirth IS NOT NULL
                AND COALESCE(pay1.yearofbirth, '') <> COALESCE(pay2.yearofbirth, '')
                )
             OR
                (
                    pay1.gender IS NOT NULL
                AND pay2.gender IS NOT NULL
                AND COALESCE(pay1.gender, '') <> COALESCE(pay2.gender, '')
                )
             OR
                (
                    pay1.age IS NOT NULL
                AND pay2.age IS NOT NULL
                AND COALESCE(pay1.age, '') <> COALESCE(pay2.age, '')
                )
             OR
                (
                    pay1.state IS NOT NULL
                AND pay2.state IS NOT NULL
                AND COALESCE(pay1.state, '') <> COALESCE(pay2.state, '')
                )
            )
    )
