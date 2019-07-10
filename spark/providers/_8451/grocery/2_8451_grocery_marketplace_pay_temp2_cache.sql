SELECT
    pay1.hvid,
    pay1.personid,
    pay1.threedigitzip,
    pay1.yearofbirth,
    pay1.gender,
    pay1.age,
    pay1.state
 FROM 8451_grocery_marketplace_pay_temp1 pay1
WHERE
/* Only select rows with info on them. */
    (
        pay1.hvid IS NOT NULL
     OR pay1.threedigitzip IS NOT NULL
     OR pay1.yearofbirth IS NOT NULL
     OR pay1.gender IS NOT NULL
     OR pay1.age IS NOT NULL
     OR pay1.state IS NOT NULL
    )
