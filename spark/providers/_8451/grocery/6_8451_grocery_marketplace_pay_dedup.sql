SELECT
    hvid,
    personid,
    threedigitzip,
    yearofbirth,
    gender,
    age,
    state
 FROM
(
    SELECT
        pay1.hvid,
        pay1.personid,
        pay1.threedigitzip,
        pay1.yearofbirth,
        pay1.gender,
        pay1.age,
        pay1.state
     FROM 8451_grocery_marketplace_pay_temp2 pay1
    UNION ALL
    SELECT
        pay2.hvid,
        pay2.personid,
        pay2.threedigitzip,
        pay2.yearofbirth,
        pay2.gender,
        pay2.age,
        pay2.state
     FROM 8451_grocery_marketplace_pay_temp3 pay2
)
GROUP BY 1, 2, 3, 4, 5, 6, 7
