SELECT
	hvid,
	threedigitzip,
	yearofbirth,
	gender,
	age,
	state,
	hvjoinkey
 FROM matching_payload
GROUP BY
	hvid,
	threedigitzip,
	yearofbirth,
	gender,
	age,
	state,
	hvjoinkey
