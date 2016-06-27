update emdeon_claim_norm
set hvid = (select l.hvid from claim_link l where l.claim_number = emdeon_claim_norm.claim_id)
where (date_received < '2016-01-01'::date and date_received > '2015-12-01'::date);
