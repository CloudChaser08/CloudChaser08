select crs.concat_unique_fields, crs.date_filled, alt.date_delivered_fulfilled, 
crs.rx_number, crs.input_file_name, crs.cob_count, crs.pharmacy_ncpdp_number,crs.claim_indicator,crs.ndc_code,crs.datavant_token1,crs.datavant_token2
from txn crs
Join raw.pdx_rx_txn_alltime alt
on COALESCE(crs.date_filled,'19000101') = COALESCE(alt.date_filled,'19000101')
and COALESCE(crs.rx_number,'XXX') = COALESCE(alt.prescription_number_filler,'XXX')
and COALESCE(crs.cob_count,'XXX') = COALESCE(alt.coordination_of_benefits_counter,'XXX')
and COALESCE(crs.pharmacy_ncpdp_number,'XXX') = COALESCE(alt.pharmacy_ncpdp_number, 'XXX')
and COALESCE(crs.claim_indicator, 'XXX') = COALESCE(alt.claim_indicator,'XXX')
and COALESCE(crs.ndc_code,'XXX') = COALESCE(alt.dispensed_ndc_number,'XXX')