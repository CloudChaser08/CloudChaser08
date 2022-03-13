##the extra shift will be applied to dates that are indicative of patient activity; processing/system dates are not necessary
##gathering all date shift case statements into this single section for ease of understanding
_DX_ = """ case when b.flag='Y' and (datediff(b.flag_dt,date_service) between -1 and 1 or datediff(b.flag_dt,date_service_end) between -1 and 1 or datediff(b.flag_dt,inst_date_admitted) between -1 and 1 or datediff(b.flag_dt,inst_date_discharged) between -1 and 1) then """
_DX_DATE_RECEIVED_ = _DX_ + """ date_add(date_received,base_shift+extra_shift) else date_add(date_received,base_shift) """
_DX_DATE_SERVICE_ = _DX_ + """ date_add(date_service,base_shift+extra_shift) else date_add(date_service,base_shift) """
_DX_DATE_SERVICE_END_ = _DX_ + """ date_add(date_service_end,base_shift+extra_shift) else date_add(date_service_end,base_shift) """
_DX_INST_DATE_ADMITTED_ = _DX_ + """ date_add(inst_date_admitted,base_shift+extra_shift) else date_add(inst_date_admitted,base_shift) """
_DX_INST_DATE_DISCHARGED_ = _DX_ + """ date_add(inst_date_discharged,base_shift+extra_shift) else date_add(inst_date_discharged,base_shift) """

_RX_ = """ case when b.flag='Y' and (datediff(b.flag_dt,date_written) between -1 and 1 or datediff(b.flag_dt,date_service) between -1 and 1 or datediff(b.flag_dt,discharge_date) between -1 and 1) then """
_RX_DATE_WRITTEN_ = _RX_ + """ date_add(date_written,base_shift+extra_shift) else date_add(date_written,base_shift) """
_RX_DATE_SERVICE_ = _RX_ + """ date_add(date_service,base_shift+extra_shift) else date_add(date_service,base_shift) """
_RX_DATE_AUTHORIZED_ = _RX_ + """ date_add(date_authorized,base_shift+extra_shift) else date_add(date_authorized,base_shift) """
_RX_DISCHARGE_DATE_ = _RX_ + """ date_add(discharge_date,base_shift+extra_shift) else date_add(discharge_date,base_shift) """
_RX_OTHER_PAYER_DATE_ = _RX_ + """ date_add(other_payer_date,base_shift+extra_shift) else date_add(other_payer_date,base_shift) """


##enrollment does not indicate a person is alive/dead so extra shifting is NA
_ENROLL_SOURCE_RECORD_DATE_ = """ date_add(source_record_date,base_shift) """
_ENROLL_DATE_START_ = """ date_add(date_start,base_shift) """
_ENROLL_DATE_END_ = """ date_add(date_end,base_shift) """


##base shift adult index pregnancy date; extra shift is only applicable to children
_INDEX_PREG_ = """ date_add(index_preg,b.base_shift) """
##per the cert the adult death is to be truncated, then base shifted, then truncated again
##the first trunc is executed in steps above (see death paragraph), the base shift and final trunc is executed below
_ADULT_DEATH_MONTH_ = """ case when a.adult_death_ind='Y' then trunc(date_add(a.adult_death_month,b.base_shift),'month') else NULL """


##extra shift is always applied to child death date
_CHILD_DEATH_DATE_ = """ case when c.flag='Y' then date_add(child_death_date,c.base_shift+c.extra_shift) else NULL """
##base shift is always applied to the child dob
_CHILD_DOB_ = """ date_add(child_dob,c.base_shift) """
