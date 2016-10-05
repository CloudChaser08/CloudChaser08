-- Sales tax may reveal geographic information that along with other public fields poses a
-- re-identification risk, nullify all sales tax fields

UPDATE pharmacyclaims_common_model
SET sales_tax=NULL,
submitted_flat_sales_tax=NULL,
submitted_percent_sales_tax_basis=NULL,
submitted_percent_sales_tax_rate=NULL,
submitted_percent_sales_tax_amount=NULL,
paid_flat_sales_tax=NULL,
paid_percent_sales_tax_basis=NULL,
paid_percent_sales_tax_rate=NULL,
paid_percent_sales_tax=NULL;
