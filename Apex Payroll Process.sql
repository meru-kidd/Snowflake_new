use schema kustom_raw.share_point_apex_payroll;

select 
from _09_26_original_sheet_1;

SELECT * 
FROM information_schema.columns 
WHERE table_name = '_09_26_ORIGINAL_SHEET_1'
ORDER BY ordinal_position;