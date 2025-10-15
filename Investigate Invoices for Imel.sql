use schema kustom_raw.sage_db_dbo;

select *
from vw_invoice_meta
where job = '25-02-87195';

select *
from vw_invoice_amts
where job = '25-02-87195'; --all dates 9/2

select *
from vw_invoice_pmts
where job = '25-02-87195';--payment date 9/16, due date 9/2

select *
from vw_job_gl_metrics
where job = '25-02-87195'
order by date_stamp desc;--cannot find activity on 9/2

select *
from vw_jct_current__transaction
where job ='25-02-87195'
order by accounting_date desc;--transaction relevant appears on 9/16 with date stamp 9/16

select *
from jct_current__transaction
where job ='25-02-87195';

select *
from glt_current__transaction
where job = '25-02-87195'
order by accounting_date desc;

select *
from ara_activity__activity
where job = '25-02-87195';

SELECT *
from vw_jct_current__transaction
where job = '24-08-31480' and transaction_type = 'Scheduled value'
order by transaction_date desc;

select *
from kustom_prepared.prepared_copy.vw_revenue_model_2
where job = '24-08-31480';

select *
from kustom_prepared.sage_dash_comparison.vw_job_data_history_v1
where job = '24-08-31480';