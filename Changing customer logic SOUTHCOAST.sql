select *
from kustom_raw.sage_db_dbo.arm_master__customer
where customer = 'BRUJEN';

select *
from kustom_raw.sage_db_dbo.art_current__transaction
where invoice = '02-87439-F';

select *
from kustom_raw.sage_db_dbo.ara_activity__activity
where invoice ='02-87439-F';

select *
from kustom_raw.sage_db_dbo.vw_invoice_meta
where invoice = '04-72577-F';

select *
from kustom_raw.information_schema.views
where table_name like '%VW_INVOICE_META%';
use schema kustom_raw.sage_db_dbo;
select distinct 
        t1.dbid, 
        ifnull(
            UPPER(t1.customer),''
            ) 
        || ifnull(
            UPPER(t1.invoice),''
            ) 
        || ifnull(
            UPPER(t1.job),''
            ) 
        || ifnull(
            upper(t1.dbid),''
            ) 
        as uniqueid,
        upper(t1.customer) as id,
        initcap(replace(t2.name,'.','')) as name,
        upper(t1.invoice) as invoice,
        t1.job,initcap(t4.description) as description, 
        t2.telephone as phone,
        t4.cost_account_prefix, 
        t2.gl_prefix,
        t4.supervisor as supervisor,
        t4.estimator as estimator,
        t4.coordinator,
        t4.MARKETING as MARKETING,
        t4.jc_admin as admin
    from art_current__transaction t1
    LEFT JOIN ARM_MASTER__CUSTOMER t2 
    on t1.Customer = t2.Customer
        and t1.dbid = t2.dbid
    LEFT JOIN ara_activity__activity t3 
    on t1.invoice = t3.invoice 
        and t1.job = t3.job 
        and t1.customer = t3.customer 
        and t1.Status_Type = t3.Status_Type 
        and t1.Status_Date = t3.Status_Date 
        and t1.status_seq = t3.status_seq
    left join (
        select * 
        from jcm_master__job 
        where _fivetran_active = TRUE
        ) t4 
    on t1.job=t4.job
    where uniqueid <> t1.dbid
        and t1._fivetran_deleted = FALSE
        and t2._fivetran_deleted = FALSE
        and t3._fivetran_deleted = FALSE
        and t1.invoice ='02-88121-F';--79K records

select count(*)
from vw_invoice_meta;--80724
use role accountadmin;
--new view definition creation for invoice meta changes the join on customer to include DBID in the join condition
create or replace view KUSTOM_RAW.SAGE_DB_DBO.VW_INVOICE_META(
	DBID,
	UNIQUEID,
	ID,
	NAME,
	INVOICE,
	JOB,
	DESCRIPTION,
	PHONE,
	COST_ACCOUNT_PREFIX,
	GL_PREFIX,
	SUPERVISOR,
	ESTIMATOR,
	COORDINATOR,
    MARKETING,
	ADMIN
) as (
    select distinct 
        t1.dbid, 
        ifnull(
            UPPER(t1.customer),''
            ) 
        || ifnull(
            UPPER(t1.invoice),''
            ) 
        || ifnull(
            UPPER(t1.job),''
            ) 
        || ifnull(
            upper(t1.dbid),''
            ) 
        as uniqueid,
        upper(t1.customer) as id,
        initcap(replace(t2.name,'.','')) as name,
        upper(t1.invoice) as invoice,
        t1.job,initcap(t4.description) as description, 
        t2.telephone as phone,
        t4.cost_account_prefix, 
        t2.gl_prefix,
        t4.supervisor as supervisor,
        t4.estimator as estimator,
        t4.coordinator,
        t4.MARKETING as MARKETING,
        t4.jc_admin as admin
    from art_current__transaction t1
    LEFT JOIN ARM_MASTER__CUSTOMER t2 
    on t1.Customer = t2.Customer 
        and t1.dbid = t2.dbid
    LEFT JOIN ara_activity__activity t3 
    on t1.invoice = t3.invoice 
        and t1.job = t3.job 
        and t1.customer = t3.customer 
        and t1.Status_Type = t3.Status_Type 
        and t1.Status_Date = t3.Status_Date 
        and t1.status_seq = t3.status_seq
    left join (
        select * 
        from jcm_master__job 
        where _fivetran_active = TRUE
        ) t4 
    on t1.job=t4.job
    where uniqueid <> t1.dbid
        and t1._fivetran_deleted = FALSE
        and t2._fivetran_deleted = FALSE
        and t3._fivetran_deleted = FALSE
    -- where clauses have been removed but the resulting meta table will not be unique due to issues with blank invoices
);