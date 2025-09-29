SELECT 
    '-- View: ' || table_name || '\n' ||
    'CREATE OR REPLACE VIEW ' || table_schema || '.' || table_name || ' AS\n' ||
    view_definition || '\n\n' AS ddl_script
FROM information_schema.views
where view_definition is not null
ORDER BY table_name;

DDL_SCRIPT
-- View: AR_AGING_SAGE
CREATE OR REPLACE VIEW SAGE_DB_DBO.AR_AGING_SAGE AS
create or replace view KUSTOM_RAW.SAGE_DB_DBO.AR_AGING_SAGE(
	ID,
	NAME,
	PHONE,
	INVOICE,
	JOB,
	DESCRIPTION,
	DUE_DATE,
	ISSUED_INVOICE_AMT,
	CASH_RECEIPT_AMT,
	CASH_RECEIPT_ADJUSTMENT_AMT,
	INVOICE_ADJUSTMENT_AMT,
	ISSUED_INVOICE_ACCT_DATE,
	CASH_RECEIPT_ACCT_DATE,
	CASH_RECEIPT_ADJUSTMENT_ACCT_DATE,
	INVOICE_ADJUSTMENT_ACCT_DATE
) as
(
with firsttable as
(SELECT t1.Customer as ID,t2.Name as Name, t2.Telephone as Phone, min(t1.invoice) as Invoice, 
	t1.Status_Date as Status_Date, t1.due_date, t4.job as job, t4.Description as Description,
	sum(t1.Amount) as transamount,
	t1.status_type as Status_Type, t1.Sequence as Sequence, t1.Accounting_Date, t3.Invoice_Date, 
	t1.Transaction_Type
FROM ART_CURRENT__TRANSACTION t1
LEFT JOIN ARM_MASTER__CUSTOMER t2 on t1.Customer = t2.Customer
LEFT JOIN ARA_ACTIVITY__ACTIVITY t3 on t1.Customer = t3.Customer and 
	t1.Status_Type = t3.Status_Type and 
	t1.Status_Date = t3.Status_Date and 
	t1.status_seq = t3.status_seq and 
    t1.transaction_type = t3.activity_type
LEFT JOIN JCM_MASTER__JOB t4 on t1.job = t4.job
where t1.Transaction_Type NOT IN('Stored materials','NSF bank charge', 'Finance charge')
	and t1.Customer <>''
    and t1.transaction_type <> 'Invoice'
    and t1._fivetran_deleted = FALSE
    and t2._fivetran_deleted = FALSE
    and t3._fivetran_deleted = FALSE
    and t4._fivetran_active = TRUE
group by t1.Customer, t2.Name,t2.Telephone, t1.Status_Date, t4.job,t4.Description, t1.status_type, t1.sequence, t1.Accounting_Date, t3.Invoice_Date,
	t1.Transaction_Type,t1.due_date
),
--select * from firsttable where id = 'butuni' order by invoice;
unpivotted_table as (
select id,name,phone,invoice,Status_Date,Accounting_Date,Invoice_Date,
    job,description, transaction_type, transamount,due_date
from firsttable
)
--below allows for check of firsttable
--select * from firsttable where id = 'butuni' order by invoice desc;
, amt_table as
(SELECT id,name, phone, invoice,job,description,due_date, "'Issued invoice'" as issued_invoice_amt, "'Cash receipt'" as Cash_Receipt_Amt, "'Cash recpt adjustmnt'" as Cash_Receipt_Adjustment_Amt, "'Invoice adjustment'" as invoice_adjustment_amt
FROM (
    SELECT ID,name,phone,invoice,job,description,transaction_type,transamount,due_date from unpivotted_table
)
    PIVOT(SUM(transamount) for transaction_type in ('Issued invoice','Cash receipt','Cash recpt adjustmnt','Invoice adjustment'))
), acct_date_table as 
(SELECT id,name, phone, invoice,job,description,due_date, "'Issued invoice'" as issued_invoice_acct_date, "'Cash receipt'" as Cash_Receipt_acct_date, "'Cash recpt adjustmnt'" as Cash_Receipt_Adjustment_acct_date, "'Invoice adjustment'" as invoice_adjustment_acct_date
FROM (
    SELECT ID,name,phone,invoice,job,description,due_Date,transaction_type,accounting_date from unpivotted_table
)
    PIVOT(MAX(accounting_date) for transaction_type in ('Issued invoice','Cash receipt','Cash recpt adjustmnt','Invoice adjustment'))
)
SELECT t1.id, t1.name, t1.phone, t1.invoice, t1.job,t1.description,t1.due_date, t1. issued_invoice_amt, t1.Cash_Receipt_amt,t1.Cash_Receipt_Adjustment_Amt, t1.invoice_adjustment_amt, t2.issued_invoice_acct_date,t2.Cash_Receipt_acct_date,t2.Cash_Receipt_Adjustment_acct_date,t2.invoice_adjustment_acct_date
FROM amt_table t1
LEFT JOIN acct_date_table t2
ON t1.ID=t2.ID and t1.invoice = t2.invoice
);


-- View: INT_SAGE_INTACCT__ACCOUNT_CLASSIFICATIONS
CREATE OR REPLACE VIEW SAGE_INTACCT_SAGE_INTACCT.INT_SAGE_INTACCT__ACCOUNT_CLASSIFICATIONS AS
create or replace   view KUSTOM_RAW.sage_intacct_sage_intacct.int_sage_intacct__account_classifications
  
  
  
  
  as (
    with gl_account as (
    select *
    from KUSTOM_RAW.sage_intacct_sage_intacct_staging.stg_sage_intacct__gl_account
), 

final as (
    select
        account_no,
        account_type,
        category,
        closing_account_title,
        case
            when category in ('Inventory','Fixed Assets','Other Current Assets','Cash and Cash Equivalents','Intercompany Receivable','Accounts Receivable','Deposits and Prepayments','Goodwill','Intangible Assets','Short-Term Investments','Inventory','Accumulated Depreciation','Other Assets','Unrealized Currency Gain/Loss','Patents','Investment in Subsidiary','Escrows and Reserves','Long Term Investments') then 'Asset'
            when category in ('Partners Equity','Retained Earnings','Dividend Paid') then 'Equity'
            when category in ('Advertising and Promotion Expense','Other Operating Expense','Cost of Sales Revenue', 'Professional Services Expense','Cost of Services Revenue','Payroll Expense','Payroll Taxes','Travel Expense','Cost of Goods Sold','Other Expenses','Compensation Expense','Federal Tax','Depreciation Expense') then 'Expense'
            when category in ('Accounts Payable','Other Current Liabilities','Accrued Liabilities','Note Payable - Current','Deferred Taxes Liabilities - Long Term','Note Payable - Long Term','Other Liabilities','Deferred Revenue - Current') then 'Liability'
            when category in ('Revenue','Revenue - Sales','Dividend Income','Revenue - Other','Other Income','Revenue - Services','Revenue - Products') then 'Revenue'
            when (normal_balance = 'debit' and account_type = 'balancesheet') and category not in ('Inventory','Fixed Assets','Other Current Assets','Cash and Cash Equivalents','Intercompany Receivable','Accounts Receivable','Deposits and Prepayments','Goodwill','Intangible Assets','Short-Term Investments','Inventory','Accumulated Depreciation','Other Assets','Unrealized Currency Gain/Loss','Patents','Investment in Subsidiary','Escrows and Reserves','Long Term Investments') then 'Asset'
            when (normal_balance = 'debit' and account_type = 'incomestatement') and category not in ('Advertising and Promotion Expense','Other Operating Expense','Cost of Sales Revenue', 'Professional Services Expense','Cost of Services Revenue','Payroll Expense','Payroll Taxes','Travel Expense','Cost of Goods Sold','Other Expenses','Compensation Expense','Federal Tax','Depreciation Expense') then 'Expense'
            when (normal_balance = 'credit' and account_type = 'balancesheet' and category not in ('Accounts Payable','Other Current Liabilities','Accrued Liabilities','Note Payable - Current','Deferred Taxes Liabilities - Long Term','Note Payable - Long Term','Other Liabilities','Deferred Revenue - Current') or category not in ('Partners Equity','Retained Earnings','Dividend Paid')) then 'Liability'
            when (normal_balance = 'credit' and account_type = 'incomestatement') and category not in ('Revenue','Revenue - Sales','Dividend Income','Revenue - Other','Other Income','Revenue - Services','Revenue - Products') then 'Revenue'
        end as classification,
        normal_balance, 
        title as account_title

        --The below script allows for pass through columns.
        
    from gl_account
)

select *
from final
  )
/* {"app": "dbt", "dbt_version": "1.10.11", "profile_name": "fivetran", "target_name": "prod", "node_id": "model.sage_intacct.int_sage_intacct__account_classifications"} */;


-- View: INT_SAGE_INTACCT__GENERAL_LEDGER_BALANCES
CREATE OR REPLACE VIEW SAGE_INTACCT_SAGE_INTACCT.INT_SAGE_INTACCT__GENERAL_LEDGER_BALANCES AS
create or replace   view KUSTOM_RAW.sage_intacct_sage_intacct.int_sage_intacct__general_ledger_balances
  
  
  
  
  as (
    with general_ledger as (
    select *
    from KUSTOM_RAW.sage_intacct_sage_intacct.sage_intacct__general_ledger
), 

gl_accounting_periods as (
    select *
    from KUSTOM_RAW.sage_intacct_sage_intacct.int_sage_intacct__general_ledger_date_spine
), 


gl_period_balances_is as (
    select 
        account_no,
        account_title,
        book_id,
        category,
        classification,
        currency, 
        entry_state,
        account_type,
        cast(date_trunc('month', entry_date_at) as date) as date_month, 
        cast(date_trunc('year', entry_date_at) as date) as date_year

        
        , 
        sum(amount) as period_amount
    from general_ledger
    where account_type = 'incomestatement'
    
    group by 1,2,3,4,5,6,7,8,9,10

), 

gl_period_balances_bs as (
    select 
        account_no,
        account_title,
        book_id,
        category,
        classification,
        currency,
        entry_state,
        account_type,
        cast(date_trunc('month', entry_date_at) as date) as date_month, 
        cast(date_trunc('year', entry_date_at) as date) as date_year

        
        ,
        sum(amount) as period_amount
    from general_ledger
    where account_type = 'balancesheet'
    
    group by 1,2,3,4,5,6,7,8,9,10

), 

gl_period_balances as (
    select *
    from gl_period_balances_bs

    union all

    select *
    from gl_period_balances_is

),

gl_cumulative_balances as (
    select 
        *,
        case
            when account_type = 'balancesheet' then sum(period_amount) over (partition by account_no, account_title, book_id, entry_state 
                

                order by date_month, account_no rows unbounded preceding)
            else 0 
        end as cumulative_amount   
    from gl_period_balances

), 

gl_beginning_balance as (
    select 
        *,
        case
            when account_type = 'balancesheet' then (cumulative_amount - period_amount) 
            else 0 
        end as period_beg_amount,
        period_amount as period_net_amount, 
        cumulative_amount as period_ending_amount
    from gl_cumulative_balances

), 

gl_patch as (
    select 
        coalesce(gl_beginning_balance.account_no, gl_accounting_periods.account_no) as account_no,
        coalesce(gl_beginning_balance.account_title, gl_accounting_periods.account_title) as account_title,
        coalesce(gl_beginning_balance.book_id, gl_accounting_periods.book_id) as book_id,
        coalesce(gl_beginning_balance.category, gl_accounting_periods.category) as category,
        coalesce(gl_beginning_balance.classification, gl_accounting_periods.classification) as classification,
        coalesce(gl_beginning_balance.currency, gl_accounting_periods.currency) as currency,
        coalesce(gl_beginning_balance.entry_state, gl_accounting_periods.entry_state) as entry_state,
        coalesce(gl_beginning_balance.account_type, gl_accounting_periods.account_type) as account_type,
        coalesce(gl_beginning_balance.date_year, gl_accounting_periods.date_year) as date_year

        
        ,
        gl_accounting_periods.period_first_day,
        gl_accounting_periods.period_last_day,
        gl_accounting_periods.period_index,
        gl_beginning_balance.period_net_amount,
        gl_beginning_balance.period_beg_amount,
        gl_beginning_balance.period_ending_amount,
        case 
            when gl_beginning_balance.period_beg_amount is null and period_index = 1 then 0
            else gl_beginning_balance.period_beg_amount
        end as period_beg_amount_starter,
        case
            when gl_beginning_balance.period_ending_amount is null and period_index = 1 then 0
            else gl_beginning_balance.period_ending_amount
        end as period_ending_amount_starter
    from gl_accounting_periods

    left join gl_beginning_balance
        on gl_beginning_balance.account_no = gl_accounting_periods.account_no
            and gl_beginning_balance.account_title = gl_accounting_periods.account_title
            and gl_beginning_balance.date_month = gl_accounting_periods.period_first_day
            and gl_beginning_balance.book_id = gl_accounting_periods.book_id
            and gl_beginning_balance.entry_state = gl_accounting_periods.entry_state
            and gl_beginning_balance.currency = gl_accounting_periods.currency

), 

gl_value_partition as (
    select
        *,
        sum(case when period_ending_amount_starter is null then 0 else 1 end) over (order by account_no, account_title, book_id, entry_state, period_last_day rows unbounded preceding) as gl_partition
    from gl_patch

), 

final as (
    select
        account_no,
        account_title,
        book_id,
        category,
        classification,
        currency,
        account_type,
        date_year, 
        entry_state,
        period_first_day,
        period_last_day,
        coalesce(period_net_amount,0) as period_net_amount,
        coalesce(period_beg_amount_starter,
            first_value(period_ending_amount_starter) over (partition by gl_partition order by period_last_day rows unbounded preceding)) as period_beg_amount,
        coalesce(period_ending_amount_starter,
            first_value(period_ending_amount_starter) over (partition by gl_partition order by period_last_day rows unbounded preceding)) as period_ending_amount
        
        
    from gl_value_partition
)

select *
from final
  )
/* {"app": "dbt", "dbt_version": "1.10.11", "profile_name": "fivetran", "target_name": "prod", "node_id": "model.sage_intacct.int_sage_intacct__general_ledger_balances"} */;


-- View: INT_SAGE_INTACCT__GENERAL_LEDGER_DATE_SPINE
CREATE OR REPLACE VIEW SAGE_INTACCT_SAGE_INTACCT.INT_SAGE_INTACCT__GENERAL_LEDGER_DATE_SPINE AS
create or replace   view KUSTOM_RAW.sage_intacct_sage_intacct.int_sage_intacct__general_ledger_date_spine
  
  
  
  
  as (
    with spine as (

    

    

    
    
    

    





with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
    
    
    + 1
    as generated_number

    from

    
    p as p0
    
    

    )

    select *
    from unioned
    where generated_number <= 2
    order by generated_number



),

all_periods as (

    select (
        

    dateadd(
        month,
        row_number() over (order by generated_number) - 1,
        cast('2025-08-29' as date)
        )


    ) as date_month
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_month <= 

    dateadd(
        month,
        1,
        cast('2025-09-29' as date)
        )



)

select * from filtered


),

general_ledger as (
    select *
    from KUSTOM_RAW.sage_intacct_sage_intacct.sage_intacct__general_ledger
),

date_spine as (
    select
        cast(date_trunc('year', date_month) as date) as date_year,
        cast(date_trunc('month', date_month) as date) as period_first_day,
        cast(
        

    dateadd(
        day,
        -1,
        

    dateadd(
        month,
        1,
        date_trunc('month', date_month)
        )


        )


        as date) as period_last_day,
        row_number() over (order by cast(date_trunc('month', date_month) as date)) as period_index
    from spine
),

final as (
    select distinct
        general_ledger.account_no,
        general_ledger.account_title,
        general_ledger.account_type,
        general_ledger.book_id,
        general_ledger.category,
        general_ledger.classification,
        general_ledger.currency,
        general_ledger.entry_state,
        date_spine.date_year,
        date_spine.period_first_day,
        date_spine.period_last_day,
        date_spine.period_index
    from general_ledger

    cross join date_spine
)

select *
from final
  )
/* {"app": "dbt", "dbt_version": "1.10.11", "profile_name": "fivetran", "target_name": "prod", "node_id": "model.sage_intacct.int_sage_intacct__general_ledger_date_spine"} */;


-- View: INT_SAGE_INTACCT__RETAINED_EARNINGS
CREATE OR REPLACE VIEW SAGE_INTACCT_SAGE_INTACCT.INT_SAGE_INTACCT__RETAINED_EARNINGS AS
create or replace   view KUSTOM_RAW.sage_intacct_sage_intacct.int_sage_intacct__retained_earnings
  
  
  
  
  as (
    with general_ledger_by_period as (
    select *
    from KUSTOM_RAW.sage_intacct_sage_intacct.sage_intacct__general_ledger_by_period
),

retained_earnings_prep as (
    select
        period_first_day,
        'dbt Package Generated' as account_no,
        'Adj. Net Income' as account_title,
        'balancesheet' as account_type,
        book_id,
        'Retained Earnings' as category,
        'Equity' as classification,
        currency,
        entry_state,
        sum(period_net_amount) as period_net_amount
    from general_ledger_by_period
    where account_type = 'incomestatement'
    group by period_first_day, book_id, entry_state, currency
),

final as (
    select
        period_first_day,
        account_no,
        account_title,
        account_type,
        book_id,
        category,
        classification,
        currency,
        entry_state,
        sum(period_net_amount) over (partition by book_id, entry_state, currency
            order by period_first_day rows between unbounded preceding and current row
        ) as amount
    from retained_earnings_prep
)

select *
from final
  )
/* {"app": "dbt", "dbt_version": "1.10.11", "profile_name": "fivetran", "target_name": "prod", "node_id": "model.sage_intacct.int_sage_intacct__retained_earnings"} */;


-- View: STG_SAGE_INTACCT__GL_ACCOUNT_TMP
CREATE OR REPLACE VIEW SAGE_INTACCT_SAGE_INTACCT_STAGING.STG_SAGE_INTACCT__GL_ACCOUNT_TMP AS
create or replace   view KUSTOM_RAW.sage_intacct_sage_intacct_staging.stg_sage_intacct__gl_account_tmp
  
  
  
  
  as (
    select * from KUSTOM_RAW.sage_intacct.gl_account
  )
/* {"app": "dbt", "dbt_version": "1.10.11", "profile_name": "fivetran", "target_name": "prod", "node_id": "model.sage_intacct.stg_sage_intacct__gl_account_tmp"} */;


-- View: STG_SAGE_INTACCT__GL_BATCH_TMP
CREATE OR REPLACE VIEW SAGE_INTACCT_SAGE_INTACCT_STAGING.STG_SAGE_INTACCT__GL_BATCH_TMP AS
create or replace   view KUSTOM_RAW.sage_intacct_sage_intacct_staging.stg_sage_intacct__gl_batch_tmp
  
  
  
  
  as (
    select * 
from KUSTOM_RAW.sage_intacct.gl_batch
  )
/* {"app": "dbt", "dbt_version": "1.10.11", "profile_name": "fivetran", "target_name": "prod", "node_id": "model.sage_intacct.stg_sage_intacct__gl_batch_tmp"} */;


-- View: STG_SAGE_INTACCT__GL_DETAIL_TMP
CREATE OR REPLACE VIEW SAGE_INTACCT_SAGE_INTACCT_STAGING.STG_SAGE_INTACCT__GL_DETAIL_TMP AS
create or replace   view KUSTOM_RAW.sage_intacct_sage_intacct_staging.stg_sage_intacct__gl_detail_tmp
  
  
  
  
  as (
    select * from KUSTOM_RAW.sage_intacct.gl_detail
  )
/* {"app": "dbt", "dbt_version": "1.10.11", "profile_name": "fivetran", "target_name": "prod", "node_id": "model.sage_intacct.stg_sage_intacct__gl_detail_tmp"} */;


-- View: VW_ACCOUNTING_SUMMARY
CREATE OR REPLACE VIEW DASH.VW_ACCOUNTING_SUMMARY AS
create or replace view vw_accounting_summary as (
    with t1 as (
    select 
        job_id, 
        actual_gross_profit as actual_GP, 
        actual_gross_profit_percentage/100 as actual_GP_Percentage, 
        adjusted_invoice_subtotal, 
        Balance_Owing,
        Change_Order_AMount,
        Collected_Subtotal,
        Consumables_Cost,
        Equipment_cost, 
        estimate_gross_profit_amount_after_woadjustment, 
        estimate_gross_profit_percentage_after_woadjustment/100 as estimated_gp_percentage_after_woadadjusment,
        estimated_gross_profit_amount_from_estimate_import,
        estimated_gross_profit_percentage_from_estimate_import/100 as estimated_gp_percentage_from_estimate_import,
        estimate_gross_profit,estimate_uninvoiced_amount,
        estimate_unpaid, 
        gross_profit_percentage/100 as gp_percentage,
        initial_estimate,
        invoice_subtotal,
        labor_cost,
        materials_cost,
        original_estimate,
        other_cost,
        professional_fee,
        recognized_revenue,
        referral_fee_cost,
        subtrade_cost,
        supplement_estimate,
        total_collected,
        total_estimates,
        total_job_cost,
        total_work_order_budget,
        warranty_cost,
        estimate_depreciation,
    case 
        when _modified = (max(_modified) over (partition by job_id)) 
        then 1 
        else 0 
    end as latest_flag
    from accountingsummary
)
    select t1.*,t2.tax,
        case
            when total_estimates <= 10000 then '0-10K'
            when total_estimates <= 25000 then '10-25K'
            when total_estimates <= 50000 then '25-50K'
            when total_estimates <= 100000 then '50-100K'
            else '100K+'
        end as size_group
    from t1 
    left join (
        select job_id, sum(tax) as tax
        from vw_dash_invoice_summary
        group by job_id
    ) t2
    on t1.job_id = t2.job_id
    where latest_flag = 1
);


-- View: VW_ADP_HEADCOUNT
CREATE OR REPLACE VIEW ADP.VW_ADP_HEADCOUNT AS
CREATE OR REPLACE VIEW KUSTOM_RAW.ADP.VW_ADP_HEADCOUNT
(
    _FILE,
    _LINE,
    _MODIFIED,
    _FIVETRAN_SYNCED,
    COST_NUMBER,
    HIRE_REASON_DESCRIPTION,
    LOCATION_DESCRIPTION,
    HIRE_DATE,
    REPORTS_TO_ASSOCIATE_ID,
    THIS_IS_A_MANAGEMENT_POSITION,
    NAICS_WORKERS_COMP_DESCRIPTION,
    JOB_TITLE_CODE,
    WORKER_CATEGORY_DESCRIPTION,
    NAICS_WORKERS_COMP_CODE,
    REPORTS_TO_JOB_TITLE_DESCRIPTION,
    HOME_DEPARTMENT_DESCRIPTION,
    MIDDLE_INITIAL,
    POSITION_STATUS,
    LEGAL_FIRST_NAME,
    VOLUNTARY_INVOLUNTARY_TERMINATION_FLAG,
    BIRTH_DATE,
    PAYROLL_COMPANY_CODE,
    JOB_TITLE_DESCRIPTION,
    REGULAR_PAY_RATE_AMOUNT,
    REHIRE_DATE,
    ANNUAL_SALARY,
    ASSOCIATE_ID,
    REPORTS_TO_LEGAL_NAME,
    LEGAL_LAST_NAME,
    TERMINATION_DATE,
    REGULAR_PAY_RATE_DESCRIPTION,
    BUSINESS_UNIT_CODE,
    POSITION_ID,
    WORK_CONTACT_WORK_EMAIL,
    LATEST_FLAG
) AS
(
    -- Pick the single most-recently modified file
    WITH latest_file AS (
        SELECT _FILE
        FROM headcount
        GROUP BY _FILE
        ORDER BY MAX(_MODIFIED) DESC
        LIMIT 1
    ),
    flag_version AS (
        SELECT
            h._FILE,
            h._LINE,
            h._MODIFIED,
            h._FIVETRAN_SYNCED,
            h.COST_NUMBER,
            h.HIRE_REASON_DESCRIPTION,
            h.LOCATION_DESCRIPTION,
            h.HIRE_DATE,
            h.REPORTS_TO_ASSOCIATE_ID,
            h.THIS_IS_A_MANAGEMENT_POSITION,
            h.NAICS_WORKERS_COMP_DESCRIPTION,
            h.JOB_TITLE_CODE,
            h.WORKER_CATEGORY_DESCRIPTION,
            h.NAICS_WORKERS_COMP_CODE,
            h.REPORTS_TO_JOB_TITLE_DESCRIPTION,
            h.HOME_DEPARTMENT_DESCRIPTION,
            h.MIDDLE_INITIAL,
            h.POSITION_STATUS,
            h.LEGAL_FIRST_NAME,
            h.VOLUNTARY_INVOLUNTARY_TERMINATION_FLAG,
            h.BIRTH_DATE,
            h.PAYROLL_COMPANY_CODE,
            h.JOB_TITLE_DESCRIPTION,
            h.REGULAR_PAY_RATE_AMOUNT,
            h.REHIRE_DATE,
            h.ANNUAL_SALARY,
            h.ASSOCIATE_ID,
            h.REPORTS_TO_LEGAL_NAME,
            h.LEGAL_LAST_NAME,
            h.TERMINATION_DATE,
            h.REGULAR_PAY_RATE_DESCRIPTION,
            h.BUSINESS_UNIT_CODE,
            h.POSITION_ID,
            h.WORK_CONTACT_WORK_EMAIL,
            CASE
                WHEN h._MODIFIED = MAX(h._MODIFIED) OVER (PARTITION BY h.ASSOCIATE_ID) THEN 1
                ELSE 0
            END AS LATEST_FLAG
        FROM headcount h
        JOIN latest_file lf
          ON h._FILE = lf._FILE
    )
    SELECT *
    FROM flag_version
    WHERE LATEST_FLAG = 1
);


-- View: VW_ADP_JOB_CHANGES
CREATE OR REPLACE VIEW ADP.VW_ADP_JOB_CHANGES AS
create or replace view KUSTOM_RAW.ADP.VW_ADP_JOB_CHANGES(
	PROJECTID,
	NAME,
	STATUS
) as (
SELECT
    concat(projectid, '.') as projectid,
    name,
    status
FROM KUSTOM_RAW.SAGE_INTACCT.PROJECT
WHERE _FIVETRAN_DELETED = FALSE
  AND (
        status ILIKE 'active'
        OR (status ILIKE 'inactive'
            AND whenmodified >= DATEADD(day, -60, CURRENT_DATE))));


-- View: VW_ADP_PAYROLL
CREATE OR REPLACE VIEW ADP.VW_ADP_PAYROLL AS
create or replace view vw_adp_payroll as (
    with account_table as(
        select 
            distinct(account) as account_code
        from payroll 
    ),
    
    array_table as (
        select 
            account_code
            , split(replace(account_code, '.', ''), '/') as account_code_array
        from account_table
    ),
    
    array_breakdown as (
        select
            account_code
            , substr(get(account_code_array, 0), 1, 5) as prefix
            , substr(get(account_code_array, 0), 6, 1) as ee_type
            , substr(get(account_code_array, 1), 0, 3) as location
            , substr(get(account_code_array, 2), 0, 11) as job_number
            , array_size(account_code_array) as array_size
        from array_table
    )
    
    select
        _FILE as file_name
        , _LINE as line_number
        , _MODIFIED as date_uploaded
        , _FIVETRAN_SYNCED as date_synced
        , DATE as date_worked
        , hours as hours
        , comments
        , notes
        , pay_code
        , employee_name
        , wages
        , hourly_rate
        , pay_rule
        , concat(substr(id, 0, 3), substr(id, 5)) as id
        , account
        , case
            when prefix in ('0','-') then ''
            else prefix
          end as prefix
        , ee_type as ee_type_prefix
        , case
            when ee_type = 'A' then 'Admin'
            when ee_type = 'F' then 'Field'
            when ee_type = 'P' then 'Production'
            else 'N/A'
        end as ee_type
        , b.location as location
        , case
            when job_number like '%/%' then '' 
            else job_number
          end as job_number
    from payroll a
    left join array_breakdown b
    on a.account = b.account_code
)


-- View: VW_APEX_INVOICE_AMTS
CREATE OR REPLACE VIEW SAGE_DB_DBO.VW_APEX_INVOICE_AMTS AS
create or replace view KUSTOM_RAW.SAGE_DB_DBO.VW_APEX_INVOICE_AMTS(
	DBID,
	UNIQUEID,
	CUSTOMER,
	INVOICE,
	JOB,
	TRANSACTION_TYPE,
	AMOUNT,
	ACCOUNTING_DATE,
	DUE_DATE,
	STATUS_DATE
) as (
SELECT 
    'APEX' AS DBID,
    ifnull(UPPER(I."CustomerName"),'') 
        || ifnull(
            UPPER(I."Id"),''
            ) 
        || ifnull(
            UPPER(C."Name"),''
            ) 
        || ifnull(
            'APEX',''
            )
    AS UNIQUEID,
    I."CustomerName" AS CUSTOMER,
    I."Id" AS invoice,
    C."Name" AS JOB,
    I."TemplateName" AS Transaction_type, //find correct fields
    I."Subtotal" as amount,
    //I."AppliedAmount", amount paid to date
    //I."BalanceRemaining", //remaining amount
    I."TxnDate" AS accounting_date,
    I."DueDate" as due_date,
    I."ShipDate" as status_date
from kustom_raw.skyvia."Invoice" I
left JOIN kustom_raw.skyvia."Customer" C
    ON c."FullName" = I."CustomerName");


-- View: VW_APEX_INVOICE_META
CREATE OR REPLACE VIEW SAGE_DB_DBO.VW_APEX_INVOICE_META AS
create or replace view KUSTOM_RAW.SAGE_DB_DBO.VW_APEX_INVOICE_META(
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
SELECT distinct
    'APEX' AS DBID,
    ifnull(UPPER(I."CustomerName"),'') 
        || ifnull(
            UPPER(I."Id"),''
            ) 
        || ifnull(
            UPPER(C."Name"),''
            ) 
        || ifnull(
            'APEX',''
            )
    AS UNIQUEID,
    upper(I."CustomerName") as id,
    I."BillAddr1" as name,
    upper(I."Id") as invoice,
    C."Name" AS JOB,
    I."BillAddr2" as description,
    Null as Phone,
    '99-83' as cost_account_prefix,
    '99-83' as GL_PREFIX,
    t3.supervisor_dash AS SUPERVISOR,
    t3.estimator_dash AS ESTIMATOR,
    t3.coordinator_dash AS COORDINATOR,
    t3.marketing_dash AS MARKETING,
    NULL as Admin
FROM kustom_raw.skyvia."Invoice" I
left JOIN kustom_raw.skyvia."Customer" C
    ON c."FullName" = I."CustomerName"
LEFT JOIN KUSTOM_PREPARED.SAGE_DASH_COMPARISON.VW_INTERNAL_PARTICIPANTS_PIVOT t3
    on c."Name" = t3.job_number);


-- View: VW_APEX_INVOICE_PMTS
CREATE OR REPLACE VIEW SAGE_DB_DBO.VW_APEX_INVOICE_PMTS AS
create or replace view KUSTOM_RAW.SAGE_DB_DBO.VW_APEX_INVOICE_PMTS(
	DBID,
	UNIQUEID,
	CUSTOMER,
	INVOICE,
	JOB,
	TRANSACTION_TYPE,
	AMOUNT,
	ACCOUNTING_DATE,
	DUE_DATE,
	STATUS_DATE,
	ISPAID
) as (
--WITH t1 AS (
 --SELECT
      --t."Id" AS source_row_id,
      --f.index AS array_index,
      --f.value:TxnID::STRING            AS TxnID,
      --f.value:TxnType::STRING          AS TxnType,
      --f.value:TxnDate::TIMESTAMP_NTZ   AS TxnDate,
      --f.value:RefNumber::STRING        AS RefNumber,
      --f.value:LinkType::STRING         AS LinkType,
      --f.value:Amount::FLOAT            AS Amount
  --FROM kustom_raw.skyvia."Invoice" t,
       --LATERAL FLATTEN(
         --input => CASE
                    --WHEN TYPEOF(TRY_PARSE_JSON(t."LinkedTxn")) = 'ARRAY'
                      --THEN TRY_PARSE_JSON(t."LinkedTxn")
                    --WHEN TYPEOF(TRY_PARSE_JSON(t."LinkedTxn")) = 'OBJECT'
                      --THEN ARRAY_CONSTRUCT(TRY_PARSE_JSON(t."LinkedTxn"))
                    --ELSE NULL
                  --END
       --) f
  --WHERE t."LinkedTxn" IS NOT NULL 
    --AND TRY_PARSE_JSON(t."LinkedTxn") IS NOT NULL
    --AND f.value:TxnType::STRING = 'ReceivePayment'  
--)
SELECT
    'APEX' AS DBID,
    IFNULL(UPPER(I."CustomerName"),'')
      || IFNULL(UPPER(I."Id"),'')
      || IFNULL(UPPER(C."Name"),'')
      || 'APEX'                      AS UNIQUEID,
    I."CustomerName"                 AS CUSTOMER,
    I."Id"                           AS INVOICE,
    C."Name"                         AS JOB,
    I."TemplateName"                 AS TRANSACTION_TYPE,
    --t1.Amount                        AS AMOUNT,
    I."AppliedAmount"                AS AMOUNT,
    I."TxnDate"::TIMESTAMP_NTZ       AS ACCOUNTING_DATE, 
    I."DueDate"                      AS DUE_DATE,
    I."ShipDate"                     AS STATUS_DATE,
    I."IsPaid"                       AS IS_PAID
FROM kustom_raw.skyvia."Invoice" I
LEFT JOIN kustom_raw.skyvia."Customer" C
  ON C."FullName" = I."CustomerName");


-- View: VW_COLLECTIONS
CREATE OR REPLACE VIEW SAGE_DB_DBO.VW_COLLECTIONS AS
create or replace view KUSTOM_RAW.SAGE_DB_DBO.VW_COLLECTIONS(
	ACTVTY_SEQ,
	CUSTOMER,
	DBID,
	STATUS_DATE,
	STATUS_SEQ,
	STATUS_TYPE,
	TYPE_ADJUSTED,
	ACTIVITY_TYPE,
	DRAW,
	ADJUSTMENT,
	DESCRIPTION,
	DUE_DATE,
	AMOUNT,
	CASH_RECEIPT_TYPE,
	RETAINAGE_BILLED,
	CONTRACT_ITEM,
	ADJUSTMENT_TYPE,
	CONTRACT,
	DATE_STAMP,
	EXTRA,
	ACTIVITY_NOTES,
	ACTIVITY_STATUS,
	DEPOSIT_DATE,
	ADJUSTMENT_ACTIVITY,
	DEPOSIT_ID,
	DEPOSIT_ITEM,
	ADJUSTMENT_TYPENUMBER,
	INVOICE,
	REFERENCE,
	RETAINAGE_HELD,
	TIME_STAMP,
	ACTIVITY_FILE_LINKS,
	COST_CODE,
	ACTIVITY_DATE,
	ROW_ID,
	FINANCE_CHARGE_AMOUNT,
	CASH_RECEIPT,
	ROW_VERSION,
	BANK_ACCOUNT,
	ADJUSTMENT_DATE,
	JOB,
	RESERVED,
	OPERATOR_STAMP,
	INVOICE_DATE,
	_FIVETRAN_DELETED,
	_FIVETRAN_SYNCED
) as (
    SELECT
        *
    FROM 
        KUSTOM_RAW.SAGE_DB_DBO.ARA_ACTIVITY__ACTIVITY
    WHERE 
        CASH_RECEIPT_TYPE <> 'Not Used' and _fivetran_deleted = FALSE
);


-- View: VW_COMPANY_CONTACTS
CREATE OR REPLACE VIEW SALESFORCE_INPUT.VW_COMPANY_CONTACTS AS
CREATE OR REPLACE VIEW KUSTOM_RAW.SALESFORCE_INPUT.VW_COMPANY_CONTACTS AS (
    WITH t1 AS (
        SELECT 
            company_id, 
            index,
            value,
            CASE 
                WHEN _modified = (MAX(_modified) OVER (PARTITION BY company_id)) 
                THEN 1 
                ELSE 0 
            END AS latest_flag
        FROM KUSTOM_RAW.DASH.COMPANY_CONTACTS,
        TABLE(FLATTEN(contacts))        
    )

    SELECT 
        Company_ID,
        Value:Address:AddressLine1::String AS AddressLine1,
        Value:Address:AddressLine2::String AS AddressLine2,
        Value:Address:City::String AS City,
        Value:Address:Country::String AS Country,
        Value:Address:County::String AS County,
        Value:Address:PostalCode::String AS PostalCode,
        Value:Address:StateProvince::String AS State,
        Value:BillingAddress:AddressLine1::String AS Billing_AddressLine1,
        Value:BillingAddress:AddressLine2::String AS Billing_AddressLine2,
        Value:BillingAddress:City::String AS Billing_City,
        Value:BillingAddress:Country::String AS Billing_Country,
        Value:BillingAddress:County::String AS Billing_County,
        Value:BillingAddress:PostalCode::String AS Billing_PostalCode,
        Value:BillingAddress:StateProvince::String AS Billing_State,
        Value:ContactID AS ContactID,
        Value:CorrespondenceEmail::String AS CorrespondenceEmail,
        Value:InquiryEmail::String AS InquiryEmail,
        Value:MailingAddress:AddressLine1::String AS Mailing_AddressLine1,
        Value:MailingAddress:AddressLine2::String AS Mailing_AddressLine2,
        Value:MailingAddress:City::String AS Mailing_City,
        Value:MailingAddress:Country::String AS Mailing_Country,
        Value:MailingAddress:County::String AS Mailing_County,
        Value:MailingAddress:PostalCode::String AS Mailing_PostalCode,
        Value:MailingAddress:StateProvince::String AS Mailing_State,
        Value:MainPhone.Extension::String AS PhoneExtension,
        Value:MainPhone.Number::String AS Phone,
        Value:Website::String AS Website
    FROM t1
    WHERE latest_flag = 1
)
;


-- View: VW_COMPANY_DETAIL
CREATE OR REPLACE VIEW DASH.VW_COMPANY_DETAIL AS
create or replace view vw_company_detail as(
with company_info as (
select company_id, company_info:FranchiseeID as FranchiseeID,company_info:Name::String as Company_Name,company_info:ResponsibleRep:FirstName::String || ' ' || company_info:ResponsibleRep:LastName::String as Rep_Name, company_info:Type::String as Company_Type,
case 
    when _modified = (max(_modified) over (partition by company_id)) 
    then 1 
    else 0 
end as latest_flag
from company_detail
)
select * from company_info
where latest_flag = 1
);


-- View: VW_COMPANY_DETAIL
CREATE OR REPLACE VIEW SALESFORCE_INPUT.VW_COMPANY_DETAIL AS
CREATE OR REPLACE VIEW KUSTOM_RAW.SALESFORCE_INPUT.VW_COMPANY_DETAIL AS (
    WITH t1 AS (
        SELECT 
            Company_ID,
            Is_Active,
            Company_Info:Categories[0]::String AS Category,
            Company_Info:FranchiseeID AS FranchiseeID,
            Company_Info:GroupAndRoutes AS GroupAndRoutes,
            Company_Info:MarketingCampaigns[0]::String AS MarketingCampaign,
            Company_Info:Name::String AS Name,
            Company_Info:ParentCompanyID AS ParentCompanyID,
            Company_Info:Rank AS Rank,
            Company_Info:ReferralType::String AS ReferralType,
            Company_Info:ReferredBy AS ReferredBy,
            Company_Info:ResponsibleRep.FirstName::String AS RepFirstName,
            Company_Info:ResponsibleRep.LastName::String AS RepLastName,
            Company_Info:SageAccountNumber::String AS SageAccountNumber,
            Company_Info:SalesStage::String AS SalesStage,
            Company_Info:SalesStatus::String AS SalesStatus,
            Company_Info:Type::String AS Type,
            CASE 
                WHEN _modified = (MAX(_modified) OVER (PARTITION BY Company_ID)) 
                    then 1 
                    else 0 
                end as latest_flag
        FROM KUSTOM_RAW.DASH.COMPANY_DETAIL
    )
    SELECT *
    FROM t1
    WHERE latest_flag = 1
)
;


-- View: VW_COMPLIANCE_TASK
CREATE OR REPLACE VIEW DASH.VW_COMPLIANCE_TASK AS
create view vw_compliance_task as (
with compliance_flat as(
select job_id, compliance_task_id, action_title, assignee:FirstName::String || ' ' || assignee:LastName::String as Assignee, completed_date, Due_Date, Exception_reason, required_action, note, 
case 
    when _modified = (max(_modified) over (partition by compliance_task_id)) 
    then 1 
    else 0 
end as latest_flag
from compliance_task
)
select * 
from compliance_flat
where latest_flag = 1
);


-- View: VW_CONCUR_EXPENSE
CREATE OR REPLACE VIEW CONCUR.VW_CONCUR_EXPENSE AS
create or replace view vw_concur_expense as (
    select 
        t1.transaction_date, 
        t1.approved_amount,
        t1.posted_amount,
        t1.Location_subdivision, 
        t1.spend_category, 
        t1.vendor_description, 
        t1.expense_Type_name, 
        t1.user_id, 
        t1.org_unit_2, 
        t1.org_unit_1,
        t2.approval_status_code,
        t2.approval_status_name,
        t1.payment_type_name,
        t2.payment_status_name,
        t2.payment_status_code,
        custom_2_code as allocation_job_number,
        custom_1_code as division,
        t4.account_code_1 as account_code,
        right(
            left(
                t1.org_unit_2,12
            ),11
        ) as job_number
    from expense_entry t1
    left join report t2
    on t1.report_id = t2.id
    left join itemization t3
    on t1.ID = t3.expense_entry_id
    LEFT JOIN allocation t4
    on t3.id = t4.itemization_id
);


-- View: VW_DASH_INVOICE_DETAIL
CREATE OR REPLACE VIEW DASH.VW_DASH_INVOICE_DETAIL AS
create or replace view vw_dash_invoice_detail as
(
with invoice_flat as 
(
select t1.job_id,invoices1.index,invoices1.value, case when _modified = (max(_modified) over (partition by t1.job_id)) then 1 else 0 end as latest_flag
from accountingdetail t1,
table(flatten(t1.invoices)) as invoices1
)
select i1.job_id,i1.index as Invoice_Index, date(i1.value:DateAdded) as Invoice_Date_Added,date(i1.value:DateInvoiced) as Date_Invoiced,i1.Value:InvoiceID as InvoiceID, i2.index as line_index, i2.value:AddedDate::DAte as Invoice_Line_Date_Added, i2.Value:Adjustment as Line_Adjustment, i2.Value:Details::String as Line_Details, i2.Value:EquipmentRate as Equipment_Rate, i2.Value:ExtendedAmount as Line_Extended_Amount,i2.Value:InvoiceLineItemID as Line_Item_ID, i2.Value:ItemDescription::String as Item_Description, i2.value:LaborRate as Labor_Rate,i2.value:LineDate::Date as Line_Date,i2.Value:MaterialRate as Material_Rate, i2.value:Overhead as Overhead, i2.value:Profit as Profit, i2.value:Rate as Rate,i2.value:Status::String as Line_Status
from invoice_flat i1,
table(flatten(i1.value:InvoiceLineItems)) as i2
where latest_flag = 1
);


-- View: VW_DASH_INVOICE_SUMMARY
CREATE OR REPLACE VIEW DASH.VW_DASH_INVOICE_SUMMARY AS
create or replace view vw_dash_invoice_summary as
(
    with invoice_flat as 
    (
        select t1.job_id,invoices1.index,invoices1.value, case when _modified = (max(_modified) over (partition by t1.job_id)) then 1 else 0 end as latest_flag
        from accountingdetail t1,
        table(flatten(t1.invoices)) as invoices1
    )
    select 
        i1.job_id,i1.index as Invoice_Index, 
        i1.value:Amount as invoiced_amount, 
        i1.value:DateAdded::Date as Invoice_Date_Added, 
        i1.value:DateInvoiced::Date as Date_Invoiced, 
        i1.value:DateLastUpdate::Date as Invoice_Last_Updated, 
        i1.value:InvoiceBalance as Invoice_Balance,
        i1.Value:InvoiceID as InvoiceID,
        i1.Value:Tax as Tax
    from invoice_flat i1
    where latest_flag = 1
);


-- View: VW_DASH_PAYMENT_DETAIL
CREATE OR REPLACE VIEW DASH.VW_DASH_PAYMENT_DETAIL AS
create or replace view vw_dash_payment_detail as 
(
with payments_flat as
(
select t1.job_id,payment1.index,payment1.value, case when _modified = (max(_modified) over (partition by t1.job_id)) then 1 else 0 end as latest_flag
from accountingdetail t1,
table(flatten(t1.payments)) as payment1
)
select p1.job_id,index,value:Amount as paid_amount,date(value:DateAdded) as Payment_Date_Added, date(value:DateLastUpdate) as Payment_Last_Updated, date(value:DatePaid) as Date_Paid,value:DiscountAmount as Discount_Amount, value:InvoiceID as InvoiceID, value:Memo::String as Memo, value:Mode::string as Payment_Mode, value:PaymentID as PaymentID,value:Status::string as status
from payments_flat p1
where latest_flag =1
);


-- View: VW_GLT_CURRENT__TRANSACTION
CREATE OR REPLACE VIEW SALESFORCE_INPUT.VW_GLT_CURRENT__TRANSACTION AS
CREATE OR REPLACE VIEW KUSTOM_RAW.SALESFORCE_INPUT.VW_GLT_CURRENT__TRANSACTION AS (
    SELECT
        *
    FROM 
        KUSTOM_RAW.SAGE_DB_DBO.GLT_CURRENT__TRANSACTION
)
;


-- View: VW_INDIVIDUAL_DETAIL
CREATE OR REPLACE VIEW SALESFORCE_INPUT.VW_INDIVIDUAL_DETAIL AS
CREATE OR REPLACE VIEW KUSTOM_RAW.SALESFORCE_INPUT.VW_INDIVIDUAL_DETAIL AS (
    WITH t1 AS (
        SELECT
            Is_Active,
            Individual_ID,
            Individual_Detail_Info:Anniversary AS Anniversary,
            Individual_Detail_Info:Assistant::STRING AS Assistant,
            Individual_Detail_Info:Categories::STRING AS Categories,
            Individual_Detail_Info:CompanyID AS CompanyID,
            Individual_Detail_Info:ContactType::String AS ContactType,
            Individual_Detail_Info:DateOfBirth AS DateOfBirth,
            Individual_Detail_Info:FranchiseeId AS FranchiseeId,
            Individual_Detail_Info:GroupAndRoutes::STRING AS GroupAndRoutes,
            Individual_Detail_Info:JobTitle::STRING AS JobTitle,
            Individual_Detail_Info:MarketingCampaigns::STRING AS MarketingCampaigns,
            Individual_Detail_Info:Name:FirstName::STRING AS FirstName,
            Individual_Detail_Info:Name:LastName::STRING AS LastName,
            Individual_Detail_Info:ParentCompany::STRING AS ParentCompany,
            Individual_Detail_Info:PotentialReferralRevenue AS PotentialReferralRevenue,
            Individual_Detail_Info:PotentialReferralVolume AS PotentialReferralVolume,
            Individual_Detail_Info:Rank AS Rank,
            Individual_Detail_Info:ReferralType::STRING AS ReferralType,
            Individual_Detail_Info:ReferredBy::STRING AS ReferredBy,
            Individual_Detail_Info:ResponsibleRep:FirstName::STRING AS ResponsibleRepFirstName,
            Individual_Detail_Info:ResponsibleRep:LastName::STRING AS ResponsibleRepLastName,
            Individual_Detail_Info:SageAccountNumber AS SageAccountNumber,
            Individual_Detail_Info:SalesStage::STRING AS SalesStage,
            Individual_Detail_Info:SalesStatus::STRING AS SalesStatus,
            Individual_Detail_Info:Title::STRING AS Title,
            CASE 
                WHEN _modified = (MAX(_modified) OVER (PARTITION BY Individual_ID)) 
                    then 1 
                    else 0 
                end as latest_flag
        FROM KUSTOM_RAW.DASH.INDIVIDUAL_DETAIL
    )

    SELECT *
    FROM t1
    WHERE latest_flag = 1
)
    



-- View: VW_INTERNAL_PARTICIPANTS
CREATE OR REPLACE VIEW DASH.VW_INTERNAL_PARTICIPANTS AS
create or replace view vw_internal_participants as (
with flattened_internal_participants as (
select job_id, p1.index, p1.value, 
case 
    when _modified = (max(_modified) over (partition by job_id)) 
    then 1 
    else 0 
end as latest_flag
from internalparticipants,
table(flatten(participants)) as p1
)
select job_id, value:ID as Internal_Participant_ID, Value:PersonName:FirstName::String || ' ' || value:PersonName:LastName::String as ParticipantName, value:Type::String as Participant_Role
from flattened_internal_participants
where latest_Flag = 1
);


-- View: VW_INVOICE_AMTS
CREATE OR REPLACE VIEW SAGE_DB_DBO.VW_INVOICE_AMTS AS
create or replace view KUSTOM_RAW.SAGE_DB_DBO.VW_INVOICE_AMTS(
	DBID,
	UNIQUEID,
	CUSTOMER,
	INVOICE,
	JOB,
	TRANSACTION_TYPE,
	AMOUNT,
	ACCOUNTING_DATE,
	DUE_DATE,
	STATUS_DATE
) as (
    select 
        dbid,
        ifnull(UPPER(t1.customer),'') 
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
        t1.customer,
        t1.invoice, 
        t1.job, 
        t1.transaction_type,
        t1.amount,
        t1.accounting_date,
        t1.due_date, 
        t1.status_date
    from art_current__transaction t1
    where t1.transaction_type in (
        'Issued invoice',
        'Invoice adjustment',
        'Invoice'
        ) 
        and uniqueid <> dbid
        and _fivetran_deleted = FALSE
);


-- View: VW_INVOICE_DETAILS
CREATE OR REPLACE VIEW SALESFORCE_INPUT.VW_INVOICE_DETAILS AS
CREATE OR REPLACE VIEW KUSTOM_RAW.SALESFORCE_INPUT.VW_INVOICE_DETAILS AS (
    WITH invoice_flat AS (
        SELECT 
            t1.job_id,invoices1.index,
            invoices1.value, 
            CASE 
                WHEN _modified = (MAX(_modified) OVER (PARTITION BY t1.job_id)) 
                THEN 1 
                ELSE 0 
            END AS latest_flag
    FROM KUSTOM_RAW.DASH.ACCOUNTINGDETAIL t1,
    TABLE(FLATTEN(t1.invoices)) AS invoices1
    )
    
    SELECT 
        i1.job_id,i1.index AS Invoice_Index, 
        i1.value:DateAdded::Date AS Invoice_Date_Added,
        i1.value:DateInvoiced::Date AS Date_Invoiced,
        i1.Value:InvoiceID AS InvoiceID, 
        i2.index AS line_index, 
        i2.value:AddedDate::Date AS Invoice_Line_Date_Added, 
        i2.Value:Adjustment AS Line_Adjustment, 
        i2.Value:Details::String AS Line_Details, 
        i2.Value:EquipmentRate AS Equipment_Rate, 
        i2.Value:ExtendedAmount AS Line_Extended_Amount,
        i2.Value:InvoiceLineItemID AS Line_Item_ID, 
        i2.Value:ItemDescription::String AS Item_Description, 
        i2.value:LaborRate AS Labor_Rate,
        i2.value:LineDate::Date AS Line_Date,
        i2.Value:MaterialRate AS Material_Rate, 
        i2.value:Overhead AS Overhead, 
        i2.value:Profit AS Profit, 
        i2.value:Rate AS Rate,
        i2.value:Status::String AS Line_Status
    FROM invoice_flat i1,
    TABLE(FLATTEN(i1.value:InvoiceLineItems)) AS i2
    WHERE latest_flag = 1
)
;


-- View: VW_INVOICE_META
CREATE OR REPLACE VIEW SAGE_DB_DBO.VW_INVOICE_META AS
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


-- View: VW_INVOICE_PMTS
CREATE OR REPLACE VIEW SAGE_DB_DBO.VW_INVOICE_PMTS AS
create or replace view KUSTOM_RAW.SAGE_DB_DBO.VW_INVOICE_PMTS(
	DBID,
	UNIQUEID,
	CUSTOMER,
	INVOICE,
	JOB,
	TRANSACTION_TYPE,
	AMOUNT,
	ACCOUNTING_DATE,
	DUE_DATE,
	STATUS_DATE
) as (
    select 
        dbid,
        ifnull(
            UPPER(t1.customer),''
            ) 
            || ifnull(
                UPPER(t1.invoice),''
                ) 
            || ifnull(
                UPPER(t1.job),''
                ) 
            ||ifnull(
                upper(t1.dbid),''
                ) 
        as uniqueid, 
        t1.customer,
        t1.invoice, 
        t1.job, 
        t1.transaction_type,
        t1.amount,
        t1.accounting_date,
        t1.due_date,
        t1.status_date
    from art_current__transaction t1
    where t1.transaction_type in (
        'Cash receipt',
        'Cash recpt adjustmnt'
        ) 
        and uniqueid <> dbid
        and _fivetran_deleted = FALSE
);


-- View: VW_INVOICE_SUMMARY
CREATE OR REPLACE VIEW SALESFORCE_INPUT.VW_INVOICE_SUMMARY AS
CREATE OR REPLACE VIEW KUSTOM_RAW.SALESFORCE_INPUT.VW_INVOICE_SUMMARY AS (
    WITH invoice_flat AS (
        SELECT 
            t1.job_id,invoices1.index,
            invoices1.value,
            CASE 
                WHEN _modified = (max(_modified) OVER (partition BY t1.job_id)) 
                THEN 1 
                ELSE 0 
            END AS latest_flag
        FROM KUSTOM_RAW.DASH.ACCOUNTINGDETAIL t1,
        TABLE(FLATTEN(t1.invoices)) AS invoices1
    )
    
    SELECT 
        i1.job_id,
        i1.index AS Invoice_Index, 
        i1.value:Amount AS invoiced_amount, 
        i1.value:DateAdded::Date AS Invoice_Date_Added, 
        i1.value:DateInvoiced::Date AS Date_Invoiced, 
        i1.value:DateLastUpdate::Date AS Invoice_Last_Updated, 
        i1.value:InvoiceBalance AS Invoice_Balance,
        i1.Value:InvoiceID AS InvoiceID,
        i1.Value:Tax AS Tax
    FROM invoice_flat i1
    WHERE latest_flag = 1
)
;


-- View: VW_JCM_JPB_MASTER
CREATE OR REPLACE VIEW SALESFORCE_INPUT.VW_JCM_JPB_MASTER AS
CREATE OR REPLACE VIEW KUSTOM_RAW.SALESFORCE_INPUT.VW_JCM_JPB_MASTER AS (
    SELECT
        DBID,
    	JOB,
    	CUSTOMER,
    	EXTRAS_BILLING_BALANCE,
    	OFFICE,
    	EXTRAS_ORIG_CONTRACT_AMOUNT,
    	JURISDICTION,
    	TAX_ON_EXTRAS_JTD_DOLLRS_PD,
    	EXTRAS_MTD_EQUIPMENT_COST,
    	WTD_LABOR_UNITS,
    	WTD_EQUIPMENT_COST,
    	EXTRAS_NM_PAYMENTS,
    	TAX_ON_EXTRAS_WTD_DP,
    	EXTRAS_NM_DOLLARS_PAID,
    	EXTRA_TOTAL_EQUIPMENT_UNITS_EST,
    	EXTRA_COMMITMENT_INV__LABOR,
    	ORIG_ESTIMATE_FINALIZED,
    	LAST_COST_UPDATE,
    	MSC_CAT_AMT_4,
    	LW_CONDITIONAL_THRU_DATE,
    	FAX_PHONE,
    	TOTAL_MATERIAL_COMMITMENT,
    	JTD_APRVD_CONTRACT_CHGS,
    	PERCENT_COMPLETE,
    	EXTRAS_JTD_OVRHD_DLLRS_PAID,
    	TOTAL_SUB_COMMITMENT,
    	EXTRAS_QTD_APRVD_EST_CHGS,
    	TAX_ON_EXTRAS_YTD_COST,
    	EXTRAS_NM_ADJUSTMENT,
    	EXTRAS_JTD_APRVD_CNTRC_CHGS,
    	EXTRAS_YTD_WORK_BILLED,
    	EXTRAS_CUSTOM_TOTAL_1_TOTAL,
    	LM_OVERHEAD_COST,
    	CUSTOMER_PHONE_2,
    	MIN_VENDOR_TYPE_1_LW_AMOUNT,
    	TAX_ON_EXTRAS_NM_COST,
    	EXTRAS_MSC_CAT_AMT_3,
    	TAX_ON_JTD_DOLLARS_PAID,
    	EXTRAS_JTD_PAYMENTS,
    	EXTRAS_LM_LABOR_UNITS,
    	ROW_VERSION,
    	JTD_LABOR_DOLLARS_PAID,
    	EXTRAS_QTD_COST,
    	EXTRAS_JTD_LABOR_UNITS,
    	LM_LABOR_COST,
    	EXTRAS_LM_RETAINAGE_HELD,
    	REQ_VENDOR_TYPE_2_MASTER_APPROVAL,
    	TAX_ON_YTD_DOLLARS_PAID,
    	COST_OF_SALES_ACCOUNT,
    	PENDING_ESTIMATE_CHANGES,
    	MISC_TITLE_2,
    	EXTRA_COMMITMENT_INV__MATERIAL,
    	OWNER_CONTRACT_ID,
    	MTD_PAYMENTS,
    	EXTRAS_NM_COST,
    	LW_UNCOND_THRU_DATE,
    	CUSTOMER_CITY,
    	EXTRAS_NM_LABOR_COST,
    	EXTRAS_VERBAL_OKAY_CONTRACT_CHANGES,
    	CUSTOM_TOTAL_5_TOTAL,
    	LIEN__LEGAL,
    	LAST_DRAW,
    	LM_RETAINAGE_HELD,
    	CUSTOMER_ZIP_CODE,
    	TAX_ON_EXTRAS_MTD_DOLLRS_PD,
    	AR_CUSTOMER,
    	STATE,
    	NM_COST,
    	NM_LABOR_COST,
    	TAX_ON_MTD_DOLLARS_PAID,
    	JTD_OVERHEAD_DOLLARS_PAID,
    	JTD_ADJUSTMENT,
    	EXTRAS_MTD_LABOR_UNITS,
    	JTD_LABOR_COST,
    	MSC_EXTRA_AMT_3,
    	EXTRAS_LM_DOLLARS_PAID,
    	LAB_HRS_TO_COMP,
    	EXTRAS_JTD_MATERIAL_COST,
    	EXTRAS_MTD_MATERIAL_COST,
    	EXEMPT_FROM_FINANCE_CHARGE AS FINANCE_CHARGE_EXEMPTION,
    	NM_APRVD_CONTRACT_CHGS,
    	EXTRAS_JTD_SUB_DLLRS_PAID,
    	SITE_PHONE,
    	EXTRAS_MTD_DOLLARS_PAID,
    	DATE_STAMP,
    	REFERRAL_SOURCE,
    	TAX_ON_LM_DOLLARS_PAID,
    	NM_ADJUSTMENT,
    	LM_LABOR_UNITS,
    	EXTRAS_JTD_EQUIPMENT_UNITS,
    	NM_PAYMENTS,
    	REVENUE_ACCOUNT,
    	QTD_APRVD_ESTIMATE_CHGS,
    	SECURITY_ID,
    	YTD_RETAINAGE_HELD,
    	EXTRAS_LW_COST,
    	EXTRAS_NM_RETAINAGE_HELD,
    	EXTRAS_QTD_PAYMENTS,
    	PRODUCE_VENDOR_TYPE_4_LIEN_WAIVER,
    	NM_APRVD_ESTIMATE_CHGS,
    	MSC_EXTRA_AMT_1,
    	EXTRAS_LM_MATERIAL_COST,
    	UNIT_DESCRIPTION,
    	TOTAL_SUBCONTRACT_EST,
    	MIN_VENDOR_TYPE_4_LW_AMOUNT,
    	EXTRAS_NM_LABOR_UNITS,
    	EXTRAS_ORIGINAL_ESTIMATE,
    	EXTRAS_JTD_OVERHEAD_COST,
    	JOB_TYPE,
    	EXTRA_COMMITMENT_INV__EQUIPMENT,
    	EXTRAS_QTD_DOLLARS_PAID,
    	JTD_SUBCONTRACT_COST,
    	EXTRAS_MSC_CC_AMT_5,
    	EXTRAS_WTD_LABOR_COST,
    	TAX_ON_QTD_DOLLARS_PAID,
    	JOB_NOTES,
    	ORIGINAL_CONTRACT_AMOUNT,
    	JTD_SUB_DOLLARS_PAID,
    	PRODUCE_VENDOR_TYPE_3_LIEN_WAIVER,
    	EXTRAS_YTD_PAYMENTS,
    	EXTRAS_CUSTOM_TOTAL_4_TOTAL,
    	TAX_ON_LW_COST,
    	MISC_TITLE_4,
    	CHECKLIST_3,
    	TOTAL_EQUIPMENT_ESTIMATE,
    	EXTRAS_WTD_EQUIPMENT_UNITS,
    	CHECKLIST_9,
    	BANK_ACCOUNT,
    	EXTRAS_JTD_RETAINAGE_HELD,
    	COMMITMENT_INVOICED__MATERIAL,
    	EXTRA_COMMITMENT_INV__OTHER,
    	QTD_ADJUSTMENT,
    	MSC_JOB_AMT_1,
    	EXTRA_COMMITMENT_INVOICED,
    	TAX_ON_EXTRAS_JTD_COST,
    	MISC_TITLE_1,
    	YTD_APRVD_CONTRACT_CHGS,
    	MTD_RETAINAGE_HELD,
    	TOTAL_MATERIAL_ESTIMATE,
    	EXTRAS_COST_TO_COMP,
    	CERTIFIED_PROJECT,
    	MIN_VENDOR_TYPE_2_LW_AMOUNT,
    	REQ_VENDOR_TYPE_1_MASTER_APPROVAL,
    	WTD_DOLLARS_PAID,
    	MTD_WORK_BILLED,
    	WORK_STATE,
    	EXTRAS_JTD_LABOR_DLLRS_PAID,
    	EXTRA_TOTAL_OVERHEAD_COMMITMENT,
    	BILL_TO_ADDRESS_2,
    	REQ_VENDOR_TYPE_3_MASTER_APPROVAL,
    	QTD_APRVD_CONTRACT_CHGS,
    	JTD_RETAINAGE_HELD,
    	FULL_REVENUE_ACCOUNT,
    	TAX_ON_QTD_COST,
    	EXTRAS_JTD_LABOR_COST,
    	PRODUCE_VENDOR_TYPE_1_LIEN_WAIVER,
    	ADDRESS_1,
    	USE_VENDOR_TYPE_3_COMMITMENT_LW_AMOUNT,
    	CUSTOMER_ADDRESS_1,
    	EXTRAS_REVISED_CONTRACT_AMT,
    	EXTRAS_JTD_ADJUSTMENT,
    	EXTRAS_MTD_OTHER_COST,
    	JOB_FILE_LINKS,
    	WTD_LABOR_COST,
    	QTD_WORK_BILLED,
    	TAX_ON_JTD_COST,
    	REVISED_CONTRACT_AMOUNT,
    	JTD_EQUIPMENT_COST,
    	CUSTOM_TOTAL_4_TOTAL,
    	EXTRAS_CUSTOM_TOTAL_5_TOTAL,
    	EXTRAS,
    	CUSTOM_TOTAL_2_TOTAL,
    	EXTRA_TOTAL_SUB_ESTIMATE,
    	TAX_ON_WTD_DOLLARS_PD,
    	REVENUE_RECOG_METHOD,
    	USER_DEF_KEY_1,
    	EXTRAS_WTD_COST,
    	YEAR_BUILT,
    	OTHER_TAX_GROUP,
    	ORIGINAL_ESTIMATE,
    	ZIP_CODE,
    	LM_ADJUSTMENT,
    	LABOR_TAX_GROUP,
    	USE_VENDOR_TYPE_1_COMMITMENT_LW_AMOUNT,
    	MSC_JOB_AMT_3,
    	JTD_LABOR_UNITS,
    	MTD_LABOR_COST,
    	LM_EQUIPMENT_UNITS,
    	CHECKLIST_7,
    	TAX_ON_EXTRAS_QTD_COST,
    	EXTRAS_JTD_OTHER_DLLRS_PAID,
    	EXTRAS_MTD_LABOR_COST,
    	MSC_CC_AMT_5,
    	ESTIMATED_START_DATE,
    	JTD_OVERHEAD_COST,
    	MSC_CAT_AMT_3,
    	FINANCE_CHARGE_TYPE,
    	JOB_COMPLETE,
    	LM_MATERIAL_COST,
        CHECKLIST_5,
        RECEIVABLE_BALANCE,
        TOTAL_LABOR_UNITS_EST,
        QTD_RETAINAGE_HELD,
        TAX_ON_WTD_COST,
        TAX_ON_EXTRAS_WTD_COST,
        ACTUAL_START_DATE,
        EXTRA_ORIGINAL_COMMITMENT,
        JTD_MATERIAL_COST,
        CHECKLIST_12,
        MSC_CAT_AMT_1,
        EXTRAS_YTD_ADJUSTMENT,
        TYPE,
        PRODUCE_VENDOR_TYPE_2_LIEN_WAIVER,
        REVISED_COMP_DATE,
        NM_WORK_BILLED,
        EXTRAS_QTD_RETAINAGE_HELD,
        PROJECT_MANAGER_EMAIL,
        REVISED_START_DATE,
        TOTAL_OTHER_COMMITMENT,
        EXTRAS_MTD_RETAINAGE_HELD,
        EXTRAS_CUSTOM_TOTAL_6_TOTAL,
        CUSTOM_TOTAL_3_TOTAL,
        USE_VENDOR_TYPE_2_COMMITMENT_LW_AMOUNT,
        MARKETING,
        REQ_VENDOR_TYPE_3_CERTIFIED_RPTS,
        MSC_CC_AMT_6,
        EE_COMMISION_PD,
        CHECKLIST_1,
        BD__MARKETING,
        TAX_ON_EXTRAS_MTD_CST,
        MIN_VENDOR_TYPE_1_COMMITMENT_LW_AMOUNT,
        MSC_EXTRA_AMT_2,
        COMMITMENT_INVOICED,
        REVISED_COMMITMENT,
        EXTRAS_POTENTIAL_CO_CONTRACT_CHANGES,
        EXTRAS_MTD_PAYMENTS,
        EXTRAS_YTD_APRVD_EST_CHGS,
        EXTRAS_MTD_APRVD_EST_CHGS,
        CHECKLIST_6,
        MIN_VENDOR_TYPE_3_LW_AMOUNT,
        EXTRAS_LM_APRVD_CNTRC_CHGS,
        TAX_ON_MTD_COST,
        SUPERVISOR,
        EXTRAS_NM_MATERIAL_COST,
        JTD_PAYMENTS,
        WTD_COST,
        TAX_ON_EXTRAS_YTD_DOLLRS_PD,
        SCOPE_OF_WORK,
        EXTRA_APPROVED_COMMITMENT_CHANGES,
        TAX_ON_EXTRAS_LM_COST,
        BILL_TO_CITY,
        EXTRAS_TOTAL_ESTIMATE,
        EXTRAS_WTD_DOLLARS_PAID,
        USE_VENDOR_TYPE_4_COMMITMENT_LW_AMOUNT,
        CHECKLIST_10,
        EXTRA_TOTAL_OTHER_COMMITMENT,
        NM_OVERHEAD_COST,
        EXTRAS_MTD_ADJUSTMENT,
        EXTRAS_WTD_LABOR_UNITS,
        PENDING_COMMITMENT_CHANGES,
        EXTRAS_JTD_EQUIPMENT_COST,
        BILL_TO_STATE,
        TAX_ON_EXTRAS_NM_DOLLRS_PD,
        LW_COST,
        CUSTOM_TOTAL_6_TOTAL,
        EXTRAS_STORED_MATERIAL,
        EXTRAS_LM_OTHER_COST,
        COST_ACCOUNT_PREFIX,
        ESTIMATOR,
        EXTRAS_JTD_COST,
        POTENTIAL_CO_CONTRACT_CHANGES,
        COMMITMENT_INVOICED__OTHER,
        EXTRAS_WTD_EQUIPMENT_COST,
        LAST_CUSTOM_TOTAL_UPDATE,
        BILL_TO_ADDRESS_1,
        MTD_COST,
        ESTIMATED_COMP_DATE,
        EXTRA_COMMITMENT_INV__SUB,
        EXTRAS_NM_OVERHEAD_COST,
        EXTRAS_MSC_CAT_AMT_4,
        COST_ACCOUNT,
        SIZE,
        EXTRAS_JTD_APRVD_EST_CHGS,
        EXTRA_TOTAL_MAT_COMMITMENT,
        EXTRAS_COST_AT_COMP,
        NM_EQUIPMENT_COST,
        EXTRAS_TOTAL_TAX_AMOUNT,
        EXTRAS_JTD_SUB_COST,
        VERBAL_OKAY_CONTRACT_CHANGES,
        EXTRA_COMMITMENT_INV__OVERHEAD,
        EXTRAS_QTD_APRVD_CNTRC_CHGS,
        EXTRAS_MSC_CAT_AMT_2,
        REQ_VENDOR_TYPE_4_MASTER_APPROVAL,
        MTD_OTHER_COST,
        TAX_ON_EXTRAS_LW_COST,
        CONTRACT_RETAINAGE_PERCENT,
        EXTRAS_LM_SUB_COST,
        YTD_PAYMENTS,
        EXTRAS_LM_COST,
        TAX_ON_EXTRAS_LM_DOLLRS_PD,
        CUSTOMER_CONTACT_1,
        EXTRA_TOTAL_OVERHEAD_EST,
        EXTRAS_TOTAL_LABOR_ESTIMATE,
        EXTRAS_NM_OTHER_COST,
        OPERATOR_STAMP,
        EXTRAS_LAB_HRS_TO_COMP,
        CUSTOMER_NAME,
        EXTRAS_MTD_WORK_BILLED,
        ON_HOLD,
        NM_EQUIPMENT_UNITS,
        ADDRESS_2,
        PERMIT_RCVD,
        EXTRAS_YTD_APRVD_CNTRC_CHGS,
        MTD_EQUIPMENT_UNITS,
        JTD_OTHER_DOLLARS_PAID,
        CUSTOMER_STATE,
        EXTRAS_NM_APRVD_CNTRC_CHGS,
        LM_COST,
        CO_REQUEST_CONTRACT_CHANGES,
        EXTRAS_NM_APRVD_EST_CHGS,
        EXTRAS_MTD_EQUIPMENT_UNITS,
        NM_MATERIAL_COST,
        EXTRAS_MSC_CC_AMT_6,
        USE_PJ_CHANGE_MANAGEMENT,
        ROW_ID,
        LM_EQUIPMENT_COST,
        REFERRAL_COM_PD,
        TOTAL_ESTIMATE,
        JOB_TAX_GROUP,
        TOTAL_LABOR_COMMITMENT,
        LM_OTHER_COST,
        PROJECT_MANAGER,
        DATE_OF_LAST_REPORT,
        SUPERINTENDENT_EMAIL,
        BILLING_LEVEL,
        REQ_VENDOR_TYPE_2_CERTIFIED_RPTS,
        EXTRAS_MTD_OVERHEAD_COST,
        EXTRAS_JTD_OTHER_COST,
        JTD_WORK_BILLED,
        YTD_WORK_BILLED,
        YTD_APRVD_ESTIMATE_CHGS,
        MTD_LABOR_UNITS,
        LM_WORK_BILLED,
        AUTHORIZATION,
        COMMITMENT_INVOICED__LABOR,
        OVERHEAD_TAX_GROUP,
        LM_DOLLARS_PAID,
        MTD_ADJUSTMENT,
        LM_SUBCONTRACT_COST,
        FINANCE_CHARGE_PERCENTAGE,
        MATERIAL_TAX_GROUP,
        CUSTOM_TOTAL_1_TOTAL,
        EXTRAS_LM_EQUIPMENT_COST,
        EXTRAS_JTD_EQUIPMENT_DLLRS_PAID,
        JC_ADMIN,
        LM_PAYMENTS,
        TAX_ON_EXTRAS_QTD_DOLLRS_PD,
        TAX_ON_NM_DOLLARS_PAID,
        APPROVED_COMMITMENT_CHANGES,
        MIN_VENDOR_TYPE_2_COMMITMENT_LW_AMOUNT,
        CERT_RPT_WEEK_ENDING_DAY,
        EXTRAS_NM_EQUIPMENT_UNITS,
        EXTRA_TOTAL_EQUIPMENT_ESTIMATE,
        EXTRAS_LM_OVERHEAD_COST,
        WORK_LOCAL,
        EXTRAS_LM_ADJUSTMENT,
        EXTRAS_YTD_DOLLARS_PAID,
        CONTRACT_DATE,
        LW_DOLLARS_PAID,
        LAST_DAY_OF_MONTH,
        EXTRAS_PNDNG_ESTIMATE_CHGS,
        EXTRAS_MSC_CAT_AMT_1,
        MIN_VENDOR_TYPE_4_COMMITMENT_LW_AMOUNT,
        LM_APRVD_ESTIMATE_CHGS,
        EXTRAS_NM_SUB_COST,
        WORKERS_COMP_GROUP,
        EXTRAS_JTD_WORK_BILLED,
        JTD_EQUIPMENT_UNITS,
        SCOPE_APPROVED,
        CITY,
        REQ_VENDOR_TYPE_1_CERTIFIED_RPTS,
        EXTRAS_LM_WORK_BILLED,
        QTD_PAYMENTS,
        CUSTOMER_CONTACT_2,
        CHECKLIST_8,
        ORIGINAL_COMMITMENT,
        NM_LABOR_UNITS,
        EXTRAS_CUSTOM_TOTAL_3_TOTAL,
        MISC_TITLE_3,
        EXTRAS_MTD_CST,
        TOTAL_OVERHEAD_COMMITMENT,
        STORED_MATERIAL,
        BILL_TO_ZIP_CODE,
        COMMITMENT_INVOICED__SUB,
        DEFAULT_REVENUE_CODE,
        EXTRAS_QTD_ADJUSTMENT,
        EXTRAS_YTD_RETAINAGE_HELD,
        YTD_ADJUSTMENT,
        RETAINAGE_PERCENT,
        COST_TO_COMP,
        COMMITMENT_INVOICED__EQUIPMENT,
        JTD_MATERIAL_DOLLARS_PAID,
        EXTRA_TOTAL_OTHER_ESTIMATE,
        CUSTOMER_FAX,
        TAX_ON_YTD_COST,
        EXTRAS_NM_WORK_BILLED,
        YTD_COST,
        COMMITMENT_INVOICED__OVERHEAD,
        EXTRAS_CO_REQUEST_CONTRACT_CHANGES,
        EXTRA_TOTAL_LABOR_COMMITMENT,
        EXTRAS_YTD_COST,
        EXTRAS_JTD_DOLLARS_PAID,
        JTD_OTHER_COST,
        QTD_COST,
        EXTRA_REVISED_COMMITMENT,
        CUSTOMER_ADDRESS_2,
        EXTRAS_CUSTOM_TOTAL_2_TOTAL,
        EXTRAS_LM_LABOR_COST,
        NM_RETAINAGE_HELD,
        EXTRA_TOTAL_SUB_COMMITMENT,
        YTD_DOLLARS_PAID,
        EXTRAS_MTD_APRVD_CNTRC_CHGS,
        EQUIPMENT_TAX_GROUP,
        EXTRAS_QTD_WORK_BILLED,
        JTD_DOLLARS_PAID,
        CHECKLIST_11,
        JTD_COST,
        TAX_ON_LM_COST,
        DESCRIPTION,
        EXTRA_TOTAL_EQUIPMENT_COMMITMENT,
        TOTAL_OVERHEAD_ESTIMATE,
        ACTUAL_COMPLETE_DATE,
        JTD_APRVD_ESTIMATE_CHGS,
        DAILY_ENTRY_CONTROL,
        NM_SUBCONTRACT_COST,
        REQ_VENDOR_TYPE_4_CERTIFIED_RPTS,
        MTD_SUBCONTRACT_COST,
        EXTRAS_LM_EQUIPMENT_UNITS,
        MTD_OVERHEAD_COST,
        MISC_PHONE,
        EXTRAS_JTD_MAT_DLLRS_PAID,
        LM_APRVD_CONTRACT_CHGS,
        MTD_APRVD_CONTRACT_CHGS,
        BURDEN_PERCENT,
        EXTRAS_LM_PAYMENTS,
        TAX_ON_EXTRAS_LW_DP,
        TAX_ON_LW_DOLLARS_PAID,
        COST_AT_COMP,
        MSC_JOB_AMT_2,
        EXTRA_PENDING_COMMITMENT_CHANGES,
        MSC_CAT_AMT_2,
        CHECKLIST_2,
        NM_DOLLARS_PAID,
        MTD_EQUIPMENT_COST,
        COST_ACCOUNT_GROUP,
        MTD_DOLLARS_PAID,
        CONTRACT_TYPE,
        TOTAL_LABOR_ESTIMATE,
        TOTAL_OTHER_ESTIMATE,
        EXTRAS_LW_DOLLARS_PAID,
        EXTRAS_NM_EQUIPMENT_COST,
        CUSTOMER_PHONE_1,
        WTD_EQUIPMENT_UNITS,
        EXTRAS_LM_APRVD_EST_CHGS,
        MTD_APRVD_ESTIMATE_CHGS,
        QTD_DOLLARS_PAID,
        FINANCE_CHARGE_FLAT_RATE,
        JTD_EQUIPMENT_DOLLARS_PAID,
        SHARED_DIVISION,
        BILLING_METHOD,
        MIN_VENDOR_TYPE_3_COMMITMENT_LW_AMOUNT,
        TIME_STAMP,
        TOTAL_EQUIPMENT_UNITS_EST,
        MTD_MATERIAL_COST,
        MASTER_CMPLNC_TOLERANCE,
        TAX_ON_NM_COST,
        STATUS,
        TOTAL_TAX_AMOUNT,
        SUBCONTRACT_TAX_GROUP,
        TOTAL_EQUIPMENT_COMMITMENT,
        USER_DEF_KEY_2,
        EXTRAS_MTD_SUB_COST,
        EXTRAS_TOTAL_MATERIAL_EST,
        CHECKLIST_4,
        NM_OTHER_COST,
        EXTRAS_TOTAL_LABOR_UNTS_EST,
        _FIVETRAN_SYNCED,
        _FIVETRAN_START,
        _FIVETRAN_END,
        _FIVETRAN_ACTIVE
    FROM KUSTOM_RAW.SAGE_DB_DBO.JCM_MASTER__JOB
    WHERE _FIVETRAN_ACTIVE = TRUE
)
;


-- View: VW_JCM_MASTER_COST_CODES
CREATE OR REPLACE VIEW SAGE_DB_DBO.VW_JCM_MASTER_COST_CODES AS
CREATE OR REPLACE VIEW KUSTOM_RAW.SAGE_DB_DBO.VW_JCM_MASTER_COST_CODES as (
    SELECT 
        CONCAT(JOB, '-', COST_CODE) AS JC_KEY,
        *
    FROM KUSTOM_RAW.SAGE_DB_DBO.JCM_MASTER__COST_CODE
    WHERE 
        _fivetran_deleted = FALSE 
        AND COST_CODE NOT LIKE '%-000' 
        AND EXTRA <> '99-FCA'
)
;


-- View: VW_JCT_CURRENT__TRANSACTION
CREATE OR REPLACE VIEW SAGE_DB_DBO.VW_JCT_CURRENT__TRANSACTION AS
create or replace view KUSTOM_RAW.SAGE_DB_DBO.VW_JCT_CURRENT__TRANSACTION(
	JC_KEY,
	ACCOUNTING_DATE,
	DIST_SEQUENCE,
	SEQUENCE,
	AP_PAYMENT_ID,
	TRANSACTION_TYPE,
	RUN,
	CHANGE_ORDER,
	DBID,
	TRANSACTION_DATE,
	"CHECK",
	UNIT_COST,
	JOB,
	SOURCE,
	BANK_ACCOUNT,
	PAYMENT_TYPE,
	CUSTOMER,
	OPERATOR_STAMP,
	AMOUNT,
	COST_CODE,
	CATEGORY,
	INVOICE,
	DRAW,
	APPLICATION_OF_ORIGIN,
	REF_1,
	FULL_ACCOUNT,
	DIVISION,
	ACCOUNT,
	UNITS,
	ROW_ID,
	COMMITMENT_TYPE,
	DATE_STAMP,
	TIME_STAMP,
	TRANSACTION_STATUS,
	RETAINAGE,
	VENDOR,
	ROW_VERSION,
	AR_INVOICE,
	AMOUNT_TYPE,
	DESCRIPTION,
	BATCH
) as (
    SELECT 
        CONCAT(JOB, '-', COST_CODE) AS JC_KEY
        ,ACCOUNTING_DATE
        ,dist_sequence
        ,sequence
        ,ap_payment_id
        ,transaction_type
        ,run
        ,change_order
        ,dbid
        ,transaction_date
        ,"CHECK"
        ,unit_cost
        ,job
        ,source
        ,bank_account
        ,payment_type
        ,customer
        ,operator_stamp
        ,amount
        ,cost_code
        ,category
        ,invoice
        ,draw
        ,application_of_origin
        ,ref_1
        ,case
            when credit_account is null then debit_account
            else credit_account
        end as full_account
        ,case
            when credit_account is null then left(debit_account,5)
            else left(credit_account,5)
        end as division
        ,case
            when credit_account is null then right(debit_account,7)
            else right(credit_account,7)
        end as account
        ,units
        ,row_id
        ,commitment_type
        ,date_stamp
        ,time_stamp
        ,transaction_status
        ,retainage
        ,vendor
        ,row_version
        ,ar_invoice
        ,amount_type
        ,description
        ,batch
    FROM KUSTOM_RAW.SAGE_DB_DBO.JCT_CURRENT__TRANSACTION
    WHERE _fivetran_deleted = FALSE AND COST_CODE NOT LIKE '%-000'
)
;


-- View: VW_JOBDATA
CREATE OR REPLACE VIEW SAGE_DB_DBO.VW_JOBDATA AS
create or replace view KUSTOM_RAW.SAGE_DB_DBO.VW_JOBDATA(
	JOB,
	DBID,
	DESCRIPTION,
	SIZE,
	SUPERVISOR,
	MARKETING,
	ESTIMATOR,
	REFERRAL_SOURCE,
	BD__MARKETING,
	STATUS,
	ESTIMATED_START_DATE,
	ESTIMATED_COMP_DATE,
	REVISED_START_DATE,
	REVISED_COMP_DATE,
	ACTUAL_START_DATE,
	ACTUAL_COMPLETE_DATE,
	LAST_COST_UPDATE,
	JOB_COMPLETE,
	AR_CUSTOMER,
	COST_ACCOUNT_PREFIX,
	SUPPLEMENTAL_REQUEST,
	APPROVED_SUPPLEMENTAL_AMOUNT,
	"Quote/T&M",
	CUST_ID,
	STATE,
	JOB_TYPE,
	REVISED_CONTRACT_AMOUNT,
	JTD_WORK_BILLED,
	MTD_WORK_BILLED,
	JTD_PAYMENTS,
	MTD_PAYMENTS,
	JTD_COST,
	POTENTIAL_CO_CONTRACT_CHANGES,
	LABOR_COST,
	DATE_STAMP,
	SUBCONTRACT_COST,
	ORIGINAL_CONTRACT_AMOUNT,
	ORIGINAL_ESTIMATE,
	ORIG_ESTIMATE_FINALIZED,
	ADDRESS,
	ZIP_CODE,
	CITY,
	COORDINATOR,
	ESTIMATED_COST_OVERRIDE,
	PERCENT_TO_COMPLETE_OVERRIDE,
	SAR_PO_COMMITTED_COST,
	DAILY_ENTRY_CONTROL,
	"SAR COs",
	"Invoiced SARS",
	REPORT_STATUS,
	PRODUCTION_CYCLE,
	TOTAL_CONTRACT,
	JOB_TYPE_NUMBER,
	NEW_DATE,
	JOB_MATCHING,
	JOB_MATCH_COUNT,
	DASH_DATE_ADDED,
	DASH_STATUS,
	DASH_LOSS_CATEGORY,
	DASH_REFERRAL_CATEGORY,
	DASH_PROVIDER_CATASTOPHE_NAME,
	DASH_REFERRAL_REPORTED_BY,
	DASH_REPORTED_BY,
	DASH_INSURANCE_CARRIER_NAME,
	DASH_TOTAL_ESTIMATES,
	DATE_OF_COS,
	DATE_PAID,
	DATE_MAJORITY_COMPLETE,
	DATE_TARGET_COMPLETION,
	REBUILD_CONVERSION,
	SIZE_GROUP,
    TPA_ROLE,
    TPA_COMPANY_NAME
) as
WITH initial_table as (
    SELECT 
        Job, 
        DBID,
        Description, 
        Size,
        Supervisor,
        Marketing,
        Estimator,
        Referral_Source,
        BD__Marketing,
        Status,
        Estimated_Start_Date,
        Estimated_Comp_Date,
        Revised_Start_Date,
        Revised_Comp_Date,
        Actual_Start_Date,
        Actual_Complete_Date,
        Last_Cost_Update,
        Job_Complete,
        AR_Customer,
        Cost_Account_Prefix,
        CO_Request_Contract_Changes as Supplemental_Request,
        jtd_aprvd_contract_chgs as Approved_Supplemental_Amount,
        lien__legal as "Quote/T&M",
        ar_customer as cust_id,
        STATE,
        job_type,
        Revised_Contract_Amount,
        JTD_Work_Billed,
        MTD_Work_Billed,
        JTD_Payments,
        MTD_Payments,
        JTD_Cost,
        potential_co_contract_changes,
        jtd_labor_cost as labor_cost,
        date_stamp,
        jtd_subcontract_cost as subcontract_cost, 
        original_contract_amount,
        original_estimate,
        orig_estimate_finalized,
        address_1 as address,
        zip_code,
        city,
        coordinator,
        msc_job_amt_2 as estimated_cost_override, 
        percent_complete as percent_to_complete_override,
        original_commitment as SAR_PO_COMMITTED_COST,
        daily_entry_control,
        approved_commitment_changes as "SAR COs",
        commitment_invoiced__sub as "Invoiced SARS",
        CASE
            WHEN Status = 'Unstarted' THEN 'Unstarted'
            WHEN Status = 'Closed' then 'Closed'
            WHEN job_complete = '0' THEN 'WIP'
            WHEN job_complete IS NULL then 'WIP'
            else 'Complete'
        END as Report_Status,
        CASE
            WHEN revised_Start_Date IS null THEN null
            WHEN Revised_Comp_Date IS null THEN null
            ELSE datediff(day,revised_start_date,revised_comp_date)
        END as Production_Cycle,
        CASE 
            WHEN revised_contract_Amount = 0 then CO_Request_Contract_Changes + Size
            ELSE Revised_Contract_Amount + CO_Request_Contract_Changes
        END AS Total_Contract,
        RIGHT(LEFT(Job,5),2) as Job_Type_Number
    FROM KUSTOM_RAW.SAGE_DB_DBO.JCM_MASTER__JOB
    where _fivetran_active = TRUE 
), secondary_table as (
    select *, 
    CASE 
        WHEN Report_Status = 'WIP' 
            THEN CASE
                WHEN Actual_Start_Date IS NULL 
                    THEN CASE
                        WHEN revised_start_date is null then estimated_start_date
                        else revised_start_date
                    end
                WHEN Revised_Start_Date > Actual_Start_Date THEN Revised_Start_Date
                ELSE Actual_Start_Date
            END
        WHEN Report_Status = 'Complete' THEN Revised_Comp_Date
        When Report_Status = 'Closed' 
            THEN CASE
                WHEN Actual_Complete_Date is null then revised_comp_date
                ELSE Actual_Complete_Date
            END
    END AS New_Date,
    CASE 
        WHEN Job_Type = '02' THEN CONCAT(LEFT(Job,2),RIGHT(Job,5))
        WHEN Job_Type = '04' THEN CONCAT(LEFT(Job,2),RIGHT(Job,5))
        ELSE 'n/a'
    END AS Job_Matching
    From initial_table
), job_match_check_conversion_count as (
    Select job_matching, COUNT(*) as job_match_count
    FROM secondary_table
    GROUP BY job_matching
)
SELECT 
    t1.*,
    t2.job_match_count,
    t3.dateadded as Dash_Date_Added,
    t3.status as Dash_Status,
    t3.provider_loss_category as Dash_Loss_Category,
    t3.referral_category as dash_referral_category,
    t3.Provider_Catastrophe_Name as dash_provider_catastophe_name,
    t3.referral_reported_by as dash_referral_reported_by,
    t3.REPORTED_BY AS dash_reported_by,   
    t4.insurance_company_name as dash_insurance_carrier_name,
    t5.total_estimates as dash_total_estimates,
    t6.DATE_OF_COS,
    t6.DATE_PAID,
    t6.DATE_MAJORITY_COMPLETE,
    t6.DATE_TARGET_COMPLETION,
    CASE 
        WHEN job_type <> '02' THEN 'n/a'
        WHEN job_match_count > 1 THEN 'Converted'
        ELSE 'Not Converted'
    END AS Rebuild_Conversion,
    case
        when total_contract <= 10000 then '0-10K'
        when total_contract <= 25000 then '10-25K'
        when total_contract <= 50000 then '25-50K'
        when total_contract <= 100000 then '50-100K'
        else '100K+'
    end as size_group,
    -- NEW FIELDS FROM EXTERNAL PARTICIPANTS VIEW (TPA)
    t7.TPA_ROLE,
    t7.TPA_COMPANY_NAME
from secondary_table t1
left join job_match_check_conversion_count t2
    ON t1.Job_Matching = t2.Job_Matching

left join (
    select *
    from (
        select *, 
            case
                when lastupdate = (max(lastupdate) over (partition by job_number))
                then 1 else 0
            end as duplicate_flag
        from kustom_raw.dash.vw_job_detail
    )
    where duplicate_flag = 1
) t3
    on t1.job = t3.job_number

left join (
    select insurance_company_name, job_id 
    from kustom_raw.dash.vw_job_external_participants
    where relationship_type = 'InsuranceCompany'
) t4
    on t3.job_id = t4.job_id

left join kustom_raw.dash.vw_accounting_summary t5
    on t3.job_id = t5.job_id

left join (
    SELECT 
        job_id,
        MAX(CASE WHEN PROCESS_POINT = 'Date of COS'                 THEN DATE END) AS DATE_OF_COS,
        MAX(CASE WHEN PROCESS_POINT = 'Date Paid'                   THEN DATE END) AS DATE_PAID,
        MAX(CASE WHEN PROCESS_POINT = 'Date of Majority Completion' THEN DATE END) AS DATE_MAJORITY_COMPLETE,
        MAX(CASE WHEN PROCESS_POINT = 'Date Target Completion'      THEN DATE END) AS DATE_TARGET_COMPLETION
    FROM kustom_raw.dash.vw_job_dates
    WHERE PROCESS_POINT IN ('Date of COS','Date Paid','Date of Majority Completion','Date Target Completion')
    GROUP BY job_id
) t6
    on t3.job_id = t6.job_id

-- NEW JOIN: bring in TPA fields per job
left join (
    select job_id,
           max(TPA_ROLE) as TPA_ROLE,
           max(TPA_COMPANY_NAME) as TPA_COMPANY_NAME
    from kustom_raw.dash.vw_job_external_participants
    where relationship_type = 'TPA'
    group by job_id
) t7
    on t3.job_id = t7.job_id
;


-- View: VW_JOBDATA_HISTORY
CREATE OR REPLACE VIEW SAGE_DB_DBO.VW_JOBDATA_HISTORY AS
create or replace view KUSTOM_RAW.SAGE_DB_DBO.VW_JOBDATA_HISTORY as (
    select 
        Job,
        DBID,
        _fivetran_active,
        _fivetran_start,
        _fivetran_end,
        _fivetran_synced,
        Size,
        Status,
        Estimated_Start_Date,
        Estimated_Comp_Date,
        Revised_Start_Date,
        Revised_Comp_Date,
        Actual_Start_Date,
        Actual_Complete_Date,
        Last_Cost_Update,
        Job_Complete,
        CO_Request_Contract_Changes as Supplemental_Request,
        jtd_aprvd_contract_chgs as Approved_Supplemental_Amount,
        Revised_Contract_Amount,
        JTD_Work_Billed,
        JTD_Payments,
        JTD_Cost,
        msc_job_amt_2 as estimated_cost_override, 
        percent_complete as percent_to_complete_override,
    -- report_status
    CASE
        WHEN Status = 'Unstarted' THEN 'Unstarted'
        WHEN Status = 'Closed' then 'Closed'
        WHEN job_complete = '0' THEN 'WIP'
        WHEN job_complete IS NULL then 'WIP'
        else 'Complete'
    END as Report_Status,
    -- total contract
    CASE 
        WHEN revised_contract_Amount = 0 then CO_Request_Contract_Changes + Size
        ELSE Revised_Contract_Amount + CO_Request_Contract_Changes
    END AS Total_Contract
    FROM JCM_MASTER__JOB
    where previous_day(current_date(),'su') between _fivetran_start and _fivetran_end
);


-- View: VW_JOBDATA_NEW
CREATE OR REPLACE VIEW SAGE_DB_DBO.VW_JOBDATA_NEW AS
create or replace view vw_JobData_new as
WITH initial_table as 
(
-- selection of necessary columns. Rename certain columns to fit jobdata pull
    SELECT 
        Job, 
        Description, 
        Size,
        Supervisor,
        Marketing,
        Estimator,
        Referral_Source,
        BD__Marketing,
        Status,
        Estimated_Start_Date,
        Estimated_Comp_Date,
        Revised_Start_Date,
        Revised_Comp_Date,
        Actual_Start_Date,
        Actual_Complete_Date,
        Last_Cost_Update,
        Job_Complete,
        AR_Customer,
        Cost_Account_Prefix,
    	CO_Request_Contract_Changes as Supplemental_Request,
        jtd_aprvd_contract_chgs as Approved_Supplemental_Amount,
        lien__legal as "Quote/T&M",
        ar_customer as cust_id,
        STATE,
        job_type,
        Revised_Contract_Amount,
        JTD_Work_Billed,
        --MTD_Work_Billed,
        JTD_Payments,
        --MTD_Payments,
        JTD_Cost,
        potential_co_contract_changes,
        jtd_labor_cost as labor_cost,
        date_stamp,
        jtd_subcontract_cost as subcontract_cost, 
        original_contract_amount,
        original_estimate,
        orig_estimate_finalized,
        address_1 as address,
        zip_code,
        city,
        msc_job_amt_2 as estimated_cost_override, 
        percent_complete as percent_to_complete_override,
        original_commitment as SAR_PO_COMMITTED_COST,
        approved_commitment_changes as "SAR COs",
        commitment_invoiced__sub as "Invoiced SARS",
    -- report_status
    CASE
        WHEN Status = 'Unstarted' THEN 'Unstarted'
        WHEN Status = 'Closed' then 'Closed'
        WHEN job_complete = '0' THEN 'WIP'
        WHEN job_complete IS NULL then 'WIP'
        else 'Complete'
    END as Report_Status,
   
 --production cycle calculation removed
    
-- total contract
    CASE 
        WHEN revised_contract_Amount = 0 then CO_Request_Contract_Changes + Size
        ELSE Revised_Contract_Amount + CO_Request_Contract_Changes
    END AS Total_Contract
    --RIGHT(LEFT(Job,5),2) as Job_Type_Number, no longer needed necessarily
    FROM JCM_MASTER__JOB
    where _fivetran_active = TRUE -- required for history mode to return most recent run
)
-- solely exists to calculate the complex logic for new date, this will need to be modified
    select *, 
    CASE 
        WHEN Report_Status = 'WIP' 
            THEN CASE
                WHEN Actual_Start_Date IS NULL 
                    THEN CASE
                        WHEN revised_start_date is null then estimated_start_date
                        else revised_start_date
                    end
                WHEN Revised_Start_Date > Actual_Start_Date THEN Revised_Start_Date
                ELSE Actual_Start_Date
            END
        WHEN Report_Status = 'Complete' THEN Revised_Comp_Date
        When Report_Status = 'Closed' 
            THEN CASE
                WHEN Actual_Complete_Date is null then revised_comp_date
                ELSE Actual_Complete_Date
            END
    END AS New_Date, -- New_date is relevant still but will be combined with other date info from dash to enable dates
    case
        when total_contract <= 10000 then '0-10K'
        when total_contract <= 25000 then '10-25K'
        when total_contract <= 50000 then '25-50K'
        when total_contract <= 100000 then '50-100K'
        else '100K+'
    end as size_group
    -- job_matching for conversions has been removed
    From initial_table
;


-- View: VW_JOB_COSTING
CREATE OR REPLACE VIEW DASH.VW_JOB_COSTING AS
create or replace view vw_job_costing as (
with job_costings_flat as 
(
select t1.job_id,costing1.index,costing1.value, case when _modified = (max(_modified) over (partition by t1.job_id)) then 1 else 0 end as latest_flag
from accountingdetail t1,
table(flatten(t1.job_costings)) as costing1
)
select j1.job_id,index,value:AddedBy::String as Added_By, value:Date::Date as Job_Costing_Date,value:Description::String as Description, value:Extended as Job_Costing_Amount,value:JobCostTypeCategory::String as Job_Cost_Type,value:JobCostingID as Job_Costing_ID,value:Memo::String as Memo,value:Quantity as Quantity,value:UOM::String as UOM,Value:Rate as Rate, Value:Status::String as Status, Value:TxnType::STring as Txn_Type,value:PO::STring as PO, value:PaymentTo::String as Payment_To
from job_costings_flat j1
where latest_flag = 1
);


-- View: VW_JOB_DATES
CREATE OR REPLACE VIEW DASH.VW_JOB_DATES AS
create or replace view vw_job_dates as (
with job_dates_flattened as (
select job_id, value, index, case when _modified = (max(_modified) over (partition by job_id)) then 1 else 0 end as latest_flag
from jobdates,
table(flatten(dates))
)
select job_id, value:AuditDetails:EnteredBy:FirstName::String || ' ' || value:AuditDetails:EnteredBy:LastName::String as Entered_Name, value:AuditDetails:WhenEntered::Date as Date_Entered, value:DateName::String as Process_Point, value:DateTypeID as Process_ID, value:Value::Date as Date
from job_dates_flattened
where latest_Flag = 1
);


-- View: VW_JOB_DETAIL
CREATE OR REPLACE VIEW DASH.VW_JOB_DETAIL AS
create or replace view KUSTOM_RAW.DASH.VW_JOB_DETAIL(
	DASHCOMPANYID,
	DASH_COMPANY_NAME,
	EXTERNALENTERPRISEID,
	FRANCHISEEID,
	NAME,
	ASSOCIATED_JOB_ID,
	JOB_NUMBER,
	JOB_NAME,
	DATEADDED,
    LOSS_CONTACT_FULLADDRESS,
	LASTUPDATE,
	TYPE,
	COMPLETION_PERCENTAGE,
	LOCATIONID,
	LOSS_DESCRIPTION,
	POLICY_HOLDER_TYPE,
	CLOSE_REASON,
	RECEIVEDBY,
	JOB_SOURCE,
	STATUS,
	PROVIDER_DIVISION_TYPE,
	PROVIDER_CATASTROPHE_NAME,
	CLAIMNUMBER,
	YEARBUILT,
	PROVIDER_LOSS_CATEGORY,
	TYPE_OF_LOSS,
	CUSTOMER_FULL_NAME,
	CUSTOMER_PRIMARY_CONTACT_NUMBER,
	REPORTED_BY,
	REFERRAL_CATEGORY,
	REFERRAL_TYPE,
	SOURCE_COMPANY,
	SOURCE_MARKETING_CAMPAIGN,
	REFERRAL_REPORTED_BY,
	JOB_ID,
	LATEST_FLAG,
	LATEST_ID_FLAG,
	LATEST_MODIFIED_FLAG,
	_MODIFIED,
	ADDRESSLINE1,
	ADDRESSLINE2,
	CITY,
	FIRSTNAME,
	LASTNAME,
	INDIVIDUALID,
	DIVISION_DASH,
	DASH_JOB_TYPE_NUMBER
) as (
with t1 as(
select 
    franchisee_info:DashCompanyID as dashcompanyid,
    franchisee_info:Name::String as Dash_Company_Name,
    franchisee_info:ExternalEnterpriseID::String as externalenterpriseid,
    franchisee_info:FranchiseeID as franchiseeid,
    franchisee_info:Name::String as name, 
    job_info:AssociatedJobID::String as Associated_Job_ID, 
    job_info:JobNumber::String as Job_Number, 
    job_info:JobName::String as Job_Name,
    job_info:DateAdded::date as DateAdded,
    CONCAT_WS(
      ', ',
      loss_contact_info:Address:AddressLine1::string,
      loss_contact_info:Address:City::string,
      loss_contact_info:Address:StateProvince::string,
      loss_contact_info:Address:PostalCode::string
  ) AS Loss_Contact_FullAddress,
    CASE 
        WHEN (job_info:DateLastUpdate::date IS NULL) THEN job_info:DateAdded::date
        ELSE job_info:DateLastUpdate::date
        END AS lastupdate, 
    job_info:Division::String as Type, 
    job_info:JobCompletionPercentage as Completion_Percentage, 
    job_info:LocationID as LocationID,
    Job_Info:LossDescription::String as Loss_Description, 
    job_info:PolicyHolderType::String as Policy_Holder_Type, 
    job_info:ProviderReasonForClosing::String as Close_Reason,
    job_info:ReceivedByFullName::String as ReceivedBy,
    job_info:JobSource::String as Job_Source,
    job_info:Status::String as Status,
    job_info:ProviderDivisionType::STring as Provider_Division_Type,
    job_info:ProviderCatastropheName::STRING as Provider_Catastrophe_Name,
    policy_claim_information:ClaimNumber::String as ClaimNumber, 
    POLICY_CLAIM_INFORMATION:YearBuilt::String as YearBuilt,
    policy_claim_information:ProviderLossCategory::String as Provider_Loss_Category,
    policy_claim_information:LossType::String as type_of_loss,
    CONCAT(
        job_referral_details:SourceEmployee:PersonName:LastName::String,
        ', ',
        job_referral_details:SourceEmployee:PersonName:FirstName::String
    ) as customer_full_name,
    loss_contact_info:MainPhone:Number::String as customer_primary_contact_number,
    policy_claim_information:ReportedBy::String as reported_by,
    job_referral_details:ReferralCategory as Referral_Category, 
    job_referral_details:ReferralType::String as Referral_Type,
    JOB_REFERRAL_DETAILS:SourceCompany:CompanyName::String as Source_Company,
    job_referral_details:SourceMarketingCampaign:CampaignName::String as Source_Marketing_Campaign,
    job_referral_details:SourceIndividual:PersonName:FirstName ||
        ' ' || 
        job_referral_details:SourceIndividual:PersonName:LastName 
    as Referral_Reported_By,
    jd.job_id,
    case 
        when lastupdate = (
            max(lastupdate) over (
                partition by job_number
                )
            )
        then 1 
        else 0 
    end as latest_flag,
    case
        when jd.job_id = (
            max(jd.job_id) over (
                partition by job_number
                )
            )
        then 1
        else 0
    end as latest_id_flag,
    case
        when _modified = (
            max(_modified) over (
                partition by jd.job_id
                )
            )
        then 1
        else 0
    end as latest_modified_flag,
    _modified,
    jdc.addressline1,
    jdc.addressline2,
    jdc.city,
    jdc.firstname,
    jdc.lastname,
    jdc.individualid
from kustom_raw.dash.jobdetail jd
left join kustom_raw.dash.vw_job_detail_customer jdc
on jd.job_id = jdc.job_id
)
select t1.*,
    '99-'||left(right(t1.job_number,5),2) as division_dash,
    right(left(job_number, 5),2) as dash_job_type_number
from t1 
where 
    --latest_flag = 1 and 
    latest_id_flag = 1 and latest_modified_flag = 1
);


-- View: VW_JOB_DETAILS
CREATE OR REPLACE VIEW SALESFORCE_INPUT.VW_JOB_DETAILS AS
CREATE OR REPLACE VIEW KUSTOM_RAW.SALESFORCE_INPUT.VW_JOB_DETAILS AS (
    WITH t1 AS (
        SELECT
            Job_ID,
            Claim_Info:Caller:FirstName,
            Claim_Info:Caller:LastName,
            Claim_Info:CatReferenceNumber,
            Claim_Info:ClaimCustomer,
            Claim_Info:ClaimEnteredBy:FirstName,
            Claim_Info:ClaimEnteredBy:LastName,
            Claim_Info:ClaimID,
            Claim_Info:ClientID,
            Claim_Info:ClientName,
            Claim_Info:CodeRedID,
            Claim_Info:DateOfLoss,
            Claim_Info:DateReceived,
            Claim_Info:IsProviderCreatedClaim,
            Claim_Info:LossCategory,
            Claim_Info:PolicyLimits:ContentsAmount,
            Claim_Info:PolicyLimits:DwellingAmount,
            Claim_Info:PolicyLimits:OtherStructuresAmount,
            Claim_Info:PreferredProviderLocationName,
            Claim_Info:PreferredProviderName,
            Claim_Info:ProviderLossCategory,
            Claim_Info:ReferenceID,
            Claim_Info:ReportedBy,
            Claim_Info:TypeOfLoss,
            Franchisee_Info:DashCompanyID,
            Franchisee_Info:ExternalEnterpriseID,
            Franchisee_Info:FranchiseeID,
            Franchisee_Info:Name,
            Job_Info:AssociatedJobID,
            Job_Info:DateAdded,
            Job_Info:DateLastUpdate,
            Job_Info:Division,
            Job_Info:EnvironmentalCode,
            Job_Info:EnvironmentalCodeDescription,
            Job_Info:InitialFindings,
            Job_Info:IsAdminJob,
            Job_Info:IsClosed,
            Job_Info:IsOnHold,
            Job_Info:JobCompletionPercentage,
            Job_Info:JobName,
            Job_Info:JobNumber,
            Job_Info:JobSource,
            Job_Info:LocationID,
            Job_Info:LossDescription,
            Job_Info:MasterBuilderNumber,
            Job_Info:PolicyHolderType,
            Job_Info:Priority,
            Job_Info:ProviderCatastropheName,
            Job_Info:ProviderDivisionType,
            Job_Info:ProviderReasonForClosing,
            Job_Info:ReceivedByFullName,
            Job_Info:ReferralFeeDatePaid,
            Job_Info:RegionalAreaManager:FirstName,
            Job_Info:RegionalAreaManager:LastName,
            Job_Info:RoomsAffected,
            Job_Info:SpecialInstructions,
            Job_Info:Status,
            Job_Info:WaterJobCat,
            Job_Info:WaterJobClass,
            Policy_Claim_Information:ClaimNumber,
            Policy_Claim_Information:DatePolicyExpiration,
            Policy_Claim_Information:DatePolicyStart,
            Policy_Claim_Information:ExternalFileNumber,
            Policy_Claim_Information:JobSize,
            Policy_Claim_Information:LossCategory,
            Policy_Claim_Information:LossType,
            Policy_Claim_Information:PolicyNumber,
            Policy_Claim_Information:ProviderLossCategory,
            Policy_Claim_Information:ReferredByFullName,
            Policy_Claim_Information:ReportedBy,
            Policy_Claim_Information:SecondaryLossType,
            Policy_Claim_Information:SourceOfLoss,
            Policy_Claim_Information:YearBuilt,
            Payment_Services:CollectWhen,
            Payment_Services:DateLienLiened,
            Payment_Services:DateLienReleased,
            Payment_Services:DateLienRights,
            Payment_Services:DeductibleAmount,
            Payment_Services:MbJobNumber,
            Loss_Contact_Info:Address:AddressLine1,
            Loss_Contact_Info:Address:AddressLine2,
            Loss_Contact_Info:Address:City,
            Loss_Contact_Info:Address:Country,
            Loss_Contact_Info:Address:County,
            Loss_Contact_Info:Address:PostalCode,
            Loss_Contact_Info:Address:StateProvince,
            Loss_Contact_Info:ContactPerson:FirstName,
            Loss_Contact_Info:ContactPerson:LastName,
            Loss_Contact_Info:MainPhone:Extension,
            Loss_Contact_Info:MainPhone:Number,
            Customer:CompanyDetails:CompanyID,
            Customer:CompanyDetails:CompanyName,
            Customer:CompanyDetails:ContactsDetails:Address:AddressLine1,
            Customer:CompanyDetails:ContactsDetails:Address:AddressLine2,
            Customer:CompanyDetails:ContactsDetails:Address:City,
            Customer:CompanyDetails:ContactsDetails:Address:Country,
            Customer:CompanyDetails:ContactsDetails:Address:County,
            Customer:CompanyDetails:ContactsDetails:Address:PostalCode,
            Customer:CompanyDetails:ContactsDetails:Address:StateProvince,
            Customer:CompanyDetails:ContactsDetails:BillingAddress,
            Customer:CompanyDetails:ContactsDetails:Email,
            Customer:CompanyDetails:ContactsDetails:MailingAddress,
            Customer:CompanyDetails:ContactsDetails:MainPhone:Extension,
            Customer:CompanyDetails:ContactsDetails:MainPhone:Number,
            Customer:CompanyDetails:ContactsDetails:OtherPhones,
            Customer:CompanyDetails:ContactsDetails:Website,
            Customer:CompanyDetails:MarketingRank,
            Customer:IndividualDetails,
            Customer:JobPolicyHolderType,
            Job_Referral_Details:ReferralCategory,
            Job_Referral_Details:ReferralType,
            Job_Referral_Details:SalesStage,
            Job_Referral_Details:SalesStatus,
            Job_Referral_Details:SourceCompany,
            Job_Referral_Details:SourceEmployee,
            Job_Referral_Details:SourceIndividual:ContactCategory,
            Job_Referral_Details:SourceIndividual:MarketingRank,
            Job_Referral_Details:SourceIndividual:PersonName:FirstName,
            Job_Referral_Details:SourceIndividual:PersonName:LastName,
            Job_Referral_Details:SourceMarketingCampaign,
            CASE 
                WHEN _modified = (MAX(_modified) OVER (PARTITION BY Job_ID)) 
                    then 1 
                    else 0 
                end as latest_flag
        FROM KUSTOM_RAW.DASH.JOBDETAIL
    )
    SELECT *
    FROM t1
    WHERE latest_flag = 1
)
;


-- View: VW_JOB_DETAIL_CUSTOMER
CREATE OR REPLACE VIEW DASH.VW_JOB_DETAIL_CUSTOMER AS
CREATE OR REPLACE VIEW kustom_raw.dash.VW_JOB_DETAIL_CUSTOMER as
with t1 as (
SELECT distinct
    JOB_ID
    ,CASE 
        WHEN (job_info:DateLastUpdate::date IS NULL) THEN job_info:DateAdded::date
        ELSE job_info:DateLastUpdate::date
        END AS lastupdate
    ,case 
        when _modified = (
            max(_modified) over (
                partition by job_id
                )
            )
        then 1 
        else 0 
    end as latest_flag
    ,result.value:Address:AddressLine1::string as AddressLine1
    ,result.value:Address:AddressLine2::string as AddressLine2
    ,result.value:Address:City::string as City
    ,result.value:Address:StateProvince::string as State
    ,CUSTOMER:IndividualDetails:IndividualID as IndividualID
    ,initcap(CUSTOMER:IndividualDetails:PersonName:FirstName::string) as FirstName
    ,initcap(CUSTOMER:IndividualDetails:PersonName:LastName::String) as LastName
FROM kustom_raw.dash.jobdetail,
TABLE(FLATTEN(kustom_raw.dash.jobdetail.CUSTOMER:IndividualDetails,'ContactsDetails')) result)
select distinct *
from t1
where latest_flag=1;


-- View: VW_JOB_EXTERNAL_PARTICIPANTS
CREATE OR REPLACE VIEW DASH.VW_JOB_EXTERNAL_PARTICIPANTS AS
create or replace view KUSTOM_RAW.DASH.VW_JOB_EXTERNAL_PARTICIPANTS (
    JOB_ID,
    BILL_TO_TYPE,
    INDEX,
    COMPANY_ID,
    INSURANCE_COMPANY_NAME,
    RELATIONSHIP_TYPE,
    INSURANCE_COMPANY_CONTACT,
    TPA_ROLE,
    TPA_COMPANY_NAME
) as (
    with external_participants_flattened as (
        select 
            job_id, 
            bill_to_type,
            index,
            value,
            case 
                when _modified = (max(_modified) over (partition by job_id)) 
                then 1 
                else 0 
            end as latest_flag
        from jobexternalparticipants,
             table(flatten(company_relations))        
    ),
    external_individuals_flattened as (
        select
            job_id,
            bill_to_type,
            index,
            value,
            case
                when _modified = (max(_modified) over (partition by job_id)) 
                then 1 
                else 0 
            end as latest_flag
        from jobexternalparticipants,
             table(flatten(individual_relations))
        where index = 0
        qualify (case
                    when _modified = (max(_modified) over (partition by job_id)) 
                    then 1 
                    else 0 
                 end) = 1
    )
    select distinct
        a.job_id,
        a.bill_to_type,
        a.index,
        a.value:CompanyDetails:CompanyID                          as Company_ID,
        a.value:CompanyDetails:CompanyName::string                as Insurance_Company_Name,
        a.value:RelationshipTypeName::string                      as relationship_type,
        concat(
            b.value:IndividualDetails:PersonName:FirstName::string,
            ' ',
            b.value:IndividualDetails:PersonName:LastName::string
        )                                                         as Insurance_Company_Contact,

        /* NEW COLUMNS */
        case 
            when a.value:RelationshipTypeName::string = 'TPA' 
                 then 'TPA' 
            else null 
        end                                                       as TPA_ROLE,

        case 
            when a.value:RelationshipTypeName::string = 'TPA' 
                 then a.value:CompanyDetails:CompanyName::string 
            else null 
        end                                                       as TPA_COMPANY_NAME

    from external_participants_flattened a
    left join external_individuals_flattened b
      on a.job_id = b.job_id 
     and a.index = b.index
     and a.value:RelationshipType::string = b.value:RelationshipType::string
    where a.latest_flag = 1 
      and a.index = 0
);


-- View: VW_JOB_EXTERNAL_PARTICIPANTS
CREATE OR REPLACE VIEW SALESFORCE_INPUT.VW_JOB_EXTERNAL_PARTICIPANTS AS
CREATE OR REPLACE VIEW KUSTOM_RAW.SALESFORCE_INPUT.VW_JOB_EXTERNAL_PARTICIPANTS AS (
    WITH external_participants_flattened AS (
        SELECT 
            job_id, 
            bill_to_type,
            index,
            value,
            CASE 
                WHEN _modified = (MAX(_modified) OVER (PARTITION BY job_id)) 
                THEN 1 
                ELSE 0 
            END AS latest_flag
        FROM KUSTOM_RAW.DASH.JOBEXTERNALPARTICIPANTS,
        TABLE(FLATTEN(company_relations))        
    ),
    
    external_individuals_flattened AS (
        SELECT
            job_id,
            bill_to_type,
            index,
            value,
            CASE 
                WHEN _modified = (MAX(_modified) OVER (PARTITION BY job_id)) 
                THEN 1 
                ELSE 0 
            END AS latest_flag
        FROM KUSTOM_RAW.DASH.JOBEXTERNALPARTICIPANTS,
        TABLE(FLATTEN(individual_relations))
    )
    
    SELECT 
        a.Job_ID::String AS Job_ID,
        a.Bill_to_Type::String AS Bill_to_Type,
        a.Value:CompanyDetails:CompanyID::String AS CompanyID,
        a.Value:CompanyDetails:CompanyName::String AS CompanyName,
        a.Value:CompanyDetails:ContactsDetails:Address:AddressLine1::String AS Company_AddressLine1,
        a.Value:CompanyDetails:ContactsDetails:Address:AddressLine2::String AS Company_AddressLine2,
        a.Value:CompanyDetails:ContactsDetails:Address:City::String AS Company_City,
        a.Value:CompanyDetails:ContactsDetails:Address:Country::String AS Company_Country,
        a.Value:CompanyDetails:ContactsDetails:Address:County::String AS Company_County,
        a.Value:CompanyDetails:ContactsDetails:Address:PostalCode::String AS Company_PostalCode,
        a.Value:CompanyDetails:ContactsDetails:Address:StateProvince::String AS Company_State,
        a.Value:CompanyDetails:ContactsDetails:BillingAddress:AddressLine1::String AS Billing_AddressLine1,
        a.Value:CompanyDetails:ContactsDetails:BillingAddress:AddressLine2::String AS Billing_AddressLine2,
        a.Value:CompanyDetails:ContactsDetails:BillingAddress:City::String AS Billing_City,
        a.Value:CompanyDetails:ContactsDetails:BillingAddress:Country::String AS Billing_Country,
        a.Value:CompanyDetails:ContactsDetails:BillingAddress:County::String AS Billing_County,
        a.Value:CompanyDetails:ContactsDetails:BillingAddress:PostalCode::String AS Billing_PostalCode,
        a.Value:CompanyDetails:ContactsDetails:BillingAddress:StateProvince::String AS Billing_State,
        a.Value:CompanyDetails:ContactsDetails:Email::String AS Company_Email,
        a.Value:CompanyDetails:ContactsDetails:MailingAddress:AddressLine1::String AS Mailing_AddressLine1,
        a.Value:CompanyDetails:ContactsDetails:MailingAddress:AddressLine2::String AS Mailing_AddressLine2,
        a.Value:CompanyDetails:ContactsDetails:MailingAddress:City::String AS Mailing_City,
        a.Value:CompanyDetails:ContactsDetails:MailingAddress:Country::String AS Mailing_Country,
        a.Value:CompanyDetails:ContactsDetails:MailingAddress:County::String AS Mailing_County,
        a.Value:CompanyDetails:ContactsDetails:MailingAddress:PostalCode::String AS Mailing_PostalCode,
        a.Value:CompanyDetails:ContactsDetails:MailingAddress:StateProvince::String AS Mailing_State,
        a.Value:CompanyDetails:ContactsDetails:MainPhone:Extension::String AS Company_PhoneExtension,
        a.Value:CompanyDetails:ContactsDetails:MainPhone:Number::String AS Company_Phone,
        a.Value:CompanyDetails:ContactsDetails:Website::String AS Company_Website,
        a.Value:CompanyDetails:MarketingRank::String AS Company_MarketingRank,
        a.Value:RelationshipType::String AS RelationshipType,
        a.Value:RelationshipTypeName::String As RelationshipTypeName,
        b.Value:IndividualDetails:ContactsDetails:Address:AddressLine1::String AS Individual_AddressLine1,
        b.Value:IndividualDetails:ContactsDetails:Address:AddressLine2::String AS Individual_AddressLine2,
        b.Value:IndividualDetails:ContactsDetails:Address:City::String AS Individual_City,
        b.Value:IndividualDetails:ContactsDetails:Address:Country::String AS Individual_Country,
        b.Value:IndividualDetails:ContactsDetails:Address:County::String AS Individual_County,
        b.Value:IndividualDetails:ContactsDetails:Address:PostalCode::String AS Individual_PostalCode,
        b.Value:IndividualDetails:ContactsDetails:Address:StateProvince::String AS Individual_State,
        b.Value:IndividualDetails:ContactsDetails:Email::String AS Individual_Email,
        b.Value:IndividualDetails:ContactsDetails:MainPhone:Extension::String AS Individual_PhoneExtension,
        b.Value:IndividualDetails:ContactsDetails:MainPhone:Number::String AS Individual_Phone,
        b.Value:IndividualDetails:IndividualID::String AS IndividualID,
        b.Value:IndividualDetails:MarketingRank::String AS Individual_MarketingRank,
        b.Value:IndividualDetails:PersonName:FirstName::String AS Individual_Firstname,
        b.Value:IndividualDetails:PersonName:LastName::String AS Individual_LastName,
        b.Value:IndividualDetails:PreferredCommunicationMethod::String AS Individual_PreferredCommunicationMethod,
        b.Value:IndividualDetails:Title::String AS Individual_Title
    FROM external_participants_flattened a
    LEFT JOIN external_individuals_flattened b
    ON 
        a.job_id = b.job_id 
        AND a.index = b.index 
        AND a.value:RelationshipType::String = b.value:RelationshipType::String 
    WHERE 
        a.latest_flag = 1 
        AND b.latest_flag = 1 
)
;


-- View: VW_JOB_GL_METRICS
CREATE OR REPLACE VIEW SAGE_DB_DBO.VW_JOB_GL_METRICS AS
create or replace view KUSTOM_RAW.SAGE_DB_DBO.VW_JOB_GL_METRICS as (
    select 
        dbid,
        batch,
        account as original_account, 
        left(account,5) as division, 
        right(account,7) as account,
        job,
        accounting_date,
        date_stamp,
        transaction_desc,
        sum(credit) as credit, 
        sum(debit) as debit, 
        sum(credit) + sum(debit) as balance
    from glt_current__transaction
    where right(account,7) >= 4000.00 
        and right(account,7) < 8000.00 
        --and DBID = 'KUSTOMUS' 
        and _fivetran_deleted = FALSE
    group by all
);


-- View: VW_JOB_ID_NUMBER_LOOKUP
CREATE OR REPLACE VIEW DASH.VW_JOB_ID_NUMBER_LOOKUP AS
create or replace view vw_job_id_number_lookup as (
    WITH t1 AS (
        select 
            distinct job_id,
            job_number,
            case 
                when _modified = (
                    max(_modified) over (
                        partition by job_id
                        )
                    )
                then 1 
                else 0 
            end as latest_flag,
        from vw_job_detail
    )
    SELECT 
        JOB_ID,
        JOB_NUMBER
    FROM t1
    WHERE latest_flag = 1
);


-- View: VW_JOB_INTERNAL_PARTICIPANTS
CREATE OR REPLACE VIEW SALESFORCE_INPUT.VW_JOB_INTERNAL_PARTICIPANTS AS
CREATE OR REPLACE VIEW KUSTOM_RAW.SALESFORCE_INPUT.VW_JOB_INTERNAL_PARTICIPANTS AS (
    WITH internal_participants_flattened AS (
        SELECT 
            job_id, 
            index,
            value,
            CASE 
                WHEN _modified = (MAX(_modified) OVER (PARTITION BY job_id)) 
                THEN 1 
                ELSE 0 
            END AS latest_flag
        FROM KUSTOM_RAW.DASH.INTERNALPARTICIPANTS,
        TABLE(FLATTEN(participants))        
    )

    SELECT 
        Job_ID,
        Value:ID AS ParticipantID,
        Value:PersonName:FirstName::String AS Participant_FirstName,
        Value:PersonName:LastName::String AS Participant_LastName,
        Value:Type::String AS Type
    FROM internal_participants_flattened
    WHERE latest_flag = 1
)
;


-- View: VW_LOCATION_DETAIL
CREATE OR REPLACE VIEW SALESFORCE_INPUT.VW_LOCATION_DETAIL AS
CREATE OR REPLACE VIEW KUSTOM_RAW.SALESFORCE_INPUT.VW_LOCATION_DETAIL AS (
    WITH t1 AS (
        SELECT
            Provider_ID,
            Name,
            Main_Fax,
            Location_ID,
            Address:AddressLine1,
            Address:AddressLine2,
            Address:City,
            Address:Country,
            Address:County,
            Address:PostalCode,
            Address:StateProvince,
            EDI_Addresses:AddressType,
            EDI_Addresses:AddressValue,
            Main_Phone:Extension,
            Main_Phone:Number,
            Primary_Contact:Email,
            Primary_Contact:Name:FirstName,
            Primary_Contact:Name:LastName,
            Primary_Contact:Phone,
            Business_Development_Manager_Name:FirstName,
            Business_Development_Manager_Name:LastName,
            CASE 
                WHEN _modified = (MAX(_modified) OVER (PARTITION BY Location_ID)) 
                    then 1 
                    else 0 
                end as latest_flag
        FROM KUSTOM_RAW.DASH.LOCATIONDETAIL
    )
    SELECT *
    FROM t1
    WHERE latest_flag = 1
)
;


-- View: VW_PROVIDER_DETAILS
CREATE OR REPLACE VIEW SALESFORCE_INPUT.VW_PROVIDER_DETAILS AS
CREATE OR REPLACE VIEW KUSTOM_RAW.SALESFORCE_INPUT.VW_PROVIDER_DETAILS AS (
    WITH t1 AS (
        SELECT
            Franchisor_ID,
            Name,
            Company_ID,
            Enterprise_ID,
            Type,
            Primary_Email,
            Website,
            Is_Active,
            Provider_ID,
            Address:AddressLine1,
            Address:AddressLine2,
            Address:City,
            Address:Country,
            Address:County,
            Address:PostalCode,
            Address:StateProvince,
            CASE 
                WHEN _modified = (MAX(_modified) OVER (PARTITION BY PROVIDER_ID)) 
                    then 1 
                    else 0 
                end as latest_flag
        FROM KUSTOM_RAW.DASH.PROVIDERDETAILS
    )
    SELECT *
    FROM t1
    WHERE latest_flag = 1
)
;


-- View: VW_STG_AR_COMPARISON
CREATE OR REPLACE VIEW SAGE_DB_DBO.VW_STG_AR_COMPARISON AS
create or replace view kustom_raw.sage_db_dbo.vw_stg_ar_comparison
as
select dbid
    ,accounting_date
    ,month(accounting_date) as month
    ,year(accounting_date) as year
    ,right(credit_account__accrual,7) as account
    ,left(credit_account__accrual,5) as division
    ,batch
    ,batch_source
    ,job
    ,amount_type
    ,credit_account__accrual as full_account
    ,transaction_type
    ,invoice
    ,application_of_origin
    ,sum(amount) as amount
from kustom_raw.sage_db_dbo.art_current__transaction
where _fivetran_deleted = FALSE
group by all;


-- View: VW_TSCOST
CREATE OR REPLACE VIEW TIMBERSCAN_DB_DBO_DBO.VW_TSCOST AS
create or replace view KUSTOM_RAW.TIMBERSCAN_DB_DBO_DBO.VW_TSCOST as (SELECT 
    t3.Vendor, 
    t3.Invoice, t3.Description AS Invoice_Description, 
    t3.Invoice_Date, t3.Discount_Date, 
    t3.Payment_Date, t3.Accounting_Date, 
    t2.Amount, t2.Expense_Account,
    t2.Job, t2.DistSeq, t2.Category, t1.insertedby, t1.dateinserted,t2.cost_code,t3.date_stamp,t3.operator_stamp
FROM tblInvoices t1
    LEFT OUTER JOIN APM_MASTER__DISTRIBUTION t2 ON t1.InvoiceID = t2.InvoiceID 
    LEFT OUTER JOIN APM_MASTER__INVOICE t3 ON t1.InvoiceID = t3.InvoiceID
    where t1.deleted=0 and insertedinap=0 and t1._fivetran_active = TRUE and t2._fivetran_active=TRUE and t3._fivetran_active= TRUE);


-- View: VW_TSCOST_ALL
CREATE OR REPLACE VIEW TIMBERSCAN_DB_DBO_DBO.VW_TSCOST_ALL AS
CREATE or replace VIEW vw_TSCost_all AS SELECT 
    t3.Vendor, 
    t3.Invoice, t3.Description AS Invoice_Description, 
    t3.Invoice_Date, t3.Discount_Date, 
    t3.Payment_Date, t3.Accounting_Date, 
    t3.date_stamp,
    t2.Amount, t2.Expense_Account,t2.accounts_payable_account,
    t2.Job, t2.DistSeq, t2.Category, t1.insertedby, t1.dateinserted,t2.cost_code,t1.deleted,t1.insertedinap
FROM tblInvoices t1
    LEFT OUTER JOIN APM_MASTER__DISTRIBUTION t2 ON t1.InvoiceID = t2.InvoiceID 
    LEFT OUTER JOIN APM_MASTER__INVOICE t3 ON t1.InvoiceID = t3.InvoiceID
    where t1._fivetran_active=TRUE and t2._fivetran_active=TRUE and t3._fivetran_active=TRUE;


-- View: VW_TSCOST_HISTORY
CREATE OR REPLACE VIEW TIMBERSCAN_DB_DBO_DBO.VW_TSCOST_HISTORY AS
CREATE or replace VIEW vw_TSCost_history AS SELECT 
    t3.Vendor, 
    t3.Invoice, t3.Description AS Invoice_Description, 
    t3.Invoice_Date, t3.Discount_Date, 
    t3.Payment_Date, t3.Accounting_Date, 
    t2.Amount, t2.Expense_Account,
    t2.Job, t2.DistSeq, t2.Category, t1.insertedby, t1.dateinserted,t2.cost_code,
    t1._fivetran_synced as main_sync,
    t2._fivetran_synced as secondary_sync,
    t3._fivetran_synced as tertiary_sync
FROM tblInvoices t1
    LEFT OUTER JOIN APM_MASTER__DISTRIBUTION t2 ON t1.InvoiceID = t2.InvoiceID 
    LEFT OUTER JOIN APM_MASTER__INVOICE t3 ON t1.InvoiceID = t3.InvoiceID
    where t1.deleted=0 and insertedinap=0 and (previous_day(current_date(),'su') between t1._fivetran_start and t1._fivetran_end) and (previous_day(current_Date(),'su') between t2._fivetran_start and t2._fivetran_end) and (previous_day(current_date(),'su') between t3._fivetran_start and t3._fivetran_end);