USE DATABASE KUSTOM_PREPARED;
SELECT 
    '-- View: ' || table_name || '\n' ||
    'CREATE OR REPLACE VIEW ' || table_schema || '.' || table_name || ' AS\n' ||
    view_definition || '\n\n' AS ddl_script
FROM information_schema.views
where view_definition is not null
ORDER BY table_name;

DDL_SCRIPT
-- View: SAGE_DASH_NAME_CLEAN_UP
CREATE OR REPLACE VIEW SAGE_DASH_COMPARISON.SAGE_DASH_NAME_CLEAN_UP AS
CREATE OR REPLACE VIEW sage_dash_name_clean_up AS (
    SELECT
        t1.job_number,
        t1.estimator_dash,
        t1.supervisor_dash,
        t1.marketing_dash,
        t1.coordinator_dash,
        t1.quality_control_dash,
        t1.accounting_dash,
        t2.cost_account_prefix,
        t2.estimator,
        t2.supervisor,
        t2.marketing
    FROM vw_internal_participants_pivot t1
    LEFT OUTER JOIN vw_job_data_history_v1 t2
    ON t1.job_number = t2.job
)
;


-- View: VW_ACCOUNTING_SUMMARY_DASH_PREPARED
CREATE OR REPLACE VIEW PREPARED_COPY.VW_ACCOUNTING_SUMMARY_DASH_PREPARED AS
create or replace view vw_accounting_summary_dash_prepared as (
    select *
    from kustom_raw.dash.vw_accounting_summary
);


-- View: VW_ADP_HEADCOUNT_PREPARED
CREATE OR REPLACE VIEW PREPARED_COPY.VW_ADP_HEADCOUNT_PREPARED AS
create or replace view KUSTOM_PREPARED.PREPARED_COPY.VW_ADP_HEADCOUNT_PREPARED(
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
) as (
    select *
    from kustom_raw.adp.vw_adp_headcount
);


-- View: VW_AGGREGATE_CONTRACT
CREATE OR REPLACE VIEW REVENUE_MODEL.VW_AGGREGATE_CONTRACT AS
create or replace view kustom_prepared.revenue_model.vw_aggregate_contract as 
(select 
        job
        ,transaction_start_date
        ,type
        ,lead(
            dateadd(
                day,-1,transaction_start_date
            ),
            1,
            '9999-12-31 23:59:59.000 '
        ) over(
            partition by job
            order by transaction_start_date
        ) as transaction_end_date
        ,t1.estimated_cost_percentage_override_new
        ,sum(amount) over(
            partition by job,transaction_type
            order by transaction_start_date
        ) as contract_value
        ,sum(amount) over(
            partition by job,transaction_type
            order by transaction_start_date
        ) * t1.estimated_cost_percentage_override_new as estimated_cost
    from kustom_prepared.revenue_model.vw_contract_value t1
    );


-- View: VW_AGGREGATE_CONTRACT_NEW
CREATE OR REPLACE VIEW REVENUE_MODEL.VW_AGGREGATE_CONTRACT_NEW AS
CREATE OR REPLACE VIEW kustom_prepared.revenue_model.vw_aggregate_contract_new as
WITH OrderedTransactions AS (
    SELECT
        job,
        transaction_start_date,
        time_stamp,
        transaction_type,
        type,
        amount,
        estimated_cost_percentage_override_new,
        -- Capture overriding and incremental values
        CASE
            WHEN transaction_type = 'Size' THEN amount
            ELSE NULL
        END AS size_value,
        CASE
            WHEN transaction_type = 'Scheduled Value' THEN amount
            ELSE 0
        END AS scheduled_value
        ,date_stamp
        ,operator_stamp
    FROM
        kustom_prepared.revenue_model.vw_contract_value
),
AggregatedValues AS (
    SELECT
        job,
        transaction_start_date,
        time_stamp,
        transaction_type,
        amount,
        type,
        estimated_cost_percentage_override_new,
        -- Propagate the most recent `size` value
        MAX(size_value) OVER (
            PARTITION BY job
            ORDER BY transaction_start_date, time_stamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS propagated_size_value,
        -- Calculate cumulative scheduled value
        SUM(scheduled_value) OVER (
            PARTITION BY job
            ORDER BY transaction_start_date,time_stamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_scheduled_value
        ,date_stamp
        ,operator_stamp
    FROM
        OrderedTransactions
),
FinalCalculations AS (
    SELECT
        job,
        transaction_start_date,
        time_stamp,
        transaction_type,
        type,
        LEAD(
            DATEADD(
                minute, -1, transaction_start_date
            ),
            1,
            '9999-12-31 23:59:59.000'
        ) OVER (
            PARTITION BY job
            ORDER BY transaction_start_date, time_stamp
        ) AS transaction_end_date,
        estimated_cost_percentage_override_new,
        -- Final running contract value calculation
        COALESCE(propagated_size_value, 0) + cumulative_scheduled_value AS running_contract_value
        ,date_stamp
        ,operator_stamp
    FROM
        AggregatedValues
)
SELECT
    job,
    transaction_start_date,
    transaction_end_date,
    transaction_type,
    type,
    time_stamp,
    estimated_cost_percentage_override_new,
    -- Use running_contract_value for calculations
    running_contract_value AS contract_value,
    running_contract_value * estimated_cost_percentage_override_new AS estimated_cost
    ,date_stamp
    ,operator_stamp
FROM
    FinalCalculations;


-- View: VW_AGGREGATE_CONTRACT_NO_NULLS
CREATE OR REPLACE VIEW REVENUE_MODEL.VW_AGGREGATE_CONTRACT_NO_NULLS AS
create or replace view kustom_prepared.revenue_model.vw_aggregate_contract_no_nulls as 
(
    select 
        job
        ,transaction_start_date
        ,type
        ,transaction_end_date
        ,estimated_cost_percentage_override_new
        ,COALESCE(CASE WHEN type IN ('Size','Contract Value') then first_value(contract_value) over (partition by job order by transaction_start_date) else contract_value end,contract_value) as contract_value
        ,COALESCE(case when type in ('Size','Contract Value') then first_value(estimated_cost) over (partition by job order by transaction_start_date) else estimated_cost end,estimated_cost) as estimated_cost
    from kustom_prepared.revenue_model.vw_aggregate_contract
);


-- View: VW_AGGREGATE_COST
CREATE OR REPLACE VIEW REVENUE_MODEL.VW_AGGREGATE_COST AS
create or replace view kustom_prepared.revenue_model.vw_aggregate_cost as 
    (select job
        ,transaction_type
        ,accounting_date as accounting_start_date
        ,lead(
            dateadd(
                minute,-1,accounting_date
            )
            ,1,'9999-12-31 23:59:59.000  '
        ) over (
            partition by job order by accounting_date, try_to_time(time_stamp)
        ) as accounting_end_date
        ,try_to_time(time_stamp) as time_stamp
        ,sum(amount) as amount
        ,'Sage' as cost_source
        ,transaction_type as type
        ,to_timestamp(date(t1.date_stamp) || ' ' ||to_time(t1.time_stamp)) as date_stamp
        ,t1.operator_stamp
    from kustom_raw.sage_db_dbo.vw_jct_current__transaction t1
    where transaction_type in ('AP cost','JC cost','PR cost')
    group by job
        ,accounting_date
        ,transaction_type
        ,time_stamp
        ,date_stamp
        ,operator_stamp
    union all
    select job
        ,'Timberscan' as transaction_type
        ,accounting_date as accounting_start_date
        ,lead(
            dateadd(
                minute,-1,accounting_date
            )
            ,1,'9999-12-31 23:59:59.000  '
        ) over (
            partition by job order by accounting_date
        ) as accounting_end_date
        ,time(accounting_date) as time_stamp
        ,sum(amount) as amount
        ,'Timberscan' as cost_source
        ,'Timberscan' as type
        ,date_stamp
        ,operator_stamp
    from kustom_prepared.prepared_copy.vw_tscost_prepared
    group by job, accounting_date,date_stamp,operator_stamp);


-- View: VW_APEX_AR
CREATE OR REPLACE VIEW APEX.VW_APEX_AR AS
create or replace view KUSTOM_PREPARED.APEX.VW_APEX_AR(
    _FILE,
    CLIENT_NAME,
    JOB,
    "CURRENT",
    _1_30,
    _31_60,
    _61_90,
    _90,
    TOTAL
) as
WITH filled AS (
  SELECT
      _FILE,
      FIRST_VALUE(CLIENT) IGNORE NULLS OVER (
          ORDER BY _line DESC
          ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
      ) AS CLIENT_NAME,
      JOB,
      "CURRENT",
      _1_30,
      _31_60,
      _61_90,
      _90,
      TOTAL
  FROM kustom_raw.skyvia_sharepoint.apex_ar_reporting
  WHERE TO_DATE(SUBSTR(_FILE, 1, 10), 'YYYY-MM-DD') = (
      SELECT MAX(TO_DATE(SUBSTR(_FILE, 1, 10), 'YYYY-MM-DD'))
      FROM kustom_raw.skyvia_sharepoint.apex_ar_reporting
      WHERE JOB IS NOT NULL
  )
)

SELECT *
FROM filled
WHERE JOB IS NOT NULL;


-- View: VW_APEX_INVOICE_AMTS_PREPARED
CREATE OR REPLACE VIEW PREPARED_COPY.VW_APEX_INVOICE_AMTS_PREPARED AS
create or replace view KUSTOM_PREPARED.PREPARED_COPY.VW_APEX_INVOICE_AMTS_PREPARED(
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
    select *
    from kustom_raw.sage_db_dbo.vw_apex_invoice_amts
);


-- View: VW_APEX_INVOICE_META_PREPARED
CREATE OR REPLACE VIEW PREPARED_COPY.VW_APEX_INVOICE_META_PREPARED AS
create or replace view KUSTOM_PREPARED.PREPARED_COPY.VW_APEX_INVOICE_META_PREPARED(
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
    select *
    from kustom_raw.sage_db_dbo.vw_apex_invoice_meta
);


-- View: VW_APEX_INVOICE_PMTS_PREPARED
CREATE OR REPLACE VIEW PREPARED_COPY.VW_APEX_INVOICE_PMTS_PREPARED AS
create or replace view KUSTOM_PREPARED.PREPARED_COPY.VW_APEX_INVOICE_PMTS_PREPARED(
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
) as(
    select 
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
    from kustom_raw.sage_db_dbo.VW_APEX_INVOICE_PMTS
);


-- View: VW_APEX_JOB_DATA
CREATE OR REPLACE VIEW SAGE_DASH_COMPARISON.VW_APEX_JOB_DATA AS
create or replace view KUSTOM_PREPARED.SAGE_DASH_COMPARISON.VW_APEX_JOB_DATA(
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
    TPA_COMPANY_NAME,
	CUSTOMER_FULL_NAME,
	ESTIMATED_COST_PERCENTAGE_OVERRIDE_NEW,
	TOTAL_CONTRACT_PREV_WEEK,
	WORK_BILLED_PREV_WEEK,
	REPORT_STATUS_PREV_WEEK,
	ACTUAL_COMPLETE_DATE_PREV_WEEK,
	REVISED_COMP_DATE_PREV_WEEK,
	JOB_COMPLETE_PREV_WEEK,
	ESTIMTED_COST_OVERRIDE_PREV_WEEK,
	PERCENT_TO_COMPLETE_OVERRIDE_PREV_WEEK,
	JTD_COST_PREV_WEEK,
	JTD_WORK_BILLED_PREV_WEEK
) as

WITH invoice_summary AS (
  SELECT
    i."CustomerName",
    SUM(i."Subtotal") AS JTD_Work_Billed,
    SUM(i."AppliedAmount") AS JTD_Payments,
    SUM(CASE 
        WHEN DATE_TRUNC('MONTH', i."TxnDate") = DATE_TRUNC('MONTH', CURRENT_DATE)
        THEN i."Subtotal" ELSE 0 END) AS MTD_Work_Billed,
    SUM(CASE 
        WHEN DATE_TRUNC('MONTH', i."TxnDate") = DATE_TRUNC('MONTH', CURRENT_DATE)
        THEN i."AppliedAmount" ELSE 0 END) AS MTD_Payments,
    //SUM(CASE 
        //WHEN DATE_TRUNC('MONTH', i."TxnDate") = DATE_TRUNC('MONTH', DATEADD('MONTH', -1, CURRENT_DATE))
        //THEN i."Subtotal" ELSE 0 END) AS Prior_Month_Work_Billed,
    //SUM(CASE 
        //WHEN DATE_TRUNC('MONTH', i."TxnDate") = DATE_TRUNC('MONTH', DATEADD('MONTH', -1, CURRENT_DATE))
        //THEN i."AppliedAmount" ELSE 0 END) AS Prior_Month_Payments
  FROM kustom_raw.skyvia."Invoice" i
  //WHERE "CustomerName" ilike '%MOYNIHAN%'
  GROUP BY i."CustomerName"
  
)

SELECT
    c."Name" AS JOB /// This is JOB
    , NULL AS DBID
    , c."JobDesc" AS "DESCRIPTION"
    , NULL as SIZE
    , t3.supervisor_dash AS SUPERVISOR
    , t3.marketing_dash AS MARKETING
    , t3.estimator_dash AS ESTIMATOR
    , dj.referral_reported_by as REFERRAL_SOURCE
    , dj.SOURCE_MARKETING_CAMPAIGN as BD__MARKETING
    , case 
        when c."JobStatus" = 'NotAwarded' then 'Void' 
        when c."JobStatus" = 'Pending' then 'Unstarted' 
        when c."JobStatus" = 'InProgress' then 'In progress' 
        when c."JobStatus" = 'Closed' then 'Closed' 
      end as STATUS
    , NULL AS ESTIMATED_START_DATE
    , c."JobProjectedEndDate" AS ESTIMATED_COMP_DATE
    , c."JobStartDate" AS REVISED_START_DATE
    , NULL AS REVISED_COMP_DATE
    , c."JobStartDate" AS ACTUAL_START_DATE
    , c."JobEndDate" AS ACTUAL_COMPLETE_DATE
    , NULL AS LAST_COST_UPDATE
    , CASE
        WHEN c."JobStatus" = 'Closed' THEN 'true'
        Else false
      END AS JOB_COMPLETE
    , c."ParentName" AS AR_CUSTOMER
    , '99-83' as COST_ACCOUNT_PREFIX
    , NULL AS SUPPLEMENTAL_REQUEST
    , NULL AS APPROVED_SUPPLEMENTAL_AMOUNT
    , NULL AS "Quote/T&M"
    , c."ParentId" AS CUST_ID
    , c."BillState" AS STATE
    , c."JobTypeName" AS JOB_TYPE
    , e."TotalAmount" AS REVISED_CONTRACT_AMOUNT
    , s.JTD_Work_Billed AS JTD_WORK_BILLED
    , s.MTD_Work_Billed AS MTD_WORK_BILLED
    , -s.JTD_Payments AS JTD_PAYMENTS
    //, (
        //SELECT SUM(i."AppliedAmount")
       // FROM kustom_raw.skyvia."Invoice" i
       // WHERE i."CustomerName" = c."FullName"
     // ) AS JTD_Payments
    , s.MTD_Payments AS MTD_PAYMENTS
    , t2.total_job_cost as JTD_COST
    , NULL AS POTENTIAL_CO_CONTRACT_CHANGES
    , t2.labor_cost AS LABOR_COST
    , c."TimeCreated" AS DATE_STAMP
    , t2.subtrade_cost as SUBCONTRACT_COST
    , e."TotalAmount" AS ORIGINAL_CONTRACT_AMOUNT
    , NULL AS ORIGINAL_ESTIMATE
    , NULL AS ORIG_ESTIMATE_FINALIZED
    , c."BillAddr2" AS ADDRESS
    , c."BillPostalCode" AS ZIP_CODE
    , c."BillCity" AS CITY
    , t3.coordinator_dash AS COORDINATOR
    , NULL AS ESTIMATED_COST_OVERRIDE
	, CASE 
        WHEN e."TotalAmount" * (1 - t2.ESTIMATED_GP_PERCENTAGE_AFTER_WOADADJUSMENT) = 0 
         OR e."TotalAmount" * (1 - t2.ESTIMATED_GP_PERCENTAGE_AFTER_WOADADJUSMENT) IS NULL 
         OR e."TotalAmount" IS NULL 
        THEN NULL 
        ELSE t2.total_job_cost / (e."TotalAmount" * (1-(t2.ESTIMATED_GP_PERCENTAGE_AFTER_WOADADJUSMENT)))
      END AS PERCENT_TO_COMPLETE_OVERRIDE
	, NULL AS SAR_PO_COMMITTED_COST
	, NULL AS DAILY_ENTRY_CONTROL
	, NULL AS "SAR COs"
	, NULL AS "Invoiced SARS"
	, CASE
        WHEN c."JobStatus" = 'Pending' THEN 'Unstarted'
        WHEN c."JobStatus" = 'Closed' then 'Closed'
        WHEN c."JobStatus" = '0' THEN 'WIP'
        WHEN c."JobStatus" IS NULL then 'WIP'
        WHEN c."JobStatus" = 'InProgress' then 'WIP'
        else 'Complete'
      END as Report_Status
	, NULL AS PRODUCTION_CYCLE
    , e."TotalAmount" AS TOTAL_CONTRACT
    , dj.dash_job_type_number AS JOB_TYPE_NUMBER
    , NULL AS NEW_DATE
    , NULL AS JOB_MATCHING
    , NULL AS JOB_MATCH_COUNT
    , dj.dateadded as DASH_DATE_ADDED
    , dj.status as DASH_STATUS
    , dj.provider_loss_category as DASH_LOSS_CATEGORY
    , dj.referral_category as DASH_REFERRAL_CATEGORY
    , dj.Provider_Catastrophe_Name as DASH_PROVIDER_CATASTOPHE_NAME
    , dj.referral_reported_by as DASH_REFERRAL_REPORTED_BY
    , dj.reported_by as DASH_REPORTED_BY
    , NULL AS DASH_INSURANCE_CARRIER_NAME
    , NULL AS DASH_TOTAL_ESTIMATES
    , t6.DATE_OF_COS
    , t6.DATE_PAID
    , t6.DATE_MAJORITY_COMPLETE
    , t6.DATE_TARGET_COMPLETION
    , NULL AS REBUILD_CONVERSION
    , case
        when e."TotalAmount" <= 10000 then '0-10K'
        when e."TotalAmount" <= 25000 then '10-25K'
        when e."TotalAmount" <= 50000 then '25-50K'
        when e."TotalAmount" <= 100000 then '50-100K'
        else '100K+'
      end as size_group
    , NULL as PA_ROLE
    , NULL AS TPA_COMPANY_NAME
    , c."FullName" AS CUSTOMER_FULL_NAME
    , NULL AS ESTIMATED_COST_PERCENTAGE_OVERRIDE_NEW
	, NULL AS TOTAL_CONTRACT_PREV_WEEK
	, NULL AS WORK_BILLED_PREV_WEEK
	, NULL AS REPORT_STATUS_PREV_WEEK
	, NULL AS ACTUAL_COMPLETE_DATE_PREV_WEEK
	, NULL AS REVISED_COMP_DATE_PREV_WEEK
	, NULL AS JOB_COMPLETE_PREV_WEEK
	, NULL AS ESTIMTED_COST_OVERRIDE_PREV_WEEK
	, NULL AS PERCENT_TO_COMPLETE_OVERRIDE_PREV_WEEK
	, NULL AS JTD_COST_PREV_WEEK
	, NULL AS JTD_WORK_BILLED_PREV_WEEK
    //, c. "CustomerTypeName" AS STATUS_1
    //, c."JobStatus" AS STATUS_2
    ///, c.JTD Billed
    ///, c.MTD BIlled
    //, i."AppliedAmount" AS JTD_PAYMENTS 
    //, e."TotalAmount" AS ORIGINAL_CONTRACT_AMOUNT
    //, dj.ASSOCIATED_JOB_ID
    //, dj.referral_reported_by as REFERRAL_SOURCE
    //, dj.RECEIVEDBY

FROM KUSTOM_RAW.SKYVIA."Customer" c
//LEFT JOIN KUSTOM_RAW.SKYVIA."Invoice" i
    //ON c."FullName" = i."CustomerName"
LEFT JOIN KUSTOM_RAW.SKYVIA."Estimate" e
    ON c."Id" = e."CustomerId"
//LEFT JOIN history h
    //on c."FullName" = "CustomerName"
LEFT JOIN invoice_summary s
    ON c."FullName" = s."CustomerName"
LEFT JOIN kustom_raw.dash.vw_job_detail dj
    on c."Name" = dj.job_number
LEFT JOIN KUSTOM_PREPARED.PREPARED_COPY.VW_ACCOUNTING_SUMMARY_DASH_PREPARED t2
    on dj.job_id = t2.job_id
LEFT JOIN KUSTOM_PREPARED.SAGE_DASH_COMPARISON.VW_INTERNAL_PARTICIPANTS_PIVOT t3
    on c."Name" = t3.job_number

LEFT JOIN (
    SELECT 
        job_id,
       MAX(CASE WHEN PROCESS_POINT = 'Date of COS' THEN DATE END) AS DATE_OF_COS,
       MAX(CASE WHEN PROCESS_POINT = 'Date Paid' THEN DATE END) AS DATE_PAID,
       MAX(CASE WHEN PROCESS_POINT = 'Date of Majority Completion' THEN DATE END) AS DATE_MAJORITY_COMPLETE,
       MAX(CASE WHEN PROCESS_POINT = 'Date Target Completion' THEN DATE END) AS DATE_TARGET_COMPLETION
    FROM kustom_raw.dash.vw_job_dates
    WHERE PROCESS_POINT IN ('Date of COS', 'Date Paid', 'Date of Majority Completion', 'Date Target Completion')
    GROUP BY job_id
) t6

on dj.ASSOCIATED_JOB_ID = t6.job_id
where c."ParentId" is not null;


-- View: VW_APEX_WIP_JOBS
CREATE OR REPLACE VIEW APEX.VW_APEX_WIP_JOBS AS
CREATE OR REPLACE VIEW KUSTOM_PREPARED.APEX.VW_APEX_WIP_JOBS AS
SELECT 
    _file,
    BILLED_C_EP_ AS BILLED_C_EP,
    _10_OH_CURRENT_ AS "10_PERCENT_OH_CURRENT",
    C_EP,
    COST,
    PGM_ AS PGM_PERCENT,
    CTC,
    _JOB_NAME AS JOB,
    _OF_BUDGET AS PERCENT_OF_BUDGET,
    UNDERBILLED,
    _10_OH_TOTAL_ AS "10_PERCENT_OH_TOTAL",
    TEAM,
    BUDGET,
    _DONE AS PERCENT_DONE,
    BILLED,
    CONTRACT,
    OVERBILLED
FROM KUSTOM_RAW.SKYVIA_SHAREPOINT.APEX_WIP_JOBS
WHERE _JOB_NAME IS NOT NULL
  AND TO_DATE(SUBSTR(_FILE, 1, 10), 'YYYY-MM-DD') = (
      SELECT MAX(TO_DATE(SUBSTR(_FILE, 1, 10), 'YYYY-MM-DD'))
      FROM KUSTOM_RAW.SKYVIA_SHAREPOINT.APEX_WIP_JOBS
      WHERE _JOB_NAME IS NOT NULL);


-- View: VW_CASH_DEPOSIT_REGISTER
CREATE OR REPLACE VIEW PREPARED_COPY.VW_CASH_DEPOSIT_REGISTER AS
create or replace view KUSTOM_PREPARED.PREPARED_COPY.VW_CASH_DEPOSIT_REGISTER (
    BANK_ACCOUNT,
    DEPOSIT_ID,
    PREFIX,
    TYPE, 
    DESCRIPTION,
    ACCOUNTING_DATE,
    SUBTRACTION,
    ADDITION,
    POSTED,
    GL_DEBIT_ACCOUNT,
    GL_CREDIT_ACCOUNT,
    RECONCILIATION_STATUS,
    CLEARED_AMOUNT,
    CLEARED_DATE,
    RECONCILE_IN_PROGRESS,
    STATEMENT_DATE,
    AP_PAYMENT_ID,
    PAYMENT_TYPE,
    "CHECK",
    PAYEE,
    CHECK_TYPE,
    VOIDED, 
    VOID_DATE,
    VOIDED_AMOUNT,
    STOP_PAYMENT,
    STOP_PAYMENT_DATE,
    DEPOSIT_DATE,
    DEPOSIT_TYPE,
    WITHDRAWAL_TYPE,
    ADJUSTMENT_TYPE,
    APPLICATION_OF_ORIGIN,
    BATCH,
    BATCH_SOURCE,
    OPERATOR_STAMP,
    DATE_STAMP
) as (
    select 
    BANK_ACCOUNT,
    DEPOSIT_ID,
    LEFT(DEPOSIT_ID, 5) AS PREFIX,
    TYPE, 
    DESCRIPTION,
    ACCOUNTING_DATE,
    SUBTRACTION,
    ADDITION,
    POSTED,
    GL_DEBIT_ACCOUNT,
    GL_CREDIT_ACCOUNT,
    RECONCILIATION_STATUS,
    CLEARED_AMOUNT,
    CLEARED_DATE,
    RECONCILE_IN_PROGRESS,
    STATEMENT_DATE,
    AP_PAYMENT_ID,
    PAYMENT_TYPE,
    "CHECK",
    PAYEE,
    CHECK_TYPE,
    VOIDED, 
    VOID_DATE,
    VOIDED_AMOUNT,
    STOP_PAYMENT,
    STOP_PAYMENT_DATE,
    DEPOSIT_DATE,
    DEPOSIT_TYPE,
    WITHDRAWAL_TYPE,
    ADJUSTMENT_TYPE,
    APPLICATION_OF_ORIGIN,
    BATCH,
    BATCH_SOURCE,
    OPERATOR_STAMP,
    DATE_STAMP
    from kustom_raw.sage_db_dbo.cmt_register__transaction
);


-- View: VW_COMBINED_COST
CREATE OR REPLACE VIEW REVENUE_MODEL.VW_COMBINED_COST AS
create or replace view kustom_prepared.revenue_model.vw_combined_Cost as 
(select t1.job
        ,t1.accounting_date
        ,t1.time_stamp
        ,t1.cost_amount
        ,t1.type
        ,COALESCE(
        CASE WHEN contract_value IS NULL OR contract_value = 0 
             THEN first_VALUE(contract_value) OVER (
                    PARTITION BY job
                    ORDER BY accounting_date desc
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                  )
             ELSE contract_value
        END,
        contract_value
    ) AS contract_value
        ,COALESCE(
            CASE WHEN estimated_cost IS NULL OR estimated_cost = 0 
                 THEN first_VALUE(estimated_cost) OVER (
                        PARTITION BY job
                        ORDER BY accounting_date desc
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                      )
                 ELSE estimated_cost
            END,
            estimated_cost
        ) AS estimated_cost
        ,COALESCE(
            CASE WHEN estimated_cost_percentage_override_new IS NULL OR estimated_cost_percentage_override_new = 0 
                 THEN first_VALUE(estimated_cost_percentage_override_new) OVER (
                        PARTITION BY job
                        ORDER BY accounting_date desc
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                      )
                 ELSE estimated_cost_percentage_override_new
            END,
            estimated_cost_percentage_override_new
        ) AS estimated_cost_percentage_override_new
        ,sum(
            cost_amount
        ) over(
            partition by job order by accounting_date
        ) as jtd_cost
        ,cost_source
        ,date_stamp
        ,operator_stamp
    from kustom_prepared.revenue_model.vw_combined_pre_cost t1);


-- View: VW_COMBINED_PRE_COST
CREATE OR REPLACE VIEW REVENUE_MODEL.VW_COMBINED_PRE_COST AS
create or replace view kustom_prepared.revenue_model.vw_combined_pre_cost as 
(select t1.job
        ,t1.accounting_start_date as accounting_date
        ,amount as cost_amount
        ,contract_value
        ,estimated_cost
        ,t2.estimated_cost_percentage_override_new
        ,cost_source
        ,t1.type
        ,t1.time_stamp
        ,t1.date_stamp
        ,t1.operator_stamp
    from kustom_prepared.revenue_model.vw_aggregate_cost t1
    left join kustom_prepared.revenue_model.vw_aggregate_contract_new t2
    on t1.job=t2.job
        and t1.accounting_start_date between t2.transaction_start_date and t2.transaction_end_date
        -- and t1.job = '24-02-31423' and t2.job = '24-02-31423'
    
    union all
    
    select t2.job
        ,t2.transaction_start_date as accounting_date
        ,0 as cost_amount
        ,contract_value
        ,estimated_cost
        ,t2.estimated_cost_percentage_override_new
        ,cost_source
        ,t2.type
        ,t2.time_stamp
        ,t2.date_stamp
        ,t2.operator_stamp
    from kustom_prepared.revenue_model.vw_aggregate_contract_new t2
    left join kustom_prepared.revenue_model.vw_aggregate_cost t1
    on t2.job = t1.job 
        and t2.transaction_start_date between t1.accounting_start_date and t1.accounting_end_date);


-- View: VW_COMBINED_TABLE
CREATE OR REPLACE VIEW REVENUE_MODEL.VW_COMBINED_TABLE AS
create or replace view kustom_prepared.revenue_model.vw_combined_table as (
select t1.job
        ,t1.accounting_date
        ,t1.cost_amount
        ,t1.contract_value
        ,t1.estimated_cost
        ,t1.jtd_cost
        ,t1.estimated_cost_percentage_override_new
        ,t1.type
        ,t1.time_stamp
        ,case 
            when t3.actual_complete_date <= accounting_date then 1
            when t1.jtd_cost >= estimated_cost then 1
            else div0(t1.jtd_cost,estimated_cost)
        end as percent_complete_actual
        ,t2.percent_complete_override
        ,t3.job_type_number
        ,t3.revised_comp_date
        ,t3.actual_complete_date
        ,dateadd(
            day,14,revised_comp_date
        ) as little_rule_02_date
        ,dateadd(
            day,21,revised_comp_date
        ) as little_rule_04_date
        ,cost_source
        ,t1.date_stamp
        ,t1.operator_stamp
    from kustom_prepared.revenue_model.vw_combined_cost t1
    left join kustom_prepared.revenue_model.vw_percent_complete_override t2
        on t1.job = t2.job
            and t1.accounting_date between t2.transaction_start_date and t2.transaction_end_date
    left join kustom_prepared.prepared_copy.vw_job_data_prepared t3
    on t1.job = t3.job
    where right(description,4) NOT LIKE '%VOID%' AND right(description,3) NOT LIKE '%JNS%'
);


-- View: VW_CONCUR_EXPENSE_PREPARED
CREATE OR REPLACE VIEW PREPARED_COPY.VW_CONCUR_EXPENSE_PREPARED AS
create or replace view vw_concur_expense_prepared as (
    select * 
    from kustom_raw.concur.vw_concur_expense
);


-- View: VW_CONTRACT_VALUE
CREATE OR REPLACE VIEW REVENUE_MODEL.VW_CONTRACT_VALUE AS
create or replace view kustom_prepared.revenue_model.vw_contract_value as 
(select 
        t1.job as job
        ,transaction_date as transaction_start_date
        ,'Scheduled Value' as transaction_type
        ,t2.estimated_cost_percentage_override_new
        ,sum(amount) as amount
        ,'Contract Value' as type
        ,try_to_time(time_stamp) as time_stamp
        ,to_timestamp(date(t1.date_stamp) || ' ' ||to_time(t1.time_stamp)) as date_stamp
        ,t1.operator_stamp
    from kustom_raw.sage_db_dbo.vw_jct_current__transaction t1
    right join (
        select distinct job
            ,estimated_cost_percentage_override_new
            ,size
        from kustom_prepared.prepared_copy.vw_job_data_prepared
    )t2
    using(job)
    where transaction_type in ('Scheduled value','Aprvd schdl val chng','Pndng schd val chg 2','Pndng schdl val chng') and amount <> 0
    group by all
    
    union all

    select distinct
        job as job
        ,new_date as transaction_date
        ,'Size' as transaction_type
        ,t1.estimated_cost_percentage_override_new
        ,size as amount
        ,'Size' as type
        ,time(date_stamp) as time_stamp
        ,to_timestamp(date(date_stamp) || ' ' ||to_time(time_stamp)) as date_stamp
        ,null as operator_stamp
    from kustom_prepared.prepared_copy.vw_job_data_prepared t1
    where size <> 0 

    union all

    select distinct 
        t1.job as job
        ,dateadd(day,14,t2.revised_comp_date) as transaction_start_date
        ,'Scheduled Value' as transaction_type
        ,t2.estimated_cost_percentage_override_new
        ,0 as amount
        ,'Little Rule non 04' as type
        ,time(t2.date_stamp) as time_stamp
        ,to_timestamp(date(t1.date_stamp) || ' ' ||to_time(t1.time_stamp)) as date_stamp
        ,t1.operator_stamp
    from kustom_raw.sage_db_dbo.vw_jct_current__transaction t1
    right join (
        select distinct job
            ,estimated_cost_percentage_override_new
            ,size
            ,revised_comp_date
            ,job_type_number
            ,date_stamp
        from kustom_prepared.prepared_copy.vw_job_data_prepared
    ) t2
    using(job)
    where job_type_number in ('01','02','05','06','08','10') and revised_comp_date is not null 
        
    union all

    select distinct
        t2.job as job
        ,dateadd(day,21,t2.revised_comp_date) as  transaction_start_date
        ,'Scheduled Value' as transaction_type
        ,t2.estimated_cost_percentage_override_new
        ,0 as amount
        ,'Little Rule 04' as type
        ,time(t2.date_stamp) as time_stamp
        ,to_timestamp(date(t1.date_stamp) || ' ' ||to_time(t1.time_stamp)) as date_stamp
        ,t1.operator_stamp
    from kustom_raw.sage_db_dbo.vw_jct_current__transaction t1
    right join (
        select distinct job
            ,estimated_cost_percentage_override_new
            ,size
            ,revised_comp_date
            ,job_type_number
            ,date_stamp
        from kustom_prepared.prepared_copy.vw_job_data_prepared
    ) t2
    using(job)
    where job_type_number in ('04') and revised_comp_date is not null

    union all 

    select distinct
        t2.job as job
        ,actual_complete_date as transaction_start_date
        ,'Scheduled Value' as transaction_type
        ,t2.estimated_cost_percentage_override_new
        ,0 as amount
        ,'Complete Date' as type
        ,time(t2.date_stamp) as time_stamp
        ,to_timestamp(date(t1.date_stamp) || ' ' ||to_time(t1.time_stamp)) as date_stamp
        ,t1.operator_stamp
    from kustom_raw.sage_db_dbo.vw_jct_current__transaction t1
    right join (
        select distinct job
            ,estimated_cost_percentage_override_new
            ,size
            ,actual_complete_date
            ,job_type_number
            ,date_stamp
        from kustom_prepared.prepared_copy.vw_job_data_prepared
    ) t2
    using(job)
    where actual_complete_date is not null

    union all

    --use work billed dates to create transaction placeholders
    select distinct
        t1.job
        ,t1.accounting_start_date as transaction_start_date
        ,'Scheduled Value' as transaction_type
        ,t2.estimated_cost_percentage_override_new
        ,0 as amount
        ,'Work Billed' as type
        ,time(t2.date_stamp) as time_stamp
        ,t1.date_stamp
        ,t1.operator_stamp
    from kustom_prepared.revenue_model.vw_work_billed_aggregation t1
    right join (
        select distinct job
            ,estimated_cost_percentage_override_new
            ,size
            ,actual_complete_date
            ,job_type_number
            ,date_stamp
        from kustom_prepared.prepared_copy.vw_job_data_prepared
    ) t2
    using (job)
    union all
    --union the percent complete transactions in as well
    select distinct
        t1.job
        ,t1.transaction_start_date
        ,'Scheduled Value' as transaction_type
        ,t2.estimated_cost_percentage_override_new
        ,0 as amount
        ,'Percent Complete' as type
        ,time(t1.time_stamp) as time_stamp
        ,t1.date_stamp
        ,t1.operator_stamp
    from kustom_prepared.revenue_model.vw_percent_complete_override t1
    right join (
        select distinct job
            ,estimated_cost_percentage_override_new
            ,size
            ,actual_complete_date
            ,job_type_number
            ,date_stamp
        from kustom_prepared.prepared_copy.vw_job_data_prepared
    ) t2
    using (job)
        );


-- View: VW_COUPA_JOB_COST_CODE
CREATE OR REPLACE VIEW COUPA_INPUT.VW_COUPA_JOB_COST_CODE AS
create or replace view KUSTOM_PREPARED.COUPA_INPUT.VW_COUPA_JOB_COST_CODE(
	NAME,
	ACTIVE,
	LOOKUP,
	DESCRIPTION,
	"External Ref Num",
	"External Ref Code",
	"Chart of Accounts",
	DEFAULT,
	"Unique Job",
	TIMESTAMP
) as (
select 
    description as name,
    case 
        when status in ('In progress', 'Unstarted') then TRUE 
        else false 
    end as Active,
    'Job_Number' as Lookup,
    '' as Description,
    cost_code as "External Ref Num",
    concat(job,' | ', cost_code) as "External Ref Code", 
    '' as "Chart of Accounts",
    'No' as Default,
    concat(description, ' (',job,')') as "Unique Job",
    _Fivetran_synced as timestamp
from kustom_raw.sage_db_dbo.jcm_master__cost_code
);


-- View: VW_COUPA_JOB_MASTER
CREATE OR REPLACE VIEW COUPA_INPUT.VW_COUPA_JOB_MASTER AS
create or replace view vw_coupa_job_master as (
select 
    concat(description, ' (',job,')') as Name, 
    case 
        when status = 'In progress' then TRUE 
        else false 
    end as Active, 
    'Job Number' as Lookup, 
    description,
    Job as "External Ref Num",
    Job as "External Ref Code",
    '' as "Chart of Accounts",
    '' as "Parent External Ref Code",
    'No' as Default,
    _fivetran_synced as timestamp
from kustom_raw.sage_db_dbo.jcm_master__job
where _fivetran_active = TRUE
);


-- View: VW_FILTER_REVENUE_DATA
CREATE OR REPLACE VIEW REVENUE_MODEL.VW_FILTER_REVENUE_DATA AS
CREATE OR REPLACE VIEW KUSTOM_PREPARED.REVENUE_MODEL.vw_filter_revenue_data as (
select distinct job
from kustom_prepared.prepared_copy.vw_revenue_model_2
where type = 'Size' and job in (select distinct job from kustom_prepared.prepared_copy.vw_revenue_model_2 group by all having sum(incremental_work_billed)=0)
)


-- View: VW_GL_ACCOUNT_MASTER
CREATE OR REPLACE VIEW PREPARED_COPY.VW_GL_ACCOUNT_MASTER AS
create or replace view kustom_prepared.prepared_copy.vw_gl_account_master as (
    select account
        ,intercompany_status
        ,account_title
        ,control_account_type
        ,account_type
        ,control_account
        ,left(account,5) as division
        ,right(account,7) as gl_account
    from kustom_raw.sage_db_dbo.glm_master__account
    where _fivetran_deleted=FALSE and dbid='KUSTOMUS'
);


-- View: VW_GL_TRANSACTIONS
CREATE OR REPLACE VIEW PREPARED_COPY.VW_GL_TRANSACTIONS AS
create or replace view kustom_prepared.prepared_copy.vw_gl_transactions as (
select *
    ,left(account,5) as division
    ,right(account,7) as gl_account
from kustom_raw.sage_db_dbo.glt_current__transaction
where _fivetran_deleted = FALSE and dbid = 'KUSTOMUS'
);


-- View: VW_INTERNAL_PARTICIPANTS_PIVOT
CREATE OR REPLACE VIEW SAGE_DASH_COMPARISON.VW_INTERNAL_PARTICIPANTS_PIVOT AS
create or replace view kustom_prepared.sage_dash_comparison.vw_internal_participants_pivot as(
    with t1 as (
        select distinct job_id, participant_role, participantname
        from kustom_raw.dash.vw_internal_participants
    )
    select p.job_id, t2.job_number,
        "'Accounting'" as Accounting_Dash,
        "'Supervisor'" as Supervisor_Dash,
        "'General Manager'" as GM_Dash,
        "'Program Coordinator'" as Program_Coordinator_Dash,
        "'QualityControl'" as Quality_Control_Dash,
        "'Estimator'" as Estimator_Dash,
        "'Coordinator'" as Coordinator_Dash,
        "'Marketing'" as Marketing_Dash,
        "'Business Manager'" as Business_Manager_Dash,
        "'ReceivedBy'" as Received_By_Dash,
        "'RSS'" AS RSS_Dash,
        "'Remote Estimating'" AS REMOTE_ESTIMATOR_DASH,
        --operations manager and ops. manager can be combined
        case 
            when "'Operations Manager'" is null
            then "'Ops. Manager'"
            else "'Operations Manager'"
        end as Operations_Manager_Dash
    from t1
    pivot(max(participantname) for participant_role in (
        'Accounting',
        'Supervisor',
        'General Manager',
        'Program Coordinator',
        'Operations Manager',
        'QualityControl',
        'Estimator',
        'Coordinator',
        'Marketing',
        'Ops. Manager',
        'Business Manager',
        'ReceivedBy',
        'RSS',
        'Remote Estimating'
        )
    ) as p
    left join kustom_raw.dash.vw_job_id_number_lookup t2
    ON p.job_id = t2.job_id
);


-- View: VW_INVOICE_AMTS_PREPARED
CREATE OR REPLACE VIEW PREPARED_COPY.VW_INVOICE_AMTS_PREPARED AS
create or replace view vw_invoice_amts_prepared as (
    select *
    from kustom_raw.sage_db_dbo.vw_invoice_amts
);


-- View: VW_INVOICE_AMTS_PREPARED_V1
CREATE OR REPLACE VIEW PREPARED_COPY.VW_INVOICE_AMTS_PREPARED_V1 AS
create or replace view KUSTOM_PREPARED.PREPARED_COPY.VW_INVOICE_AMTS_PREPARED_V1(
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
    select *
    from KUSTOM_PREPARED.prepared_copy.vw_apex_invoice_amts_prepared

    union all

    select *
    from KUSTOM_PREPARED.prepared_copy.vw_invoice_amts_prepared
);


-- View: VW_INVOICE_META_PREPARED
CREATE OR REPLACE VIEW PREPARED_COPY.VW_INVOICE_META_PREPARED AS
create or replace view KUSTOM_PREPARED.PREPARED_COPY.VW_INVOICE_META_PREPARED as (
    select *
    from kustom_raw.sage_db_dbo.vw_invoice_meta
);


-- View: VW_INVOICE_META_PREPARED_V1
CREATE OR REPLACE VIEW PREPARED_COPY.VW_INVOICE_META_PREPARED_V1 AS
create or replace view KUSTOM_PREPARED.PREPARED_COPY.VW_INVOICE_META_PREPARED_V1(
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
    select *
    from KUSTOM_PREPARED.prepared_copy.VW_APEX_INVOICE_META_PREPARED

    union all

    select *
    from KUSTOM_PREPARED.prepared_copy.VW_INVOICE_META_PREPARED
);


-- View: VW_INVOICE_PMTS_PREPARED
CREATE OR REPLACE VIEW PREPARED_COPY.VW_INVOICE_PMTS_PREPARED AS
create or replace view vw_invoice_pmts_prepared as(
    select *
    from kustom_raw.sage_db_dbo.vw_invoice_pmts
);


-- View: VW_INVOICE_PMTS_PREPARED_V1
CREATE OR REPLACE VIEW PREPARED_COPY.VW_INVOICE_PMTS_PREPARED_V1 AS
create or replace view KUSTOM_PREPARED.PREPARED_COPY.VW_INVOICE_PMTS_PREPARED_V1(
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
    select *
    from KUSTOM_PREPARED.prepared_copy.vw_apex_invoice_pmts_prepared

    union all

    select *
    from KUSTOM_PREPARED.prepared_copy.vw_invoice_pmts_prepared
);


-- View: VW_JOB_CONVERSION_DASH
CREATE OR REPLACE VIEW SAGE_DASH_COMPARISON.VW_JOB_CONVERSION_DASH AS
create or replace view kustom_prepared.sage_dash_comparison.vw_job_conversion_dash as(
    -- create two nearly identical tables as CTEs that only have job id's with status = WIP or Clsoed and the Closed status has a majority complete date (from date table). can do in two
    with initial_table as (
        select
            t1.job_id,
            t1.associated_job_id,
            t1.type,
            t1.status,
            t1.close_reason,
            t2.job_number as job_number
        from kustom_raw.dash.vw_job_detail t1
        inner join kustom_prepared.sage_dash_comparison.vw_job_dates_pivot t2
        on t1.job_id = t2.job_id
        where (
            t1.status = 'Closed' and 
            t2.date_majority_complete IS NOT NULL and
            t1.close_reason in ('Completed – Paid in Full ', 'Completed – Paid in Full')
        ) or t1.status in ('Work in Progress','Invoice Pending','Pre-Production','Accounts Receivable','Waiting for Final Closure', 'Completed without Paperwork')
    ),
    associated_table as (
        select
            t3.job_id,
            t3.type,
            t3.status,
            t3.close_reason,
            t3.job_name,
            t4.job_number as job_number
        from kustom_raw.dash.vw_job_detail t3
        inner join kustom_prepared.sage_dash_comparison.vw_job_dates_pivot t4
        on t3.job_id = t4.job_id
        where (
            t3.status = 'Closed' and 
            t4.date_majority_complete IS NOT NULL and
            t3.close_reason in ('Completed – Paid in Full ', 'Completed – Paid in Full')
        ) or t3.status in ('Work in Progress','Invoice Pending','Pre-Production','Accounts Receivable','Waiting for Final Closure', 'Completed without Paperwork')
    ),
    joined_table as(
        select 
            t5.job_id as initial_job_id,
            t5.job_number as initial_Job_number,
            t5.type as initial_job_type,
            t5.status as inital_job_status,
            t5.close_reason as intial_job_close_reason,
            t6.job_id as associated_job_id,
            t6.job_number as associated_job_number,
            t6.type as associated_job_type,
            t6.status as associated_job_status,
            t6.close_reason as associated_job_close_reason,
            t6.job_name as associated_job_name
        from initial_table t5
        INNER JOIN associated_table t6
        ON t5.associated_job_id = t6.job_id
    )
        select * ,
        -- denominator for conversion (this only includes jobs we actually won) THis is not really the correct potential conversion flag
            case
                --when Initial_Job_type IN ('Emergency Services','Mold Remediation','ABT','Board-Up/Trade','Fire Rep','Fire Repairs')
                when Initial_Job_type IN ('Emergency Services')
                then 1
                when Initial_Job_type = 'Emergency Services' AND Associated_JOB_TYPE = 'Rebuild'
                then 1
                else 0
            end as potential_conversion_flag,
            -- numerator for conversion % based on Conversions email from LEo to Sam identifying the type to type pairs that mean rebuild conversion
            case 
                --when initial_job_type IN ('Emergency Services','Environmental Abatement') AND Associated_JOB_TYPE in ('Rebuild','Fire Rep','Structure Only')
                --then 1
                --WHEN initial_job_type = 'Rebuild' and associated_job_type IN ('Emergency Services','Mold Remediation','Environmental Abatement')
                --then 1
                --WHEN initial_job_type = 'Mold Remediation' and associated_job_type IN ('Rebuild','Emergency Services')
                --then 1
                --when initial_job_type = 'ABT' and associated_job_type = 'Rebuild'
                --then 1
                --when initial_job_type in ('Structural Cleaning','Board-Up/Trade') and associated_job_type = 'Rebuild'
                --then 1
                --when initial_job_type = 'Fire Repairs' and associated_job_type = 'Contents'
                when initial_job_type IN ('Emergency Services','Rebuild') AND Associated_JOB_TYPE in ('Emergency Services','Rebuild')
                then 1
                else 0
            end as rebuild_conversion
        from joined_table
    );


-- View: VW_JOB_DATA_ESTIMATED_COST_CREATION
CREATE OR REPLACE VIEW PREPARED_COPY.VW_JOB_DATA_ESTIMATED_COST_CREATION AS
create or replace view KUSTOM_PREPARED.PREPARED_COPY.VW_JOB_DATA_ESTIMATED_COST_CREATION(
	JOB,
	TOTAL_CONTRACT,
	SIZE,
	OLD_ESTCOST,
	GENERAL_ESTCOST,
	QWK_ESTCOST_NORM,
	QWK_ESTCOST_PREV,
	TYPE_NEW,
	ESTIMATED_COST_PERCENTAGE_OVERRIDE_NEW
) as
select distinct t1.job
    --,t1.job_type
    ,t1.total_contract
    --,t1.dbid
    ,t1.size
    --,t1.cost_account_prefix
    ,t2.cogs as old_estcost
    ,t3.estimated_cost_percentage as general_estcost
    ,t4.estimated_cost_percentage as QWK_estcost_norm
    ,t5.estimated_cost_percentage as QWK_estcost_prev
    ,right(left(t1.job,5),2) as type_new
    ,case 
        when t1.estimated_cost_override > 0 then estimated_cost_override
        when t1.dbid = 'SOUTHCOAST' and left(t1.job,2)<=23 then .4
        when t2.cogs is null then 
        case 
            when t3.estimated_cost_percentage is null then 
                case 
                    when t4.estimated_cost_percentage is null then t5.estimated_cost_percentage
                    when t1.daily_entry_control = 'Require Daily Entry' then t5.estimated_cost_percentage
                    else t4.estimated_cost_percentage
                end
            else t3.estimated_cost_percentage
        end    
        else t2.cogs
    end as estimated_cost_percentage_override_new
from kustom_raw.sage_db_dbo.vw_jobdata t1
left join kustom_raw.share_point.estimated_cost_mapping_division_in t2
on t1.cost_account_prefix = t2.division_ 
    and left(t1.job,2)<='23'
left join kustom_raw.share_point.estimated_cost_mapping_type_kustom t3
on t1.total_contract between t3.lower_limit and t3.upper_limit 
    and dbid IN ('KUSTOMUS','RAINIER')
    and left(t1.job,2)>='24'
    and right(left(t1.job,5),2)=t3.type_number
left join kustom_raw.share_point.qwk_mapping_estimated_cost_norm_in t4
on t1.dbid = 'SOUTHCOAST'  
    and right(left(t1.job,5),2)=t4.type_number 
    and t1.total_contract between t4.lower_limit and t4.upper_limit
left join kustom_raw.share_point.qwk_prevailing_estimated_cost_in t5
on t1.dbid = 'SOUTHCOAST'
    and t1.daily_entry_control = 'Require Daily Entry'
    and right(left(t1.job,5),2)=t5.type_number;


-- View: VW_JOB_DATA_HISTORY_V1
CREATE OR REPLACE VIEW SAGE_DASH_COMPARISON.VW_JOB_DATA_HISTORY_V1 AS
create or replace view KUSTOM_PREPARED.SAGE_DASH_COMPARISON.VW_JOB_DATA_HISTORY_V1(
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
    TPA_COMPANY_NAME,
	CUSTOMER_FULL_NAME,
	ESTIMATED_COST_PERCENTAGE_OVERRIDE_NEW,
	TOTAL_CONTRACT_PREV_WEEK,
	WORK_BILLED_PREV_WEEK,
	REPORT_STATUS_PREV_WEEK,
	ACTUAL_COMPLETE_DATE_PREV_WEEK,
	REVISED_COMP_DATE_PREV_WEEK,
	JOB_COMPLETE_PREV_WEEK,
	ESTIMTED_COST_OVERRIDE_PREV_WEEK,
	PERCENT_TO_COMPLETE_OVERRIDE_PREV_WEEK,
	JTD_COST_PREV_WEEK,
	JTD_WORK_BILLED_PREV_WEEK
) as (
    WITH job_data_vw as (
        select * from kustom_prepared.prepared_copy.vw_job_data_prepared
    ),
    job_data_history_vw as(
        select * from kustom_raw.sage_db_dbo.vw_jobdata_history    
    )
    select
        v1.*,
        v2.total_contract as total_contract_prev_week,
        v2.jtd_work_billed as work_billed_prev_week,
        v2.report_status as report_status_prev_week,
        v2.actual_complete_date as actual_complete_date_prev_week,
        v2.revised_comp_date as revised_comp_date_prev_week,
        v2.job_complete as job_complete_prev_week,
        v2.estimated_cost_override as estimted_cost_override_prev_week,
        v2.percent_to_complete_override as percent_to_complete_override_prev_week,
        v2.JTD_Cost as JTD_Cost_Prev_Week,
        v2.JTD_Work_Billed as JTD_Work_Billed_Prev_Week
    from job_data_vw v1
    left join job_data_history_vw v2
    on v1.job = v2.job and v1.dbid = v2.dbid
);


-- View: VW_JOB_DATA_HISTORY_V2
CREATE OR REPLACE VIEW SAGE_DASH_COMPARISON.VW_JOB_DATA_HISTORY_V2 AS
create or replace view KUSTOM_PREPARED.SAGE_DASH_COMPARISON.VW_JOB_DATA_HISTORY_V2(
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
    TPA_COMPANY_NAME,
	CUSTOMER_FULL_NAME,
	ESTIMATED_COST_PERCENTAGE_OVERRIDE_NEW,
	TOTAL_CONTRACT_PREV_WEEK,
	WORK_BILLED_PREV_WEEK,
	REPORT_STATUS_PREV_WEEK,
	ACTUAL_COMPLETE_DATE_PREV_WEEK,
	REVISED_COMP_DATE_PREV_WEEK,
	JOB_COMPLETE_PREV_WEEK,
	ESTIMTED_COST_OVERRIDE_PREV_WEEK,
	PERCENT_TO_COMPLETE_OVERRIDE_PREV_WEEK,
	JTD_COST_PREV_WEEK,
	JTD_WORK_BILLED_PREV_WEEK
) as (
SELECT * 
FROM KUSTOM_PREPARED.SAGE_DASH_COMPARISON.VW_APEX_JOB_DATA

UNION ALL

SELECT * 
FROM KUSTOM_PREPARED.SAGE_DASH_COMPARISON.VW_JOB_DATA_HISTORY_V1);


-- View: VW_JOB_DATA_PREPARED
CREATE OR REPLACE VIEW PREPARED_COPY.VW_JOB_DATA_PREPARED AS
create or replace view KUSTOM_PREPARED.PREPARED_COPY.VW_JOB_DATA_PREPARED(
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
    TPA_COMPANY_NAME,
	CUSTOMER_FULL_NAME,
	ESTIMATED_COST_PERCENTAGE_OVERRIDE_NEW
) as (
    select 
        t1.*,
        t2.name AS CUSTOMER_FULL_NAME,
        t3.estimated_cost_percentage_override_new
    from kustom_raw.sage_db_dbo.vw_jobdata t1
    LEFT JOIN KUSTOM_RAW.SAGE_DB_DBO.ARM_MASTER__CUSTOMER t2
    ON t1.ar_customer = t2.customer
    LEFT JOIN kustom_prepared.prepared_copy.vw_job_data_estimated_cost_creation t3
    on t1.job = t3.job
    --QUALIFY row_number() OVER (PARTITION BY t2.CUSTOMER ORDER BY to_timestamp(date(t2.DATE_STAMP)||' '||TIME(t2.TIME_STAMP)) desc, t2.date_established)=1
);


-- View: VW_JOB_DATES_PIVOT
CREATE OR REPLACE VIEW SAGE_DASH_COMPARISON.VW_JOB_DATES_PIVOT AS
create or replace view KUSTOM_PREPARED.SAGE_DASH_COMPARISON.VW_JOB_DATES_PIVOT(
	JOB_NUMBER,
	TYPE,
	JOB_ID,
	DATE_RECEIVED,
	DATE_CONTACTED,
	DATE_INSPECTED,
	DATE_ESTIMATE_SENT,
	DATE_WORK_AUTH,
	DATE_ESTIMATE_APPROVED,
	DATE_STARTED,
	DATE_MAJORITY_COMPLETE,
	DATE_OF_COS,
	DATE_PAID,
	DATE_CLOSED,
	DATE_INVENTORIED,
	DATE_TARGET_COMPLETION,
	INTO_PRODUCTION_DATE,
	DATE_INVOICED,
	DATE_OF_LOSS,
	TARGET_START_DATE,
	RECEIVED_TO_CONTACT,
	RECEIVED_TO_INSPECTED,
	ESTIMATE_REPAIR_LAG,
	PAST_START_DATE_FLAG,
	JOB_TYPE,
	PRODUCTION_CYCLE,
	INVOICE_LAG,
	COLLECTION_LAG,
	FULL_CYCLE,
	RECEIVED_TO_DATE_INSPECTED,
	RECEIVED_TO_DATE_ESTIMATE_APPROVED,
	STARTED_TO_MAJORITY_COMPLETE,
	RECEIVED_TO_STARTED,
    RECEIVED_TO_WORK_AUTH,
    WORK_AUTH_TO_ESTIMATE_APPROVED,
    estimate_approved_to_COS,
    estimate_approved_to_majority_complete,
    received_to_COS
) as(
with t1 as(
    select 
        distinct job_id, 
        date,
        Process_Point
    from kustom_raw.dash.vw_job_dates
), t2 as (
select job_id, 
    "'Date Received'" as Date_received,
    "'Date Contacted'" as Date_Contacted,
    "'Date Inspected'" as Date_Inspected,
    "'Date Estimate Sent'" as Date_Estimate_Sent,
    "'Date of Work Authorization'" as Date_Work_Auth,
    "'Date Estimate Approved'" as Date_Estimate_Approved,
    "'Date Started'" as Date_Started,
    "'Date of Majority Completion'" as Date_Majority_Complete,
    "'Date of COS'" as Date_of_COS,
    "'Date Paid'" as Date_Paid,
    "'Date Closed'" as Date_Closed,
    "'Date Inventoried'" as Date_Inventoried,
    "'Date Target Completion'" as Date_Target_Completion,
    "'Into Production Date'" as Into_Production_Date,
    "'Date Invoiced'" as Date_Invoiced,
    "'Date of Loss'" as Date_of_Loss,
    "'Target Start Date'" as Target_Start_Date
from t1
PIVOT(MIN(date) for process_point in (
    'Date Received',
    'Date Contacted',
    'Date Inspected',
    'Date Estimate Sent',
    'Date of Work Authorization',
    'Date Estimate Approved',
    'Date Started',
    'Date of Majority Completion',
    'Date of COS',
    'Date Paid',
    'Date Closed',
    'Date Inventoried',
    'Date Target Completion',
    'Into Production Date',
    'Date Invoiced',
    'Date of Loss',
    'Target Start Date'
)) as p)
select t4.job_number, t5.type, t2.*,
    datediff(day,date_received,date_contacted) as Received_to_Contact,
    datediff(day,date_received,date_inspected) as received_to_inspected,
    case
        when t5.type = 'Emergency Services'
        then datediff(day,Date_majority_complete,Date_Estimate_Sent)
        else datediff(day,Date_Received,Date_Estimate_Sent)
    end as Estimate_Repair_Lag,
    case 
        when target_start_Date < current_date()
        then 1
        else 0
    end as Past_Start_Date_Flag,
    right(left(t4.job_number,5),2) as job_type,
    datediff(day,date_started,date_Majority_complete) as production_cycle,
    datediff(day,date_majority_complete,date_invoiced) as invoice_lag,
    datediff(day,date_invoiced,date_closed) as collection_lag,
    datediff(day,date_received,date_majority_complete) as full_cycle,
    datediff(day,date_received,date_inspected) as received_to_date_inspected,
    datediff(day,date_received, Date_Estimate_Approved) as received_to_date_estimate_approved,
    datediff(day,date_started,date_majority_complete) as started_to_majority_complete,
    datediff(day,date_received,date_started) as received_to_started,
    datediff(day,date_received,date_work_auth) as received_to_work_auth,
    datediff(day,date_work_auth,date_estimate_approved) as work_auth_to_estimate_approved,
    datediff(day,date_of_COS,Date_Estimate_Approved) as estimate_approved_to_COS,
    datediff(day,date_majority_complete,date_estimate_approved) as estimate_approved_to_majority_complete,
    datediff(day,date_received,date_of_COS) as received_to_COS
from t2
left join kustom_raw.dash.vw_job_id_number_lookup t4
on t2.job_id = t4.job_id
left join kustom_raw.dash.vw_job_detail t5
on t2.job_id = t5.job_id
);


-- View: VW_JOB_DETAIL_LEADS
CREATE OR REPLACE VIEW SAGE_DASH_COMPARISON.VW_JOB_DETAIL_LEADS AS
create or replace view kustom_prepared.sage_dash_comparison.vw_job_detail_leads as (
    select t1.*
    from kustom_raw.dash.vw_job_detail t1
    LEFT JOIN kustom_raw.sage_db_dbo.vw_jobdata t2
    ON t1.job_number = t2.job
    where t2.job IS NULL
);


-- View: VW_JOB_DETAIL_PREPARED
CREATE OR REPLACE VIEW SAGE_DASH_COMPARISON.VW_JOB_DETAIL_PREPARED AS
create or replace view KUSTOM_PREPARED.SAGE_DASH_COMPARISON.VW_JOB_DETAIL_PREPARED(
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
	DASH_JOB_TYPE_NUMBER,
	TEMP_JOB_FLAG,
	ASSOCIATED_JOB_NUMBER,
	ASSOCIATED_JOB_TYPE,
	ASSOCIATED_JOB_NAME,
	REBUILD_CONVERSION,
	POTENTIAL_CONVERSION_FLAG,
	SAGE_STATUS_IN_DASH
) as 
select 
        t1.*
        ,case
            when try_to_double(regexp_replace(t1.job_number,'-')) is null then FALSE
            ELSE TRUE
        END AS Temp_Job_Flag
        ,t2.associated_job_number
        ,t2.associated_job_type
        ,t2.associated_job_name
        ,t2.rebuild_conversion
        ,case
            when t1.type = 'Emergency Services' and t1.job_number not like '-PAS%'
                THEN
                    case
                        WHEN t2.associated_job_type = 'Rebuild'
                        THEN 1
                        WHEN t2.associated_job_type IS NULL
                        THEN
                            case
                                when t1.status IN (
                                    'Work in Progress',
                                    'Invoice Pending',
                                    'Pre-Production',
                                    'Accounts Receivable',
                                    'Waiting for Final Closure', 
                                    'Completed without Paperwork'
                                ) then 1
                                when t1.status  = 'Closed'
                                then 
                                    case
                                        when t1.close_reason = 'Internal Error'
                                        then 0
                                        else 1
                                    end
                                else 0
                            end
                        else 0
                    end
                else 0
            end as potential_conversion_flag
        ,case
            when t1.status = 'Closed' and t4.total_job_cost = 0
            then 'Unsold'
            when  t1.status = 'Closed'
            then 'Closed'
            when t1.status in ('Unknown Status','Pending Sales')
            then 'Unstarted'
            else 'WIP'
        end as Sage_Status_in_Dash --replace group customization for RE report filtering
from kustom_raw.dash.vw_job_detail t1
left join (
    select * 
    from kustom_prepared.sage_dash_comparison.vw_job_conversion_dash 
    where rebuild_conversion = 1
) t2
on t1.job_id = t2.initial_job_id
left join kustom_prepared.sage_dash_comparison.vw_job_dates_pivot t3
on t1.job_id = t3.job_id
left join kustom_prepared.prepared_copy.vw_accounting_summary_dash_prepared t4
on t1.job_id = t4.job_id
;


-- View: VW_JOB_EXTERNAL_PARTICIPANTS_DASH_PREPARED
CREATE OR REPLACE VIEW PREPARED_COPY.VW_JOB_EXTERNAL_PARTICIPANTS_DASH_PREPARED AS
create or replace view KUSTOM_PREPARED.PREPARED_COPY.VW_JOB_EXTERNAL_PARTICIPANTS_DASH_PREPARED(
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
    select * 
    from kustom_raw.dash.vw_job_external_participants
);


-- View: VW_JOB_GL_COMPARISON_PREPARED
CREATE OR REPLACE VIEW PREPARED_COPY.VW_JOB_GL_COMPARISON_PREPARED AS
create or replace view KUSTOM_PREPARED.PREPARED_COPY.VW_JOB_GL_COMPARISON_PREPARED(
	DBID_JC,
	BATCH_JC,
	DIVSION_JC,
	ACCOUNT_JC,
	FULL_ACCOUNT_JC,
	MONTH_JC,
	YEAR_JC,
	JC_AMOUNT,
	DBID_GL,
	BATCH_GL,
	DIVSION_GL,
	ACCOUNT_GL,
	FULL_ACCOUNT_GL,
	MONTH_GL,
	YEAR_GL,
	GL_AMOUNT,
	VARIANCE
) as (
    with gl as (
        select dbid
            ,batch
            ,original_account as full_account
            ,division
            ,account
            ,month(accounting_date) as month
            ,year(accounting_date) as year
            ,-sum(balance) as gl_amount
        from kustom_prepared.prepared_copy.vw_job_gl_metrics_prepared
        group by all
    ),
    jc as (
        select dbid
            ,batch
            ,full_account
            ,division
            ,account
            ,month(accounting_date) as month
            ,year(accounting_date) as year
            ,sum(amount) as jc_amount
            ,'JC' as source
        from kustom_raw.sage_db_dbo.vw_jct_current__transaction
        group by all
        --union
        --select dbid
            --,batch
            --,full_account
            --,division
            --,account
            --,month(accounting_date) as month
            --,year(accounting_date) as year
            --,sum(amount) as jc_amount
            --,'AR' as source
        --from kustom_raw.sage_db_dbo.vw_stg_ar_comparison
        --where job is null
        --group by all
    )
    select 
        jc.dbid as dbid_jc
        ,jc.batch as batch_jc
        ,jc.division as divsion_jc
        ,jc.account as account_jc
        ,jc.full_account as full_account_jc
        ,jc.month as month_jc
        ,jc.year as year_jc
        ,sum(jc.jc_amount) as jc_amount
        ,gl.dbid as dbid_gl
        ,gl.batch as batch_gl
        ,gl.division as divsion_gl
        ,gl.account as account_gl
        ,gl.full_account as full_account_gl
        ,gl.month as month_gl
        ,gl.year as year_gl
        ,sum(gl.gl_amount) as gl_amount
        ,case 
            when sum(jc_amount) is null then 0 - sum(gl_amount)
            when sum(gl_amount) is null then sum(jc_amount) - 0
            else sum(jc_amount) - sum(gl_amount)
        end as variance
        ,
    from jc
    full join gl
    using(month,year,batch, dbid, full_account)
    group by all
);


-- View: VW_JOB_GL_METRICS_5055_ONLY
CREATE OR REPLACE VIEW PREPARED_COPY.VW_JOB_GL_METRICS_5055_ONLY AS
create or replace view kustom_prepared.prepared_copy.vw_job_gl_metrics_5055_only as (
select 
    t1.account as account,
    left(t1.account,5) as division_code,
    right(t1.account,7) as account_number,
    t1.accounting_date, 
    t1.run_number,
    t1.sequence_number, 
    t1.debit, 
    t1.credit,
    t1.reference_1,
    t1.reference_2, 
    t1.customer, 
    t1.batch,
    t1.cost_code, 
    t1.row_version,
    t1.job,
    t2.description as job_desc,
    t1.operator_stamp,
    t1.vendor,
    t1.application_of_origin as origin,
    t1.date_stamp,
    t1.transaction_desc,
    t1.transaction_notes,
    t3.transaction_date
from kustom_raw.sage_db_dbo.glt_current__transaction t1
left join (select * from kustom_raw.sage_db_dbo.jcm_master__JOB where _fivetran_active = TRUE and dbid='KUSTOMUS') t2
on t1.job = t2.job
left join (
    select 
        max(transaction_date) as transaction_date, 
        job, 
        batch 
    from kustom_raw.sage_db_dbo.jct_current__transaction
    group by job, batch) t3
on t1.job = t3.job and t1.batch = t3.batch
where (right(t1.account,7) = '5055.00' and t1.dbid = 'KUSTOMUS'));


-- View: VW_JOB_GL_METRICS_5055_RUNS
CREATE OR REPLACE VIEW PREPARED_COPY.VW_JOB_GL_METRICS_5055_RUNS AS
create or replace view kustom_prepared.prepared_copy.vw_job_gl_metrics_5055_runs as (
select 
    t1.account as account,
    left(t1.account,5) as division_code,
    right(t1.account,7) as account_number,
    t1.accounting_date, 
    t1.run_number,
    t1.sequence_number, 
    t1.debit, 
    t1.credit,
    t1.reference_1,
    t1.reference_2, 
    t1.customer, 
    t1.batch,
    t1.cost_code, 
    t1.row_version,
    t1.job,
    t2.description as job_desc,
    t1.operator_stamp,
    t1.vendor,
    t1.application_of_origin as origin,
    t1.date_stamp,
    t1.transaction_desc,
    t1.transaction_notes
from kustom_raw.sage_db_dbo.glt_current__transaction t1
left join (
    select * 
    from kustom_raw.sage_db_dbo.jcm_master__JOB 
    where _fivetran_active = TRUE and dbid='KUSTOMUS'
) t2
on t1.job = t2.job
where run_number in (select distinct run_number
    from kustom_raw.sage_db_dbo.glt_current__transaction
    where right(account,7)<>'5055.00' and dbid = 'KUSTOMUS'
) and t1.dbid = 'KUSTOMUS' and batch in (
    select distinct batch
    from kustom_raw.sage_db_dbo.glt_current__transaction
    where right(account,7) <> '5055.00' and dbid = 'KUSTOMUS') and
    right(account,7) <>'5055.00'
);


-- View: VW_JOB_GL_METRICS_PREPARED
CREATE OR REPLACE VIEW PREPARED_COPY.VW_JOB_GL_METRICS_PREPARED AS
create or replace view KUSTOM_PREPARED.PREPARED_COPY.VW_JOB_GL_METRICS_PREPARED as (
    select * 
    from kustom_raw.sage_db_dbo.vw_job_gl_metrics
);


-- View: VW_JTD_REVENUE_CALCULATION
CREATE OR REPLACE VIEW REVENUE_MODEL.VW_JTD_REVENUE_CALCULATION AS
create or replace view kustom_prepared.revenue_model.vw_jtd_revenue_calculation as 
(select t1.*
            ,case
                when little_rule_02_date <= accounting_date
                then 1
                else 0
            end as little_rule_02_flag
            ,case
                when little_rule_04_date <= accounting_date
                then 1
                else 0
            end as little_rule_04_flag
            ,case 
                when actual_complete_date <= accounting_date then 1
                when percent_complete_override>1 then 1
                when percent_complete_override>0 then percent_complete_override
                when little_rule_02_date <= accounting_date AND job_type_number in ('01','02','05','06','08','10') then 1
                when little_rule_04_date <= accounting_date AND div0(jtd_cost,estimated_cost) >= .85 then 1
                when jtd_cost >= estimated_cost then 1
                else div0(jtd_cost,estimated_cost)
            end*contract_value as jtd_revenue
            ,case 
                when actual_complete_date <= accounting_date then 1
                when percent_complete_override>1 then 1
                when percent_complete_override>0 then percent_complete_override
                when little_rule_02_date <= accounting_date AND job_type_number in ('01','02','05','06','08','10') then 1
                when little_rule_04_date <= accounting_date AND div0(jtd_cost,estimated_cost) >= .85 then 1
                when jtd_cost >= estimated_cost then 1
                else div0(jtd_cost,estimated_cost)
            end as percent_complete_override_with_little_rule
            ,ifnull(t2.running_total_work_billed,0) as jtd_work_billed
        from kustom_prepared.revenue_model.vw_combined_table t1
        left join kustom_prepared.revenue_model.vw_work_billed_aggregation t2
        on t1.job = t2.job and t1.accounting_date between t2.accounting_start_date and t2.accounting_end_date);


-- View: VW_LAST_PERIOD_CLOSED
CREATE OR REPLACE VIEW PREPARED_COPY.VW_LAST_PERIOD_CLOSED AS
create or replace view vw_last_period_closed as (
    select period_ending_date as last_period_closed_date
    from kustom_raw.sage_db_dbo.glm_master__account_prefix_a
    where dbid = 'KUSTOMUS'
)
;


-- View: VW_LAST_PERIOD_CLOSED_VW
CREATE OR REPLACE VIEW PREPARED_COPY.VW_LAST_PERIOD_CLOSED_VW AS
create or replace view KUSTOM_PREPARED.PREPARED_COPY.VW_LAST_PERIOD_CLOSED_VW(
	PERIOD_ENDING_DATE
) as (
    select last_day(dateadd(month,-1,last_date)) as last_date
from
    (select date(max(accounting_date)) as last_date
    from kustom_raw.sage_db_dbo.glt_current__transaction
    where right(account,7)='4400.00')
)
;


-- View: VW_PERCENT_COMPLETE_OVERRIDE
CREATE OR REPLACE VIEW REVENUE_MODEL.VW_PERCENT_COMPLETE_OVERRIDE AS
create or replace view kustom_prepared.revenue_model.vw_percent_complete_override as 
(select job
        , amount as percent_complete_override
        , transaction_date as transaction_start_date
        ,time_stamp
        , lead(
            dateadd(
                minute,-1,transaction_date
            )
            ,1,'9999-12-31 23:59:59.000  '
        ) over (
            partition by job order by transaction_date, date_stamp, time_stamp
        ) as transaction_end_date
        ,to_timestamp(date(date_stamp) || ' ' ||to_time(time_stamp)) as date_stamp
        ,operator_stamp
    from kustom_raw.sage_db_dbo.vw_jct_current__transaction
    where transaction_type = 'Percent complete');


-- View: VW_REVENUE_MODEL
CREATE OR REPLACE VIEW PREPARED_COPY.VW_REVENUE_MODEL AS
create or replace view kustom_prepared.prepared_copy.vw_revenue_model
as
select accounting_date
    ,account
    ,division
    ,invoice
    ,job
    ,dbid
    ,batch
    ,sum(amount) as amount
    ,'JC' as source
from kustom_raw.sage_db_dbo.vw_jct_current__transaction t1
where account in ('4100.00','4125.00')
group by all
union
select accounting_date
    ,account
    ,division
    ,invoice
    ,job
    ,dbid
    ,batch
    ,sum(amount) as amount
    ,'AR' as source
from kustom_raw.sage_db_dbo.vw_stg_ar_comparison
where account in ('4100.00','4125.00')
    and job is null
group by all;


-- View: VW_REVENUE_MODEL_2
CREATE OR REPLACE VIEW PREPARED_COPY.VW_REVENUE_MODEL_2 AS
create or replace view kustom_prepared.prepared_copy.vw_revenue_model_2 as

select distinct JOB,
	ACCOUNTING_DATE,
	COST_AMOUNT,
	CONTRACT_VALUE,
	ESTIMATED_COST,
	JTD_COST,
	ESTIMATED_COST_PERCENTAGE_OVERRIDE_NEW,
	TYPE,
	TIME_STAMP,
	PERCENT_COMPLETE_ACTUAL,
	PERCENT_COMPLETE_OVERRIDE,
	JOB_TYPE_NUMBER,
	REVISED_COMP_DATE,
	ACTUAL_COMPLETE_DATE,
	LITTLE_RULE_02_DATE,
	LITTLE_RULE_04_DATE,
	LITTLE_RULE_02_FLAG,
	LITTLE_RULE_04_FLAG,
	JTD_REVENUE,
	PERCENT_COMPLETE_OVERRIDE_WITH_LITTLE_RULE,
	JTD_WORK_BILLED
    ,jtd_revenue - lag(
        jtd_revenue,1,0
    ) over(
        partition by job order by accounting_date, time_stamp
    ) as incremental_jtd_revenue
    ,jtd_cost - lag(
        jtd_cost, 1,0
    ) over(
        partition by job order by accounting_date, time_stamp
    ) as incremental_jtd_cost
    , jtd_work_billed - lag(
        jtd_work_billed, 1, 0
    ) over (
        partition by job order by accounting_date, time_stamp
    ) as incremental_work_billed
    , row_number() 
        over(
            partition by job order by accounting_date, time_stamp
        ) as action_number
    ,CASE 
    WHEN --(contract_value IS NULL OR contract_value = 0) AND
         max_by(jtd_cost,accounting_date) over (partition by job) > (max_by(estimated_cost,accounting_date) over (partition by job) * 1.25)
         --AND MAX(CASE WHEN TYPE = 'Size' THEN 1 ELSE 0 END) OVER (PARTITION BY JOB) = 1
    THEN 1 
    ELSE 0 
    END AS COST_OVERRUN_FLAG,
    --  -- Calculate the latest accounting date for each job
    max(t1.ACCOUNTING_DATE) over (partition by t1.JOB) as LATEST_ACCOUNTING_DATE,
    -- Identify the most recent percent_complete_actual using first_value over descending order
    max_by(t1.percent_complete_override_with_little_rule,accounting_date) over (
        partition by t1.JOB
    ) as LATEST_PERCENT_COMPLETE_ACTUAL,
    -- Flag for "Jobs to complete": jobs over 65% complete (latest) and with no new costs in 60 days
    CASE 
        WHEN max_by(t1.percent_complete_override_with_little_rule,accounting_date) over (
                 partition by t1.JOB
             ) between .65 and .99
             AND max(t1.ACCOUNTING_DATE) over (partition by t1.JOB) < current_date() - 60
        THEN 1 
        ELSE 0 
    END as JOBS_TO_COMPLETE_FLAG,
    -- Flag for jobs that contain a type 'Size'
    MAX(CASE WHEN t1.TYPE = 'Size' THEN 1 ELSE 0 END) OVER (PARTITION BY t1.JOB) AS JOB_HAS_SIZE_FLAG,
 
       CASE 
        WHEN ROW_NUMBER() OVER (
                 PARTITION BY t1.JOB 
                 ORDER BY t1.ACCOUNTING_DATE DESC, t1.TIME_STAMP DESC
             ) = 1 
        THEN 1 
        ELSE 0 
    END AS Is_Latest_Flag
    ,date_stamp
    ,operator_stamp
from kustom_prepared.revenue_model.vw_jtd_revenue_calculation t1
where job not in (
    '22-04-99183',
    '20-04-99182',
    '23-02-02509',
    '23-07-70507',
    '23-99-12000',
    '23-07-02483',
    '24-04-32505',
    '23-03-15697',
    '23-02-33122',
    '20-04-99070',
    '23-04-33276',
    '23-07-70508',
    '23-07-70509',
    '23-07-11278',
    '24-02-33122',
    '24-02-33123',
    '24-04-14999',
    '24-04-33276',
    '24-04-99183',
    '24-07-32860',
    '24-07-81998',
    '23-04-09281',
    '24-02-09034',
    '23-07-11278',
    '24-99-12000',
    '24-02-81067',
    '23-02-36002'
)
and accounting_date<=current_date()
--where job = '24-02-31430'
--order by accounting_date
--JNS and VOID need to be removed
;


-- View: VW_TSCOST_PREPARED
CREATE OR REPLACE VIEW PREPARED_COPY.VW_TSCOST_PREPARED AS
create or replace view KUSTOM_PREPARED.PREPARED_COPY.VW_TSCOST_PREPARED as (
    select *
    from kustom_raw.timberscan_db_dbo_dbo.vw_tscost
);


-- View: VW_TS_COST_HISTORY_PREPARED
CREATE OR REPLACE VIEW PREPARED_COPY.VW_TS_COST_HISTORY_PREPARED AS
create or replace view vw_ts_cost_history_prepared as (
    select *
    from kustom_raw.timberscan_db_dbo_dbo.vw_tscost_history
);


-- View: VW_WORK_BILLED_AGGREGATION
CREATE OR REPLACE VIEW REVENUE_MODEL.VW_WORK_BILLED_AGGREGATION AS
create or replace view KUSTOM_PREPARED.REVENUE_MODEL.VW_WORK_BILLED_AGGREGATION as
(select job
    ,accounting_date as accounting_start_date
    ,time_stamp as time_stamp
    ,lead(
        dateadd(
            minute,-1,accounting_date
        )
        ,1, '9999-12-31 23:59:59.000  '
    ) over (
        partition by job order by accounting_date, date_stamp, time_stamp
    ) as accounting_end_date
    ,sum(amount) over (
        partition by job
        order by accounting_date, date_stamp,time_stamp
    ) as running_total_work_billed
    ,to_timestamp(date(date_stamp) || ' ' ||to_time(time_stamp)) as date_stamp
    ,operator_stamp
from kustom_raw.sage_db_dbo.vw_jct_current__transaction
where transaction_type = 'Work billed'
);

