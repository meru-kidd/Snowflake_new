create or replace view VW_INTERNAL_PARTICIPANTS_PIVOT(
	JOB_ID,
	JOB_NUMBER,
	ACCOUNTING_DASH,
	SUPERVISOR_DASH,
	GM_DASH,
	PROGRAM_COORDINATOR_DASH,
	QUALITY_CONTROL_DASH,
	ESTIMATOR_DASH,
	COORDINATOR_DASH,
	MARKETING_DASH,
	BUSINESS_MANAGER_DASH,
	RECEIVED_BY_DASH,
	RSS_DASH,
	REMOTE_ESTIMATOR_DASH,
	OPERATIONS_MANAGER_DASH
) as(
    with t1 as (
        select distinct job_id, participant_role, participantname
        from kustom_raw.dash.vw_internal_participants
    )
    select p.job_id, trim(t2.job_number),
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
    where job_number is not null
);