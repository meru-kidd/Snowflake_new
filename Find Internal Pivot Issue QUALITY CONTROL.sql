use schema kustom_prepared.sage_dash_comparison;

select job_number, count(*)
from vw_internal_participants_pivot
group by all
having count(*) > 1;

select *
from vw_internal_participants_pivot
where job_id in ('4804131','4915380');