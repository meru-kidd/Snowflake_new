use schema kustom_raw.sage_intacct;

select *
from project
where projectid = '25-04-35465';

select *
from gl_budget_item
where recordno = '6719';

select *
from gl_batch;

select sum(trx_amount),title,itemid,accounttype
from gl_detail t1
left join gl_account t2
on t1.accountno = t2.accountno
where projectid = '25-04-35465'
group by all;

select sum(trx_amount),itemid
from gl_entry
where projectid = '25-04-35465'
group by all;

select *
from glacctgrphierarchy;


select *
from pjestimate