
/*Query that gives TOP 20 for to, Order by count and when counts are same, 2nd level ordering is done on email_date column */
select 'to' email_dest, to_email_addr to_email_addr, cast(email_date as date) email_date, count(1) email_count
from enron.enron_mail_to
where length(to_email_addr) > 0
group by to_email_addr, cast(email_date as date)
order by email_count desc, email_date  desc 
limit 20;



/*Query that gives TOP 20 for cc. Order by count and 2nd level ordering is done on email_date column  */
select 'cc' email_dest, cc_email_addr cc_email_addr, cast(email_date as date) email_date, count(1) email_count
from enron.enron_mail_cc
where length(cc_email_addr) > 0
group by cc_email_addr, cast(email_date as date)
order by email_count desc, email_date  desc 
limit 20;


/*Query that gives TOP 20 for bcc. Order by count and 2nd level ordering is done on email_date column  */
select 'bcc' email_dest, bcc_email_addr bcc_email_addr, cast(email_date as date) email_date, count(1) email_count
from enron.enron_mail_bcc
where length(bcc_email_addr) > 0
group by bcc_email_addr, cast(email_date as date)
order by email_count desc, email_date  desc 
limit 20;




/*
Query that gives TOP 20 that includes all stages i.e. to, cc and bcc in the sum
*/
with emails_per_day as
(
select 'to' email_dest, to_email_addr email_addr, cast(email_date as date) email_date, count(1) email_count
from enron.enron_mail_to
where length(to_email_addr) > 0
group by to_email_addr, cast(email_date as date)
union
select 'cc' email_dest, cc_email_addr email_addr, cast(email_date as date) email_date, count(1) email_count
from enron.enron_mail_cc
where length(cc_email_addr) > 0
group by cc_email_addr, cast(email_date as date)
union
select 'bcc' email_dest, bcc_email_addr email_addr, cast(email_date as date) email_date, count(1) email_count
from enron.enron_mail_bcc
where length(bcc_email_addr) > 0
group by bcc_email_addr, cast(email_date as date)),
total_email_count as 
(select email_addr, email_date, sum(email_count) total_count
from emails_per_day
group by email_addr, email_date
)
select * from total_email_count
order by total_count desc, email_date  desc 
limit 20;



