#Q3 - Find the five (5) emails with the fastest response times. A response is defined as a message from one of the recipients to the original sender.
#The subject line of responses starts with either “RE:” or “FW:” (trimmed whitespace, case insensitive) followed by the original subject line. 
#The response time should be measured as the difference between when the original email was sent and when the response was sent.

with email_subj as 
(
/*Filters email subjects that are either RE:/FW:, they are needed to find fastest response according to the question */
select distinct trim(substring(subj_email,4)) subj_email_sub,subj_email  from enron.enron_mail
where lower(substring(subj_email, 1,3)) = 're:'
union all
select distinct trim(substring(subj_email,4)) subj_email_sub, subj_email from enron.enron_mail
where lower(substring(subj_email, 1,3)) = 'fw:'), 
email_group 
as 
(
/*this part of union gives subjects begin with RE:/FW:. 
email_group is used to classify data as response emails for downstream consumption*/
select '2' email_group , subj_email_sub, em.*
from enron.enron_mail em,
email_subj esub
where lower(esub.subj_email) = lower(em.subj_email)
union
#gets only sender emails from above data. email_group is used to classify data as sender emails for downstream consumption
select '1' email_group ,subj_email_sub, em.*
from enron.enron_mail em,
email_subj esub
where lower(esub.subj_email_sub) = lower(em.subj_email)),
valid_thread
as
(
/*
There have been few observation where the thread begins with RE:/FW: but the corpus is missing original emails
this cte eliminates such cases where we identify emails that only are replies (since we need here the fastest reply to 
the sender, and without sender details, data becomes invalid)
*/
select subj_email_sub, count(email_group)
from email_group
where length(subj_email_sub) > 1
group by subj_email_sub
having count(email_group) > 1
),
email_data
as 
(
/*
based on the valid threads identified from above CTE, this query ranks each email within the thread based on email_date,
row_number created here is helpful to identify the fastest email response when we validate time between 1st row numbered email(from sender) 
and 2nd email (response to the sender)
*/
select email_group.*, ROW_NUMBER() OVER (PARTITION BY subj_email_sub ORDER BY email_date) email_resp_rank
 from email_group
where subj_email_sub in (
select subj_email_sub from valid_thread
)
order by subj_email_sub)
select sender.subj_email_sub, sender.message_id, sender.email_date sender_date, resp.email_date resp_date , TIMESTAMPDIFF(MINUTE, sender.email_date, resp.email_date) total_timediff_minutes
from email_data sender,
email_data resp
where sender.subj_email_sub = resp.subj_email_sub
and sender.email_resp_rank = 1
and resp.email_resp_rank = 2
order by TIMESTAMPDIFF(MINUTE, resp.email_date, sender.email_date)
limit 5;


