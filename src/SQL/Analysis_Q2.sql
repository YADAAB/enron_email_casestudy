/*
- Let's label an email as "direct" if there is exactly one recipient and 
"broadcast" if it has multiple recipients. 
Identify the top five (5) people who received the largest number of direct emails, 
and the top five (5) people who sent the largest number of broadcast emails.

Direct Definition - [Uma] - Number of recipients is sum of recipients in  ‘to’ ,’cc’ and ‘bcc’. If there is one of them then it is direct else broadcast.  
Hence, if the sum of recipients across all 3 stages = 1, then the message is marked as direct else broadcast
 */

/*Query to get Top 5 direct email counts*/
with email_type
as
(
/*Email calculates counts of to, cc and bcc recipients and marks each message as direct/broadcast
'Direct' Definition - [Uma] - Number of recipients is sum of recipients in  ‘to’ ,’cc’ and ‘bcc’. If there is one of them then it is direct else broadcast.  
Hence, if the sum of recipients across all 3 stages = 1, then the message is marked as direct else broadcast
*/
select em.message_id, count(distinct emt.to_email_addr), count(distinct emc.cc_email_addr), count(distinct emb.bcc_email_addr),
case when (count(distinct emt.to_email_addr) + count(distinct emc.cc_email_addr) + count(distinct emb.bcc_email_addr)) = 1
then 'direct' else 'broadcast' end email_type
from enron.enron_mail em,
enron.enron_mail_to emt,
enron.enron_mail_cc emc,
enron.enron_mail_bcc emb
where em.message_id = emt.message_id
and emc.message_id = emt.message_id
and emc.message_id = emb.message_id
group by em.message_id),
emails_per_day as
(
/*
Union to get the count # of times a recipient is marked in emaisl at the grain of date, stage(to, cc and bcc) and email_type, direct/broadcast
Each part of union gets data for each stage respectively
length(to_email_addr) > 0, filter added to remove noise and empty messages with no corresponding recipients in those stages
*/
select 'to' email_dest,email_type, to_email_addr email_addr, cast(email_date as date) email_date, count(1) email_count
from enron.enron_mail_to,
email_type 
where email_type.message_id = enron_mail_to.message_id
and length(to_email_addr) > 0
group by to_email_addr, email_type, cast(email_date as date)
union
select 'cc' email_dest, email_type, cc_email_addr email_addr, cast(email_date as date) email_date, count(1) email_count
from enron.enron_mail_cc,
email_type 
where email_type.message_id = enron_mail_cc.message_id
and length(cc_email_addr) > 0
group by cc_email_addr, email_type, cast(email_date as date)
union
select 'bcc' email_dest, email_type, bcc_email_addr email_addr, cast(email_date as date) email_date, count(1) email_count
from enron.enron_mail_bcc,
email_type 
where email_type.message_id = enron_mail_bcc.message_id
and length(bcc_email_addr) > 0
group by bcc_email_addr, email_type, cast(email_date as date)),
total_email_count as 
(select email_addr, email_type, email_date, sum(email_count) total_count
from emails_per_day
group by email_addr, email_date, email_type
)
/*Query gets Top 5 direct email recipients*/
select * from total_email_count
where email_type = 'direct'
order by total_count desc, email_date  desc 
limit 5;



/*Query to get Top 5 broadcast email counts*/

with email_type
as
(
/*Email calculates counts of to, cc and bcc recipients and marks each message as direct/broadcast
Direct Definition - [Uma] - Number of recipients is sum of recipients in  ‘to’ ,’cc’ and ‘bcc’. If there is one of them then it is direct else broadcast.  
Hence, if the sum of recipients across all 3 stages = 1, then the message is marked as direct else broadcast
*/

select em.message_id, count(distinct emt.to_email_addr), count(distinct emc.cc_email_addr), count(distinct emb.bcc_email_addr),
case when (count(distinct emt.to_email_addr) + count(distinct emc.cc_email_addr) + count(distinct emb.bcc_email_addr)) = 1
then 'direct' else 'broadcast' end email_type
from enron.enron_mail em,
enron.enron_mail_to emt,
enron.enron_mail_cc emc,
enron.enron_mail_bcc emb
where em.message_id = emt.message_id
and emc.message_id = emt.message_id
and emc.message_id = emb.message_id
group by em.message_id),
emails_per_day as
(
/*
Union to get the count # of times a recipient is marked in emaisl at the grain of date, stage(to, cc and bcc) and email_type, direct/broadcast
Each part of union gets data for each stage respectively
length(to_email_addr) > 0, filter added to remove noise and empty messages with no corresponding recipients in those stages
*/

select 'to' email_dest,email_type, to_email_addr email_addr, cast(email_date as date) email_date, count(1) email_count
from enron.enron_mail_to,
email_type 
where email_type.message_id = enron_mail_to.message_id
and length(to_email_addr) > 0
group by to_email_addr, email_type, cast(email_date as date)
union
select 'cc' email_dest, email_type, cc_email_addr email_addr, cast(email_date as date) email_date, count(1) email_count
from enron.enron_mail_cc,
email_type 
where email_type.message_id = enron_mail_cc.message_id
and length(cc_email_addr) > 0
group by cc_email_addr, email_type, cast(email_date as date)
union
select 'bcc' email_dest, email_type, bcc_email_addr email_addr, cast(email_date as date) email_date, count(1) email_count
from enron.enron_mail_bcc,
email_type 
where email_type.message_id = enron_mail_bcc.message_id
and length(bcc_email_addr) > 0
group by bcc_email_addr, email_type, cast(email_date as date)),
total_email_count as 
(select email_addr, email_type, email_date, sum(email_count) total_count
from emails_per_day
group by email_addr, email_date, email_type
)
select * from total_email_count
where email_type = 'broadcast'
order by total_count desc, email_date  desc 
limit 5;

