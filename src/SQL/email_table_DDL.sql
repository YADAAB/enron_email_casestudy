#MySQL server - Server version: 8.0.17 MySQL Community Server - GPL
#MySQL Workbench - 5.2.47

#create db schema
CREATE DATABASE enron;


#date,message_id,from,to,reply_to,sender,subject,filename,mime_type,disposition_type
#enron_mail, is the header table which has data at the message_id grain

create table enron.enron_mail (
email_date datetime,
message_id varchar(500),
fr_email  varchar(500),  PRIMARY KEY,
to_email  varchar(5000),
reply_to  varchar(100),
sender  varchar(100),
subj_email varchar(500),
file_name  varchar(500),
mime_type varchar(100),
disp_type  varchar(100),
insert_datetime varchar(100)
);


#date+'\t'+mid+'\t'+to_addr+'\t'+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#enron_mail_to, is the table that stores email recipients in 'to' section which has data at the message_id, to_email_addr grain
create table enron.enron_mail_to (
email_date datetime,
message_id varchar(500),
to_email_addr  varchar(5000),
insert_datetime varchar(100),
  CONSTRAINT PRIMARY KEY  (message_id, to_email_addr)

);

#date+'\t'+mid+'\t'+cc+'\t'+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#enron_mail_cc, is the table that stores email recipients in 'cc' section which has data at the message_id, cc_email_addr grain
create table enron.enron_mail_cc (
email_date datetime,
message_id varchar(255),
cc_email_addr  varchar(255),
insert_datetime varchar(100),
  CONSTRAINT PRIMARY KEY  (message_id, cc_email_addr)
);


#date+'\t'+mid+'\t'+bcc+'\t'+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#enron_mail_bcc, is the table that stores email recipients in 'bcc' section which has data at the message_id, bcc_email_addr grain
create table enron.enron_mail_bcc (
email_date datetime,
message_id varchar(255),
bcc_email_addr  varchar(255),
insert_datetime varchar(100),
  CONSTRAINT PRIMARY KEY  (message_id, bcc_email_addr)
);



SELECT @@global.secure_file_priv;
