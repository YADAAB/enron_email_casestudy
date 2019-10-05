# LOAD statement to load tab-separated *.log files(output from enron_analysis.py) into mysql database. MySQL native load utility to support paralle copy of files into database
LOAD DATA INFILE '/mysqlfiles/enron_processed.log' INTO TABLE enron.enron_mail FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INFILE '/mysqlfiles/enron_to_processed.log' INTO TABLE enron.enron_mail_to FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INFILE '/mysqlfiles/enron_cc_processed.log' INTO TABLE enron.enron_mail_cc FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INFILE '/mysqlfiles/enron_bcc_processed.log' INTO TABLE enron.enron_mail_bcc FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';
