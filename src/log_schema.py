"""
schema file to maintain all global variables
Any regex format changes or globabl variables can be added in here, and can be imported into main script(enron_analysis.py)
This format will help us to maiontain both code and metadata as two distinct files, easy to enhance and maintain
"""

import re
import datetime
log_schema = 'date_stamp	message_id	from	to	cc	bcc	reply_to	sender	subject	filename	mime_type	disposition_type'
split_pos = [4,5]
dt_pattern = re.compile('\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}')
mid_pattern = re.compile("message_id=<.*>")
from_pattern = re.compile("from=\"(.*?)\"")
to_pattern = re.compile("to=\"(.*?)\"")
cc_pattern = re.compile("cc=\"(.*?)\"")
bcc_pattern = re.compile("bcc=\"(.*?)\"")
rto_pattern = re.compile("reply_to=\"(.*?)\"")
sender_pattern = re.compile("sender=\"(.*?)\"")
subj_pattern = re.compile("subject=\"(.*?)\",")
fname_pattern = re.compile("filename=\"(.*?)\"")
mt_pattern = re.compile("mime_type=\"(.*?)\"")
disp_pattern = re.compile("disposition_type=\"(.*?)\"")
fw_except = open('enron_exception.log', "w+")
