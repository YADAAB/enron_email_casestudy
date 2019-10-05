#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re
import datetime
from log_schema import log_schema, split_pos, dt_pattern, mid_pattern, from_pattern, to_pattern, cc_pattern, bcc_pattern, rto_pattern, sender_pattern, subj_pattern, fname_pattern, mt_pattern, disp_pattern, fw_except
import json
import uuid

def gen_json_log(proc_dt, excep_block, excep_msg):
    """
     Function to generate json log message that can be published to local filesystem for future querying and references
    """
    try:
        log_entry={"processed_date":proc_dt,
        "exception_block":excep_block,
        "excpetion_message":excep_msg,
        "update_datetime":datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        print (str(log_entry))
        fw_except.write(str(log_entry))
        fw_except.write("\n")
        #return log_entry
    except Exception as e:
        print ("[Exception]: Failed writing logs to s3 - %s" %e)

def process_enron_data(line, fw):
    """
    Function that reads enron email corpus, cleans the data and generates a csv file. 
    This function outputs csv with following definition - 
    
    date,message_id,from,to,reply_to,sender,subject,filename,mime_type,disposition_type
    
    Csv is generated at header grain, it is copied into mysql table for further analysis. 
    Other csv outputs will contain granular data with cc and bcc information
    """    

    try:
        date = dt_pattern.search(line).group()
        mid = mid_pattern.search(line).group() #message_id=<530637.1075846150302.JavaMail.evans@thyme>
        if mid:
            mid = mid[12:].replace('>','')
        fr = from_pattern.search(line).group()
        if fr:
            fr= fr[6:].replace('"','')
        to = to_pattern.search(line).group()
        if to:
            to= to[4:].replace('"','')
        rto = rto_pattern.search(line).group()
        if rto:
            rto = rto[10:].replace('"','')
        sender = sender_pattern.search(line).group()
        if sender:
            sender = sender[8:].replace('"','')
        subj = subj_pattern.search(line).group()
        if subj:
            subj = subj[9:].replace('"','').replace('\t', ' ')
        fname = fname_pattern.search(line).group()
        if fname:
            fname = fname[10:].replace('"','')
        mt = mt_pattern.search(line).group()
        if mt:
            mt = mt[11:].replace('"','')
        disp = disp_pattern.search(line).group()
        if disp:
            disp = disp[18:].replace('"','')
        #log_line = date+','+mid+','+fr+','+to+','+rto+','+sender+','+subj+','+fname+','+mt+','+disp
        log_line = date+'\t'+mid+'\t'+fr+'\t'+to+'\t'+rto+'\t'+sender+'\t'+subj+'\t'+fname+'\t'+mt+'\t'+disp+'\t'+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fw.write(log_line)
        fw.write("\n")        
    except Exception as e:
        gen_json_log(datetime.datetime.now().strftime('%Y-%m-%d'), 'email_header', str(e))
        print ('exception ', str(e))


def process_to_data(line, fw):
    """
    Function that reads enron email corpus, and generates recipients(to=) information. This is 
    processed as seperate function to accomodate the grain differences in the parent email 
    dataset where there could multiple unique recipients in the email
    
    This function outputs csv with following definition - 
    date,message_id,from,to,subject
    
    Csv is generated at 'to' grain(1-level below parent email grain), it is copied into mysql table 
    for further analysis. Other csv outputs will contain granular data with bcc information
    """        
    try:
        date = dt_pattern.search(line).group()
        mid = mid_pattern.search(line).group() #message_id=<530637.1075846150302.JavaMail.evans@thyme>
        if mid:
            mid = mid[12:].replace('>','')
        fr = from_pattern.search(line).group()
        if fr:
            fr= fr[6:].replace('"','')
        to = to_pattern.search(line).group()
        if to:
            to= to[4:].replace('"','')
        subj = subj_pattern.search(line).group()
        if subj:
            subj = subj[9:].replace('"','')
        to = to_pattern.search(line).group()
        if to:
            to= to[4:].replace('"','')
        if len(to.split(',')) > 1:
            for to_addr in to.split(','):
                #log_line = date+'\t'+mid+'\t'+fr+'\t'+subj+'\t'+to_addr
                log_line = date+'\t'+mid+'\t'+to_addr+'\t'+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                fw.write(log_line)
                fw.write("\n")                    
        else:
            log_line = date+'\t'+mid+'\t'+to+'\t'+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            fw.write(log_line)
            fw.write("\n")
    except Exception as e:
        gen_json_log(datetime.datetime.now().strftime('%Y-%m-%d'), 'email_to', str(e))
        print ('exception at cc processing ', str(e))

def process_cc_data(line, fw):
    """
    Function that reads enron email corpus, and generates cc information. This is processed as seperate function to accomodate 
    the grain differences in the parent email dataset 
    This function outputs csv with following definition - 
    
    date,message_id,from,to,cc,subject
    
    Csv is generated at cc grain(1-level below parent email grain), it is copied into mysql table for further analysis. Other 
    csv outputs will contain granular data with bcc information
    """        
    try:
        date = dt_pattern.search(line).group()
        mid = mid_pattern.search(line).group() #message_id=<530637.1075846150302.JavaMail.evans@thyme>
        if mid:
            mid = mid[12:].replace('>','')
        fr = from_pattern.search(line).group()
        if fr:
            fr= fr[6:].replace('"','')
        to = to_pattern.search(line).group()
        if to:
            to= to[4:].replace('"','')
        subj = subj_pattern.search(line).group()
        if subj:
            subj = subj[9:].replace('"','')
        cc = cc_pattern.search(line).group()
        if cc:
            cc = cc[4:].replace('"','')                
        if len(cc.split(',')) > 1:
            for cc_addr in cc.split(','):
                log_line = date+'\t'+mid+'\t'+cc_addr+'\t'+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                fw.write(log_line)
                fw.write("\n")
        else:
            log_line = date+'\t'+mid+'\t'+cc+'\t'+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            fw.write(log_line)
            fw.write("\n")                                    
    except Exception as e:
        gen_json_log(datetime.datetime.now().strftime('%Y-%m-%d'), 'email_cc', str(e))
        print ('exception at cc processing ', str(e))


def process_bcc_data(line, fw):
    """
    Function that reads enron email corpus, and generates bcc information. This is processed as seperate function to accomodate 
    the grain differences with the parent email dataset and cc dataset
    This function outputs csv with following definition - 
    
    date,message_id,from,to,bcc,subject
    
    Csv is generated at bcc grain(1-level below parent email grain), it is copied into mysql table for further analysis. Other 
    csv outputs will contain granular data with cc information
    """        
    try:
        date = dt_pattern.search(line).group()
        mid = mid_pattern.search(line).group() #message_id=<530637.1075846150302.JavaMail.evans@thyme>
        if mid:
            mid = mid[12:].replace('>','')
        fr = from_pattern.search(line).group()
        if fr:
            fr= fr[6:].replace('"','')
        to = to_pattern.search(line).group()
        if to:
            to= to[4:].replace('"','')
        subj = subj_pattern.search(line).group()
        if subj:
            subj = subj[9:].replace('"','')
        bcc = bcc_pattern.search(line).group()
        if bcc:
            bcc = bcc[5:].replace('"','')                
        if len(bcc.split(',')) > 1:
            for bcc_addr in bcc.split(','):
                log_line = date+'\t'+mid+'\t'+bcc_addr+'\t'+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                fw.write(log_line)  
                fw.write("\n")                                      
        else:
            log_line = date+'\t'+mid+'\t'+bcc+'\t'+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            fw.write(log_line)  
            fw.write("\n")                                      
    except Exception as e:
        gen_json_log(datetime.datetime.now().strftime('%Y-%m-%d'), 'email_bcc', str(e))
        print ('exception at bcc processing ', str(e))


#date_stamp,message_id,from,to,reply_to,sender,subject,filename,mime_type,disposition_type

if __name__ == "__main__":
    print ('Hello, World! ')
    fo = open("enron_emails.log", "r")
    fw_enron = open("enron_processed.log", "w+")
    fw_to = open("enron_to_processed.log", "w+")
    fw_cc = open("enron_cc_processed.log", "w+")
    fw_bcc = open("enron_bcc_processed.log", "w+")
    print ('procesing enron parent grain email data...')
    for line in fo:
        process_enron_data(line, fw_enron)
        process_to_data(line, fw_to)
        #print ('procesing enron email cc data...')
        process_cc_data(line, fw_cc)
        #print ('procesing enron email bcc data...')
        process_bcc_data(line, fw_bcc)
    fo.close()
    fw_enron.close()
    fw_to.close()
    fw_cc.close()
    fw_bcc.close()
    fw_except.close()