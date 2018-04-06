import pandas as pd
import sys
from datetime import datetime
import random
import numpy as np

inFile = sys.argv[1]
inFile2 = sys.argv[2]
outFile = sys.argv[3]
# inFile = 'log.csv' #declaring the variable with relative path with the name of the  input file
# inFile2 = 'inactivity_period.txt'
# outFile = 'sessionization.txt'

waitfile = open(inFile2, 'r')
wait_time = waitfile.read().strip()
wait_time = int(wait_time)
waitfile.close()

print ('waiting time=', wait_time)


path = inFile


def main():

    i = 0
    sk = 10000

    ip_list = set([])

    ipn = [0]
    session_id = [0]
    last_acess = [0]
    close_time = [0]


    ip_dict = dict()
    session_dict = dict()

    ip_df = pd.DataFrame({'session_id': session_id, 'last_acess': last_acess, 'close_time':close_time}, index=ipn)
    print(ip_df)



    for df in pd.read_csv(path,sep=',', header = 0, usecols=[0,1,2,4,5,6], dtype = object, chunksize = 1):  #access the file row by row with a specified separator and no header,  chunksize-number of rows at once
        current_time = df['date'].astype(str).str[:]+' '+df['time'].astype(str).str[:]
        current_time = current_time.iloc[0]
        current_time = pd.to_datetime(current_time)
        print('current_time', current_time)
        #print('i', i)
        ip = df.ip.iloc[0]

        if i == 0:
            old_time = current_time

            session = sk
            sk = sk+1
            ip_old = ip
            close_time=10000
            ip_df.loc[ip] = [close_time, current_time, session]



        s_row = pd.DataFrame()

        s_row['ip'] = df.ip

        if ip not in ip_df.index:
            s_row['start_datetime'] = current_time
        if ip in ip_df.index:
            s_row['start_datetime'] = old_time
        s_row['end_datetime'] = current_time

        delta = current_time-old_time
        delta = delta.seconds



        ip_list.add(ip_old)


        if (ip not in ip_df.index):
            session = sk
            sk = sk+1
            close_time=10000
            ip_df.loc[ip] = [close_time, current_time, session]

        if (ip in ip_df.index):
            ses_last = ip_df.loc[ip].last_acess
            ses = ip_df.loc[ip].session_id

            dt = current_time-ses_last
            dt = dt.seconds
            if dt <= wait_time:
                session = ses
                close_time=10000
                ip_df.loc[ip] = [close_time, current_time, session]
            if dt > wait_time:
                session = sk
                sk = sk+1
                close_time = current_time
                #ip_df.loc[ip] = [close_time, current_time, session]


        for idx, row in ip_df.iterrows():
            if idx!=0:
            #idx=row['session']
                #idx=row['session_id']


                #idx=ip_df.index[i]
                print('idx',idx)
                sesz_last = ip_df['last_acess'].loc[idx]
                print('ses_last',sesz_last)
                #ses_last = ip_df.loc[idx].last_acess
                #print('ses',ses_last)
                dt = current_time-sesz_last
                dt = dt.seconds
                print('dt',dt)
                ct = ip_df.loc[idx].close_time
                if ct == 10000:
                    if dt < wait_time:
                        close_time = 10000
                        print('same!')
                    if dt >= wait_time:
                        close_time = current_time
                        ip_df['close_time'].loc[idx]=close_time
                        print('close t', ip_df['close_time'].loc[idx])
                        print('updated!')

        s_row['session'] = session

        s_row['delta'] = delta
        s_row['Web']=df.cik + ' '+df.accession+' '+df.extention
        s_row['close_time'] = close_time

        if i > 0:
            summary = pd.concat([summary, s_row], axis=0)
            old_s_row = s_row

        if i == 0:
            summary = s_row
            old_s_row = s_row

        i = i+1
        old_time = current_time
        ip_old = ip
        session_old = session
        print(ip_df)
    summary1=pd.DataFrame()
    for idx, row in summary.iterrows():
        ip = row['ip']
        #print('ipx',ip)
        #print('ip_c',ip_df.loc[ip].close_time)
        #row['close_time']=ip_df.loc[ip].close_time
        ctx= ip_df.loc[ip].close_time
        #stx=ip_df.loc[ip].last_acess
        stx=row['start_datetime']

        if ctx==10000:
            ctx=current_time
            print('check!')
        print('ctx',ctx)
        print('stx',stx)
        if stx>ctx:
            ctx=stx
            print('change!')

        row['close_time']=ctx

        #print('row',row)

        summary1 = summary1.append(row)#pd.concat([summary1, row], axis=1)
        #summary['close_time'][idx]= ctx
        #summary1=pd.concat([summary1, row], axis=0)

    print('summary', summary1)


    Npages = summary1.groupby('session').agg({'ip': ['min'], 'close_time':['min'], 'start_datetime': ['min'], 'end_datetime': ['max'], 'Web': ['count']})
    print(Npages.columns)
    Npages.columns = Npages.columns.droplevel(0)
    Npages.columns = ['ip', 'close', 'start', 'end','N']
    print(Npages)
    Npages = Npages.sort_values(['close','start'],ascending=[True, True])
    #Npages = Npages.sort_values('close',ascending=True)
    delta = Npages.end-Npages.start
    delta = delta.astype('timedelta64[s]')
    delta = delta.astype('int')+1
    Npages['delta'] = delta
    Npages = Npages[['ip', 'start', 'end', 'delta', 'N']]
    print('Npages with delta', Npages)


    Npages.to_csv(outFile, mode='w', sep=',',  index=False, header=False)


main()