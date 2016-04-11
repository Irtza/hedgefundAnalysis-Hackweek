import pandas as pd
import os
import datetime 
import zipfile
import shutil
import numpy as np
from ftplib import FTP
import psycopg2
from sqlalchemy import create_engine

###########################################################
#       Dowload masterfiles from edgar ftp.
#
#       # IMPORTANT : Place index files in data/edgar/masterfiles before running
#
#       I was constrainted by low Bandwidth/download limit.    
#       Downloading last 6 years data . change date to 1993 should work fine
#       
#       please change the following Variable :
#       start_year = 2010 (default)
#
############################################################

# Utility Function
def filedownload(localfile, remfile):
    """
    This method downloads the remote file 'remfile' into the local file
    'localfile'
    """
    if os.path.isfile(localfile):
        print "file: " +localfile + " exists!"
        return
    file = open(localfile, 'wb')
    try:
        print "     Downloading file %s"%remfile
        ftp.retrbinary('RETR ' + remfile, file.write, 1024)
    except:
        print "     Unable to download file %s"%remfile
    file.close()
def savetoPostgres(df , table_name):
    '''
    Saves a DataFrame to a table in oquantdatabase PostgresSQL
    1st arg : DataFrame
    2nd arg : tablename in postgres .. will be created and overwritten
    
    Default: if exists = True
    '''
    engine = create_engine('postgresql://irtza:hedgefund@localhost:5432/oquantdatabase')
    try:
        #database table is also called bigdata
        pd.DataFrame.to_sql(df,table_name, engine,if_exists='replace')
        print "oquantdatabase table: "+table_name+": Over-written"

    except Exception ,e:
        print str(e)
        return False

    else:
        print "All the data has been BULK inserted to Postgresql: "
        return True
def maybedownload(start_year):
    ftp = FTP('ftp.sec.gov')
    ftp.login()
    ftp.cwd('/')

    #for all Quarterly files Download !
    current_year = datetime.date.today().year
    current_quarter = (datetime.date.today().month - 1) // 3 + 1

    #change starting data... Internet download LIMITED. assuming last 6 years 

    years = list(range(start_year, current_year))
    quarters = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
    history = [(y, q) for y in years for q in quarters]
    for i in range(1, current_quarter):
        history.append((current_year, 'QTR%d' % i))

    history.append((current_year,'QTR%d' % current_quarter))

    quarterly_files = ['edgar/full-index/%d/%s/master.zip' % (x[0], x[1]) for x in history]
    quarterly_files.sort()

    list_lf = []

    #Extract all the quarterly files
    for file in quarterly_files:
        lf = "edgar/masterfiles/"+file.split('/')[-3] + file.split('/')[-2] + file.split('/')[-1] 
        filedownload(lf,file)
        
        if os.path.isfile(lf):
            with zipfile.ZipFile(lf, "r") as z:
                z.extractall(z.filename.split('.')[0])        
        
        dirr = lf.split('.')[0]
        if os.path.isdir(dirr):
            shutil.move(dirr+'/master.idx', dirr +'.idx')
        
        #clean up empty folders recursively
        list_lf.append(lf.split('.')[0]+'.idx')
        
        os.removedirs(dirr)

    ftp.close()
    return list_lf

if __name__ == "__main__":

    list_lf = maybedownload(2010)
    df_list = []

    for file in list_lf:
        df = pd.read_csv(file,engine='c',sep='|',skiprows=4,header=0).dropna()
        
        #Concatenating only the interested form types
        #-----------------------------------------------------#
        #      Comment out or add form types
        #-----------------------------------------------------#
        df = pd.concat([
                df[df['Form Type'] == '13F-HR'],
                df[df['Form Type'] == '13F-HR/A'],
                #df[df['Form Type'] == 'S-1'] , 
                #df[df['Form Type'] == 'S-1/A']
            ],ignore_index=True,copy=False)
        
        df_list.append(df)

    bigdata = pd.concat(df_list ,ignore_index=True ,copy=False)
    bigdata.sort_values('CIK',inplace=True)

    #cleanup memory
    del df_list

    # Uncomment for SUMMARY OF BIGDATA
    #print len(bigdata['CIK']) , "# of Data points"
    #print len(bigdata['CIK'].unique()) , " # of Different Companies"
    #print len(bigdata['Company Name'].unique()) , " # of Different Company names"
    #print len(bigdata['Form Type'].unique()) , "     # of Form types"
    #print len(bigdata['Filename'].unique()) , "# of form submissions"
    #print "================================================="
    print "big data Summary : "
    print " ================= "
    print bigdata.describe()

    # Uncomment to save all data to postgres 
    #savetoPostgres(bigdata,"bigdata")


    #Efficient Vectorized String methods in latest pandas.

    #print "Heuristic1 assumes the company name contains some keywords.!"
    #print "Heuristic1 was made by frequent word analysis Top HedgeFund list published by OctaFinance.com"

    strfilter1 = "CAPITAL|ASSET MANAGEMENT"
    strfilter2 = "CAPITAL|ASSET MANAGEMENT|ADVISORS|ADVISERS|HOLDINGS|FINANCIAL|HEDGE|SECURITIES"

    #Filter Bigdata for HedgeFund Names
    hedgeprop1 = bigdata[bigdata['Company Name'].str.upper().str.contains(strfilter2)].sort_values('CIK')
    uniquehedgefundnames = hedgeprop1[['CIK','Company Name']].set_index(['CIK']).drop_duplicates()

    #clean up memory
    del bigdata

    savetoPostgres(uniquehedgefundnames,"hedgefund")
    # uncomment to download forms directly to pwd.. 

    #ftp.cwd('/')
    #ftp bulk Download get these urls
    #formURLs = hedgeprop1['Filename'].tolist()

    #for fu in formURLs:
    #    localfile = fu.split('/')
    #    cik = localfile[-2]
    #    localfile = "edgar/forms/"+localfile[-1]
    #    #print cik
    #    #print localfile
    #    filedownload(localfile,fu)


