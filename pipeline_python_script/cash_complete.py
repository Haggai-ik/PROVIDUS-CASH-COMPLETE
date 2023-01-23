import pandas as pd
import numpy as np
import pickle
import os
import sys
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from math import sqrt
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.model_selection import cross_val_score
from sklearn.pipeline import make_pipeline
from sklearn.metrics import mean_absolute_error
from sklearn.compose import ColumnTransformer
from pandasql import sqldf
import pyodbc
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
pd.options.mode.chained_assignment = None
import  mysql.connector
pd.options.display.float_format = '{:.5f}'.format
pysqldf = lambda q: sqldf(q, globals())
saved_model_pk=pickle.load(open (r'C:\Providus_cashmgmt_prd_script\providus_credit','rb'))
saved_model=pickle.load(open (r'C:\Providus_cashmgmt_prd_script\providus_debit','rb'))
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import date,timedelta,datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from prefect import Flow


def get_run_date():
    base = datetime.today()- timedelta(3)
    ojo_oni = [base.strftime("%Y-%m-%d")]
    return ojo_oni

def read_withdrawer(run_dt):
    try:
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};\
                                SERVER=CASH-MGT-SV01;DATABASE=CashManagement-2;UID=cashcom_user;PWD=cash@312;')
        cnxn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1')
        cnxn.setencoding('latin1')
        cursor = cnxn.cursor()
        mySql_select_Query = r"""select  * from  Analytics_cash_MGT_withdrawer_B
                              where REFERENCE_DATE = """+"'"+ run_dt +"'"+""" """
        result = cursor.execute(mySql_select_Query)
        queryFetch = cursor.fetchall()
        queryResults = pd.DataFrame.from_records(queryFetch, columns = [i[0] for i in cursor.description])
        queryResults=queryResults.apply(pd.to_numeric, errors  = 'ignore', downcast = 'float')
        print('e don work')
    except mysql.connector.Error as error:
        #print(error)
        print("Failed to connect: You go need check am".format(error))
    return queryResults


def read_deposit(run_dt):
    try:
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};\
                              SERVER=CASH-MGT-SV01;DATABASE=CashManagement-2;UID=cashcom_user;PWD=cash@312;')
        cnxn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1')
        cnxn.setencoding('latin1')
        cursor = cnxn.cursor()
        mySql_select_Query = r"""select  * from  Analytics_cash_MGT_Deposit_B
                              where REFERENCE_DATE = """+"'"+ run_dt +"'"+""" """ 
        result = cursor.execute(mySql_select_Query)
        queryFetch = cursor.fetchall()
        queryResults = pd.DataFrame.from_records(queryFetch, columns = [i[0] for i in cursor.description])
        queryResults=queryResults.apply(pd.to_numeric, errors  = 'ignore', downcast = 'float')
        print('e don work')
    except mysql.connector.Error as error:
        #print(error)
        print("Failed to connect: You go need check am".format(error))
    return queryResults



def read_data():
    datein = get_run_date()
    for run_dt in datein:
        print('processing date is ' +(run_dt))
        df_w=read_withdrawer(run_dt) 
        df_d=read_deposit(run_dt)   
    return df_w,df_d

def clean_pipeline_credit(df):
    print('preprocessing done for credit')
    dff=df[['CREDIT_1_Day_Back','CREDIT_2_Day_Back', 'CREDIT_3_Day_Back',
       'CREDIT_4_Day_Back', 'CREDIT_5_Day_Back', 'CREDIT_6_Day_Back',
       'CREDIT_7_Day_Back', 'CREDIT_8_Day_Back', 'CREDIT_9_Day_Back',
       'CREDIT_10_Day_Back', 'CREDIT_11_Day_Back', 'CREDIT_12_Day_Back',
       'CREDIT_13_Day_Back', 'CREDIT_14_Day_Back', 'MAX_CREDIT_14_Day_Back',
       'Min_CREDIT_14_Day_Back', 'Sum_CREDIT_14_Day_Back',
       'Avg_CREDIT_14_Day_Back','NEXT_Number_1',
       'NEXT_Number_2', 'NEXT_Number_3', 'Previous_Number_1',
       'Previous_Number_2', 'Previous_Number_3','PREBUBHOL',
       'POSTBUBHOL','Target_Credit']].copy()
    return dff


def clean_pipeline_debit(df1):
    print('preprocessing done for credit')
    dff1=df1[['DEBIT_1_Day_Back',
       'DEBIT_2_Day_Back', 'DEBIT_3_Day_Back', 'DEBIT_4_Day_Back',
       'DEBIT_5_Day_Back', 'DEBIT_6_Day_Back', 'DEBIT_7_Day_Back',
       'DEBIT_8_Day_Back', 'DEBIT_9_Day_Back', 'DEBIT_10_Day_Back',
       'DEBIT_11_Day_Back', 'DEBIT_12_Day_Back', 'DEBIT_13_Day_Back',
       'DEBIT_14_Day_Back', 'MAX_DEBIT_14_Day_Back', 'Min_DEBIT_14_Day_Back',
       'Sum_DEBIT_14_Day_Back', 'Avg_DEBIT_14_Day_Back',  'NEXT_Number_1',
       'NEXT_Number_2', 'NEXT_Number_3', 'Previous_Number_1',
       'Previous_Number_2', 'Previous_Number_3','PREBUBHOL',
       'POSTBUBHOL','Target_Debit']].copy()
    return dff1

def make_predictions(dc_w,dc_d):
    c_predictions=saved_model_pk.predict(dc_w)
    d_predictions=saved_model.predict(dc_d)
    print('making prediction')
    return c_predictions,d_predictions


def joining_predictions(c_predictions,d_predictions,df_d,df_w):
    DEPOSIT_PREDICTION_TABLE= pd.DataFrame({'REFERENCE_DATE': df_d.REFERENCE_DATE,
                               'FORECAST_DATE': df_d.FORCAST_DATE,
                               'BRANCH_CODE':df_d.Branch_code,
                               'TRAN_CRNCY_CODE':df_d.tran_crncy_code,
                               'TARGET_CREDIT':df_d.Target_Credit,
                               'DEP_FORECAST':c_predictions,
                               'DAY_MONTH':df_d.DAY_MONTH,
                               'MONTH':df_d.Month,
                               'PREPUBHOL':df_d.PREBUBHOL,
                               'POSTPUBHOL':df_d.POSTBUBHOL})
    WITHDRAWAL_PREDICTION_TABLE= pd.DataFrame({'REFERENCE_DATE': df_w.REFERENCE_DATE,
                               'FORECAST_DATE': df_w.FORCAST_DATE,
                               'BRANCH_CODE':df_w.Branch_code,
                               'TRAN_CRNCY_CODE':df_w.tran_crncy_code,
                               'TARGET_DEBIT':df_w.Target_Debit,
                               'WITH_FORECAST':d_predictions,
                               'DAY_MONTH':df_w.DAY_MONTH,
                               'MONTH':df_w.Month,
                               'PREPUBHOL':df_w.PREBUBHOL,
                               'POSTPUBHOL':df_w.POSTBUBHOL})
    DEPOSIT_PREDICTION_TABLE['DEP_FORECAST']=np.where((DEPOSIT_PREDICTION_TABLE['TARGET_CREDIT'] ==0) & (DEPOSIT_PREDICTION_TABLE['DEP_FORECAST'] < 0) , 0, DEPOSIT_PREDICTION_TABLE['DEP_FORECAST'])
    WITHDRAWAL_PREDICTION_TABLE['WITH_FORECAST']=np.where((WITHDRAWAL_PREDICTION_TABLE['TARGET_DEBIT'] ==0) & (WITHDRAWAL_PREDICTION_TABLE['WITH_FORECAST'] < 0) , 0, WITHDRAWAL_PREDICTION_TABLE['WITH_FORECAST'])
    DEPOSIT_PREDICTION_TABLE['TRAN_CRNCY_CODE']=DEPOSIT_PREDICTION_TABLE['TRAN_CRNCY_CODE'].astype(str)
    WITHDRAWAL_PREDICTION_TABLE['TRAN_CRNCY_CODE']=WITHDRAWAL_PREDICTION_TABLE['TRAN_CRNCY_CODE'].astype(str)
    DEPOSIT_PREDICTION_TABLE['BRANCH_CODE']=DEPOSIT_PREDICTION_TABLE['BRANCH_CODE'].astype(str)
    WITHDRAWAL_PREDICTION_TABLE['BRANCH_CODE']=WITHDRAWAL_PREDICTION_TABLE['BRANCH_CODE'].astype(str)
    print('joining_predictions.')
    return DEPOSIT_PREDICTION_TABLE,WITHDRAWAL_PREDICTION_TABLE

def deposit_to_db(DEPOSIT_PREDICTION_TABLE):
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=CASH-MGT-SV01;DATABASE=CashManagement-2;UID=cashcom_user;PWD=cash@312;')
    cnxn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1')
    cnxn.setencoding('latin1')
    cursor = cnxn.cursor()
    # Insert Dataframe into SQL Server:
    for index, row in DEPOSIT_PREDICTION_TABLE.iterrows():
         cursor.execute("INSERT INTO[CASH_MGMT].[DEPOSIT_PREDICTION_TABLE](REFERENCE_DATE,FORECAST_DATE,BRANCH_CODE,TRAN_CRNCY_CODE,TARGET_CREDIT,DEP_FORECAST,DAY_MONTH,MONTH,PREPUBHOL,POSTPUBHOL) \
         values(?,?,?,?,?,?,?,?,?,?)", row.REFERENCE_DATE,row.FORECAST_DATE,row.BRANCH_CODE,row.TRAN_CRNCY_CODE,row.TARGET_CREDIT,row.DEP_FORECAST,row.DAY_MONTH,row.MONTH,row.PREPUBHOL,row.POSTPUBHOL)
    cnxn.commit()
    cursor.close()
    print('Deposit table inserted')


def withdrawal_to_db(WITHDRAWAL_PREDICTION_TABLE):
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=CASH-MGT-SV01;DATABASE=CashManagement-2;UID=cashcom_user;PWD=cash@312;')
    cnxn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1')
    cnxn.setencoding('latin1')
    cursor = cnxn.cursor()
    # Insert Dataframe into SQL Server:
    for index, row in WITHDRAWAL_PREDICTION_TABLE.iterrows():
         cursor.execute("INSERT INTO [CASH_MGMT].[WITHDRAWAL_PREDICTION_TABLE](REFERENCE_DATE,FORECAST_DATE,BRANCH_CODE,TRAN_CRNCY_CODE,TARGET_DEBIT,WITH_FORECAST,DAY_MONTH,MONTH,PREPUBHOL,POSTPUBHOL) \
         values(?,?,?,?,?,?,?,?,?,?)", row.REFERENCE_DATE,row.FORECAST_DATE,row.BRANCH_CODE,row.TRAN_CRNCY_CODE,row.TARGET_DEBIT,row.WITH_FORECAST,row.DAY_MONTH,row.MONTH,row.PREPUBHOL,row.POSTPUBHOL)
    cnxn.commit()
    cursor.close()
    print('withdrawal table inserted')
    

def main():
    df_w,df_d=read_data()
    dc_w=clean_pipeline_credit(df_d)
    dc_d=clean_pipeline_debit(df_w)
    c_predictions,d_predictions=make_predictions(dc_w,dc_d)
    DEPOSIT_PREDICTION_TABLE,WITHDRAWAL_PREDICTION_TABLE=joining_predictions(c_predictions,d_predictions,df_d,df_w)
    deposit_to_db(DEPOSIT_PREDICTION_TABLE)
    withdrawal_to_db(WITHDRAWAL_PREDICTION_TABLE)

scheduler = BlockingScheduler()
scheduler.add_job(main, 'cron', start_date='01-11-2023',hour=7,minute=50)
scheduler.start()

if __name__=="__main__":
    main()