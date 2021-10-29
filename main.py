import requests
import pandas as pd
from pandas import json_normalize
from sqlalchemy import create_engine
from urllib.parse import quote_plus

''' Connection Parameters - Please Copy Requested Part From E-mail'''
DataBase = ''
User = ''
Password = quote_plus('')
Host = ''


'''Helping Functions'''
''' Dataframe To Mysql Table Function'''
def DataframeToSql(Table, TableName):
    Engine = create_engine("mysql://" + User + f":{Password}@" + Host + "/" + DataBase + '?charset=utf8')
    Conn = Engine.connect()
    Table.to_sql(name=TableName, con=Conn, if_exists='replace', index=False)

''' From Query To New Table Function '''
def QueryToTable(Query, TableName):
    Engine = create_engine("mysql://" + User + f":{Password}@" + Host + "/" + DataBase + '?charset=utf8')
    Conn = Engine.connect()
    Results = Conn.execute(Query)
    Data = pd.DataFrame(Results.fetchall())
    Data.columns = Results.keys()
    Data.to_sql(name=TableName, con=Conn, if_exists='replace', index=False)

''' From Query To Json File '''
def QueryToJson(Query, JsonFileName):
    Engine = create_engine("mysql://" + User + f":{Password}@" + Host + "/" + DataBase + '?charset=utf8')
    Conn = Engine.connect()
    Results = Conn.execute(Query)
    Data = pd.DataFrame(Results.fetchall())
    Data.columns = Results.keys()
    Data.to_json(JsonFileName +'.json', orient='records')


''' Tasks: '''
'''Create 4550 Tandom Users - Task 1'''
def ReadFromApi():
    Url = 'https://randomuser.me/api/?results=4500'
    global RandomUserData
    RandomUserData = json_normalize(requests.get(Url).json()['results'])

''' Create Gender Datasets - Task 2  '''
def Gender_Tables():
    DataframeToSql(RandomUserData[RandomUserData['gender'] == 'male'], 'Daniella_test_male')
    DataframeToSql(RandomUserData[RandomUserData['gender'] == 'female'], 'Daniella_test_female')

''' Create Age Sucsets - Task 3,4 '''
def Age_Tables():
    i, m = 0, 0
    RandomUserData['dob.age'] = RandomUserData['dob.age'].astype(int)
    while i < 100:
        DataframeToSql(RandomUserData[(RandomUserData['dob.age'] >= i) & (RandomUserData['dob.age'] <= (i + 10))],
                       'Daniella_test_' + str(m + 1))
        m += 1
        i += 10

''' Tasks 5,6,7 '''
def Queries():
    ''' Define Query Variables'''
    Top20Query = '(select  *  ' \
                 'from Daniella_test_female dtf ' \
                 'order by `registered.date` desc ' \
                 'limit 20) ' \
                 'union' \
                 '(select  *  ' \
                 'from Daniella_test_male dtf ' \
                 'order by `registered.date` desc ' \
                 'limit 20) '

    CombineTest20Test5 = 'select * from Daniella_test_20 union distinct select * from Daniella_test_5 '
    CombineTest20Test2 = 'select * from Daniella_test_20 union all select * from Daniella_test_2'

    ''' Run Functions'''
    QueryToTable(Top20Query, 'Daniella_test_20')
    QueryToJson(CombineTest20Test5, 'first')
    QueryToJson(CombineTest20Test2, 'second')



ReadFromApi()
Gender_Tables()
Age_Tables()
Queries()

