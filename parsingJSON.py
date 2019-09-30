import requests
import json
import psycopg2
import datetime
from collections import namedtuple

class Cur(object):
    def __init__(self, Date, Value, CharCode, Id):
        self.Date = Date
        self.Value = Value
        self.CharCode = CharCode
        self.Id = Id

conn = psycopg2.connect(dbname='dbname', user='user',
                        password='password', host='host')

def now():
    return datetime.datetime.now().strftime("%Y-%m-%d")

def generateObj(CharCode, Value):
    Id = CharCode + now()
    c = Cur(now(), Value, CharCode, Id)
    return c

def parseJSON():
    url = "https://www.cbr-xml-daily.ru/daily_json.js"
    response = requests.request("GET", url)
    x = json.loads(response.text, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    for i in range(len(x.Valute)):
            c = generateObj(x.Valute[i][2], float(x.Valute[i][5]))
            insertToBD(c, conn)

def insertToBD(c, conn):
    try:
        cursor = conn.cursor()
        postgres_insert_query = """ INSERT INTO currency_rate ("Date","Value","CharCode","Id") VALUES (%s, %s, %s, %s)"""
        record_to_insert = (c.Date, c.Value, c.CharCode, c.Id)
        cursor.execute(postgres_insert_query, record_to_insert)
        conn.commit()
        print(record_to_insert, 'Suc')
    except Exception as e:
        conn.rollback()
        print(e, 'UnSuc')

parseJSON()
