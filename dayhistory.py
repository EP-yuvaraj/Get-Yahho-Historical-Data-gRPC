import math
import psycopg2.extras

import datetime

conn = psycopg2.connect('postgresql://postgres:yuviboxer@localhost/stock_db')
def get_data(x):
  if x=="all" or x==None:
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("select * from historical_stock ORDER BY stock_price_historical_id DESC LIMIT 5")
      #columns = list(cursor.description)
    records = cursor.fetchall()
  else:
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("select * from historical_stock where ticker_id=%s ORDER BY stock_price_historical_id DESC", [x, ])
      #columns = list(cursor.description)
    records = cursor.fetchone()
  print(records)
  print(len(records))
  return records

