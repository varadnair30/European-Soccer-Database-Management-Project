import pandas as pd
import mysql.connector
connection = mysql.connector.connect(
    host='',
    user='',
    password='',
    database=''
)
count=1
if (connection):
    print("connected")
cursor = connection.cursor()

data = pd.read_csv('Country.csv')

for index, row in data.iterrows():
    query = f"INSERT INTO country_1(id , name ) VALUES ({row.iloc[0]},'{row.iloc[1]}');"
    cursor.execute(query)
    print(query);
connection.commit()
print("Done")
cursor.close()
connection.close()