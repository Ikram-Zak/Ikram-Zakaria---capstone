import numpy as np
import requests
from pprint import pprint
import pandas as pd
import numpy as np
from google.colab import files
from dotenv import load_dotenv
import os
load_dotenv()
api = os.getenv('api_key_aviation')
password = os.getenv('sql_password')
user = os.getenv('sql_user')
my_host = os.getenv('host')
import psycopg2 as psql


url = f'http://api.aviationstack.com/v1/flights?access_key={api}'
response = requests.get(url)
flight = response.json()



flight_data = flight['data']
flight_dates = []
flight_statuses = []
flight_number = []
airline_names = []
departure_airports = []
arrival_airports = []

for flight_details in flight_data:
    flight_dates.append(flight_details['flight_date'])
    flight_statuses.append(flight_details['flight_status'])
    flight_number.append(flight_details['flight']['number'])
    airline_names.append(flight_details['airline']['name'])
    departure_airports.append(flight_details['departure']['airport'])
    arrival_airports.append(flight_details['arrival']['airport'])

df_flight_details = pd.DataFrame({
    'Flight Date': flight_dates,
    'Flight Status': flight_statuses,
    'Airline Name': airline_names,
    'Flight Number': flight_number,
    'Departure Airport': departure_airports,
    'Arrival Airport': arrival_airports
})



conn = psql.connect(database = "pagila",
      user = user,
      host = my_host,
      password = password,
      port = 5432
)



cur = conn.cursor()
sql = f"""
    INSERT INTO student.IZ_aviationstack (flight_dates,flight_statuses,flight_number,airline_names,departure_airports,arrival_airports)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (flight_dates, flight_number) DO NOTHING;
"""
data = list(zip(flight_dates,flight_statuses,flight_number,airline_names,departure_airports,arrival_airports))

try:
    cur.executemany(sql, data)
    conn.commit()
    print('data has been ingested')

except psql.Error as e:
    conn.rollback()
    print(f"Error inserting data: {e}")

finally:
    # Close cursor and connection
   cur.close()
   conn.close()
