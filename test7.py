import asyncio
import time
import requests
import re
import logging
import serial
from parse import parse
import datetime
import pytz
from statistics import mean
import json

from influxdb import InfluxDBClient
from influxdb.client import InfluxDBClientError

host = 'pi3.local'
port = 8086
USER = 'grafana'
PASSWORD = 'mypassword'

headers = {
    'User-Agent': 'MatsTestApp/0.1'
}

params = {
    'lat': '57.686659',
    'lon': '12.21169'
}


ser=serial.Serial("/dev/serial0",2400,serial.EIGHTBITS,serial.PARITY_EVEN)


logging.basicConfig(level=logging.INFO)


def add_new_value(arr, val):
    arr.insert(0, val)
    arr.pop()
    return mean(arr)

async def elpris_worker():
    DBNAME = 'elpris_db'

    while True:
        today = datetime.datetime.today().strftime('%Y/%m-%d')
        price_url = "https://www.elprisetjustnu.se/api/v1/prices/" + today + "_SE3.json"
        logging.getLogger(__name__).info("Fetch data from " + price_url)
        response = requests.get(price_url).json()


        client = InfluxDBClient(host, port, USER, PASSWORD, DBNAME)

        json_db_body = []
        for entry in response:
            try:
                time_start = entry["time_start"]
                time_start_utc = re.sub(r"\+\d\d:\d\d", "Z", time_start)
                sek_per_kwh = float(entry["SEK_per_kWh"])
                json_db_body.append({"measurement": "price", "time": time_start_utc, "fields": {"sek_per_pwh": sek_per_kwh}})
            except Exception as e:
                logging.getLogger(__name__).error("Unexpected data: " + str(e) + ": " + str(response))

        logging.getLogger(__name__).info("write to db " + str(json_db_body))
        client.write_points(json_db_body)
        await asyncio.sleep(3600)

async def yr_worker():
    DBNAME = 'yr_db'
    client = InfluxDBClient(host, port, USER, PASSWORD, DBNAME)

    while True:
        response = requests.get('https://api.met.no/weatherapi/locationforecast/2.0/compact', headers=headers, params=params)

        if response.status_code == 200:
            data = json.loads(response.text)
            print(json.dumps(data, indent=4))

            json_db_body = []
            for entry in data["properties"]["timeseries"]:
                try:
                    time_start_utc = entry["time"]
                    print(time_start_utc)
                    temp = float(entry["data"]["instant"]["details"]["air_temperature"])
                    json_db_body.append({"measurement": "temp", "time": time_start_utc, "fields": {"temp": temp}})
                except Exception as e:
                    logging.getLogger(__name__).error("Unexpected data: " + str(e) + ": " + str(response))

            logging.getLogger(__name__).info("write to db " + str(json_db_body))
            client.write_points(json_db_body)
        else:
            print(f"Request failed with status code {response.status_code}")
        await asyncio.sleep(3650)



async def secondWorker():
    DBNAME = 'dba'
    client = InfluxDBClient(host, port, USER, PASSWORD, DBNAME)

    watt_list = [0] * 6
    idx = 0
    while True:
        # Read until tilde (0x7e)
        data = ser.read_until(expected=b'~')

        # If more than 1 char we are out of synk
        if len(data) != 1:
            continue

        # Read at least size part of header
        while len(data) < 3:
            data = data+ser.read_until(expected=b'~')

        # Get frame size
        l = (256*data[1]+data[2])&1023

        # Read until we get the whole frame
        while len(data)<(l+2):
            data=data+ser.read_until(expected=b'~')


        time_start_utc =  str(time.time()) #datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
#        time_start_utc = str(time_start_utc) + ":00:00"

        print(time_start_utc)
        idx += 1

        # Make shure the size of the frame is OK
        if len(data)==(l+2):
            # Parse
            try:
                result = parse(data)
            except Exception as e:
                print("Oops!", e.__class__, "occurred.")
            else:
                #Print results
                json_db_body = []

                for r in result:
#                    print(f"{r}={result[r]['value']} {result[r]['unit']}")
                    if (r != "0-0:1.0.0.255"):
                        x = r   
                        if x == "1-0:1.7.0.255":
                            x = "mapa" # 023-03-02T00:00:00+01:00
                            add_new_value(watt_list, float(result[r]['value']))
                            if idx % 6 == 0:
                                json_db_body.append({"measurement": x, "fields": { result[r]['unit']: add_new_value(watt_list, float(result[r]['value']))} })
                                logging.getLogger(__name__).info("write to db " + str(json_db_body))
                                client.write_points(json_db_body)


        await asyncio.sleep(5)




loop = asyncio.get_event_loop()
try:
    asyncio.ensure_future(elpris_worker())
    asyncio.ensure_future(secondWorker())
    asyncio.ensure_future(yr_worker())
    loop.run_forever()
except KeyboardInterrupt:
    pass
finally:
    print("Closing Loop")
    loop.close()


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

