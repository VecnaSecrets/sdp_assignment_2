import json
import datetime
import pandas as pd
import requests

with open('url_data.json', 'r') as f:
    data_url = json.load(f)


start = datetime.datetime(2022, 9, 1, hour=11, minute=15)
end = datetime.datetime(2022, 9, 1, hour=12, minute=13)
print(end-start)

data = pd.DataFrame(columns=["client_user_id", "session_id", 'dropped_frames', 'FPS',	'bitrate',	'RTT',	'timestamp',	'device'])
mask = "%Y-%m-%d %H:%M:%S"
delta = (end-start).days
for n in range(delta+1):

    date = start + datetime.timedelta(days=n)
    name = f"raw_{date.year}_{'0' if date.month < 10 else ''}{date.month}_{'0' if date.day < 10 else ''}{date.day}.csv"
    url = data_url[name]
    url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]
    buff = pd.read_csv(url)
    print(n)
    if n == 0:
        buff = buff.loc[buff['timestamp'].apply(lambda x: datetime.datetime.strptime(x, mask) > start)]
    if n == delta:
        buff = buff.loc[buff['timestamp'].apply(lambda x: datetime.datetime.strptime(x, mask) < end)]

    data = pd.concat([data, buff], axis=0)

print(data["timestamp"])



