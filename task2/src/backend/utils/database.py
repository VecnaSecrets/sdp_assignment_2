import json
from datetime import datetime, timedelta
import pandas as pd
import time
import requests
from io import StringIO
import os


class database:
    # time details
    session_start = None
    start_date = datetime(2022, 9, 1)
    last_update = datetime(2022, 9, 1)

    # core database
    data = None

    # other vars for excecution
    mask = "%Y-%m-%d %H:%M:%S"
    _ref = None
    data_urls = None
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.5249.103 Safari/537.36"}

    def __new__(self, *args, **kargs):
        if self._ref is None:
            self._ref = super().__new__(self)
        return self._ref

    def __init__(self, start=datetime(2022, 9, 1), save_path='./db.csv'):

        self.start_date = start
        #self.data = pd.DataFrame(columns=["client_user_id", "session_id", 'dropped_frames', 'FPS',	'bitrate',	'RTT',	'timestamp',	'device'])
        self.last_update = start
        self.session_start = datetime.now()
        with open('./src/backend/utils/url_data.json', 'r') as f:
            self.data_urls = json.load(f)
        self.save_path = save_path

        _ = pd.DataFrame(columns=["client_user_id", "session_id", 'dropped_frames', 'FPS',	'bitrate',	'RTT',	'timestamp',	'device'])
        _.to_csv(self.save_path, index=False)
        self.get_data_for_period(datetime(2022, 9, 1), self.start_date)

    def get_data_for_period(self, start=None, end=None, save_to_file=True):
        if start is None:
            start = self.last_update

        if end is None:
            end = self.calculate_sim_time()

        data = pd.DataFrame(columns=["client_user_id", "session_id", 'dropped_frames', 'FPS',	'bitrate',	'RTT',	'timestamp',	'device'])
        delta = (end-start).days
        for n in range(delta+1):
            date = start + timedelta(days=n)
            name = f"raw_{date.year}_{'0' if date.month < 10 else ''}{date.month}_{'0' if date.day < 10 else ''}{date.day}.csv"

            try:
                url = 'https://drive.google.com/uc?id=' + self.data_urls[name].split('/')[-2]
            except KeyError:
                print(f'File {name} not found in database')
                return False

            req = requests.get(url, headers=self.headers)
            if req.status_code != 200:
                print('Status code is {req.status_code}. Aborting')
                return False
            buff = pd.read_csv(StringIO(req.text))

            if n == 0:
                buff = buff.loc[buff['timestamp'].apply(lambda x: datetime.strptime(x, self.mask) > start)]
            if n == delta:
                buff = buff.loc[buff['timestamp'].apply(lambda x: datetime.strptime(x, self.mask) < end)]

            data = pd.concat([data, buff], axis=0)

        if save_to_file:
            data_old = pd.read_csv(self.save_path)
            pd.concat([data_old, data]).to_csv(self.save_path, index=False)

        return data

    def update_database(self, save=True):
        end = self.calculate_sim_time()
        out = self.get_data_for_period(start=self.last_update, end=end, save_to_file=save)
        self.last_update = end
        return out

    def calculate_sim_time(self):
        mins_passed = (datetime.now() - self.session_start).total_seconds() * 7
        return self.start_date + timedelta(minutes=mins_passed)

    def get_database(self):
        return pd.read_csv(self.save_path)

if __name__ == "__main__":
    # start and specify date until we download initial data
    base = database(start=datetime(2022, 9, 2))

    # update base and receive new items
    time.sleep(2)
    new_items = base.update_database()
    print(f'\ntimestamps of new items\n{new_items["timestamp"]}')

    # get actual database
    whole_base = base.get_database()
    print(f'\nwhole base shape\n{whole_base.shape}')



