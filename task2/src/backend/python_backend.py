import sys
sys.path.append("./src/backend/utils/")

from xmlrpc.client import boolean
import pandas as pd
from pathlib import Path
import psycopg2
from time import gmtime, strftime, sleep
from datetime import datetime, timedelta

from database import database
import tabulate

import numpy as np

START_DATE = datetime(2022, 9, 2)
GROUND_DATE = datetime(2022, 9, 1)
class System:    
    def __init__(self, data_dir, csv_filename, model=None) -> None:
        self.data_dir = Path(data_dir)
        self.db = database(start=START_DATE, save_path= self.data_dir / csv_filename)
        self.connect_db()
        #self.fetch_data() # kinda useless in init as database class download it upon creating itself
        self.update_user_data(csv_filename)
        self.model = model

    def connect_db(self):
        conn_string = "host='db' dbname='postgres_db' user='user' password='password'" 
        self.conn = psycopg2.connect(conn_string)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

        sql = """DROP TABLE IF EXISTS users;"""
        self.cursor.execute(sql)
        sql = """CREATE TABLE IF NOT EXISTS users (
                                        client_user_id varchar(50), 
                                        session_id varchar(60),
                                        dropped_frames VARCHAR(10),
                                        FPS VARCHAR(10),
                                        bitrate VARCHAR(10),
                                        RTT VARCHAR(10),
                                        timestamp timestamp WITHOUT TIME ZONE,
                                        device VARCHAR(50)
                                    )"""
        self.cursor.execute(sql)

    def fetch_data(self):
        #timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        data = self.db.update_database(save=True)
        timestamp = self.db.last_update.strftime("%B %d, %Y")
        csv_filename = f'db_update_{timestamp}.csv'
        data.to_csv(self.data_dir / csv_filename, index=False)

        return csv_filename

    def update_user_data(self, csv_filename):
        '''Update (add) user data in the database'''
        csv_path = self.data_dir / csv_filename

        sql = """COPY users FROM STDIN DELIMITER ',' CSV HEADER"""
        self.cursor.copy_expert(sql, open(csv_path, "r"))

    def update_model(self):
        pass

    def get_all_users(self):
        query = '''select distinct(user_id) from users;'''
        table = pd.read_sql(query, self.conn)
        return table

    def get_user_data(self, client_user_id=None, session_id=None):
        '''Returns data of a user, searched by user id and/or session id'''
        if client_user_id is not None and session_id is not None:
            query = '''select * from users 
                    where client_user_id = client_user_id and session_id = session_id;'''
        elif client_user_id is not None:
            query = '''select * from users 
                    where client_user_id = client_user_id;'''
        elif session_id is not None:
            query = '''select * from users 
                    where session_id = session_id;'''
        if query is not None:
            table = pd.read_sql(query, self.conn)
            return table
        else:
            return 'Please specify user_id and/or session_id'

    def get_status(self, start_date=None, end_date=None):
        if start_date is None:
            start_date = GROUND_DATE
        if end_date is None:
            end_date = self.db.last_update
        if start_date > end_date:
            print("ERROR: start date is later than end_date")
            return False

        sql_mask = "YYYY-MM-DD HH24:MI:SS"
        start_date = start_date.strftime(self.db.mask)
        end_date = end_date.strftime(self.db.mask)

        print(start_date, end_date)
        print(f"'{end_date}', '{self.db.mask}'")
        query = "select session_id, MIN(timestamp), EXTRACT(EPOCH FROM MAX(timestamp)-MIN(timestamp))/3600 as time_diff " \
                "from users " \
                f"where timestamp between to_timestamp('{start_date}', '{sql_mask}') and to_timestamp('{end_date}', '{sql_mask}') " \
                "group by client_user_id, session_id " \

        res = pd.read_sql(query, self.conn)

        system_summary = f'Total sessions : {res.shape[0]} \n' \
                     f'Average time spent per session : {np.round(np.sum(res["time_diff"])/res.shape[0] * 60,0)} mins \n' \
                     f'Sum of hours spent by all users : {np.round(np.sum(res["time_diff"]),2)} hours'

        return system_summary

    def save_summary_on_command(self, summary):
        save_to_file = input('Would you like to save the summary? (y/n)')
        if save_to_file == 'yes' or save_to_file == 'y':
            timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
            output_filename = self.data_dir / 'summaries' / f'system_summary_{timestamp}.txt'
            self.save_to_file(summary, output_filename)

    def get_status_past_week(self):
        '''1 : Get status for the past 7 days''' 
        # todo: query db, group and aggregate

        start_date = self.db.calculate_sim_time() - timedelta(weeks=1)

        system_summary = f'Statistics for the past 7 days: \n' + self.get_status(start_date)
        print(system_summary)
        self.save_summary_on_command(system_summary)
        return True

    def print_user_summary(self):
        '''2 : Print user summary'''
        user_id = input('Enter user id: \n')  # '0116f41a-28b1-4d81-b250-15d7956e2be1' 
        time_period = input('Enter period (yy/mm/dd - yy/mm/dd): \n') # '2022/07/10 - 2022/08/10' 
        
        if self.user_present(user_id):
            print('User found')
            user = User(user_id=user_id, system=self)

            user_summary = user.get_user_summary(time_period)
            print(user_summary)
            
            save_to_file = input('Would you like to save the summary? (y/n)')
            if save_to_file == 'yes' or save_to_file == 'y':
                output_filename = self.data_dir / 'summaries' / f'user_{self.user_id}_summary.txt'
                self.save_to_file(user_summary, output_filename)
        else:
            print('This user is not registered or has no games yet')

    def user_present(self, client_user_id) -> boolean:
        '''Query database to see if a user has played games before'''
        query = '''
            select 1 
            from users
            where client_user_id = client_user_id
            '''
        query_result = pd.read_sql(query, self.conn)
        return query_result is not None

    def save_to_file(self, content, filename):
        # todo: save summary to a txt file
        with open(filename, 'w+') as f:
            f.write(content)



    def predict_user_next_session_duration(self) -> float:
        '''3 : Predict user next session duration'''
        user_id = input('Enter user id: \n') # user_id = '0116f41a-28b1-4d81-b250-15d7956e2be1' 
        
        if self.user_present(user_id):
            print('User found')
            user = User(user_id=user_id, system=self)
            prediction = user.get_expected_next_session_duration()
            print(print(f'User {user_id} will play for {prediction} seconds next time'))
        else:
            print('This user is not registered or has no games yet')

    def update(self) -> None:
        '''4 : Fetch new data and update users data and ML model'''
        csv_filename = self.fetch_data()
        self.update_user_data(csv_filename=csv_filename)
        self.update_model()

    def get_top_users(self):
        '''5 : Get top 5 users based on time spent gaming'''
        # todo: query db with rank()
        query = "with foo as (" \
                "   select client_user_id, session_id, EXTRACT(EPOCH FROM MAX(timestamp::timestamp)-MIN(timestamp::timestamp))/3600 as time_diff " \
                "   from users " \
                "   group by client_user_id, session_id " \
                ") " \
                "select client_user_id, SUM(time_diff) as session_time from foo " \
                "group by client_user_id " \
                "order by session_time DESC " \
                "limit 5"

        print(tabulate.tabulate(pd.read_sql(query, con=self.conn), headers=["RANK", "USER ID", "HOURS"]))
        return True

    def exit_program(self):
        '''6 : Exit program'''
        summary = f'Statistics for the whole period: \n' + self.get_status()
        self.save_summary_on_command(summary)
        print(f'Good bye!!')

class User:

    def __init__(self, user_id, system) -> None:
        self.user_id = user_id
        self.system = system

    def get_user_data(self):
        pass

    def get_session_id(self):
        '''Returns latest session id'''
        pass

    def get_num_sessions(self):
        pass
    
    def get_first_session_dt(self):
        pass
    
    def get_latest_session_dt(self):
        pass
    
    def get_session_avg_duration(self):
        pass
    
    def get_favourite_device(self):
        pass
    
    def get_devices_used(self) -> list:
        pass
    
    def get_avg_round_trip_time(self):
        pass
    
    def get_avg_frames_per_second(self):
        pass
    
    def get_avg_dropped_frames(self):
        pass
    
    def get_avg_bitrate(self):
        pass
    
    def get_num_failed_sessions(self):
        pass

    def get_expected_next_session_duration(self):
        '''A model predicts the duration of next session for the user'''
        df = pd.DataFrame({'user_id':[self.user_id]})
        timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        df.to_csv(self.system.data_dir / 'model_input' / f'user_id_{self.user_id}_{timestamp}.csv')
        
        prediction_file_path = self.system.data_dir / 'model_output' / f'user_id_{self.user_id}_{timestamp}.csv'
        while not prediction_file_path.exists():
            sleep(3)
        prediction_df = pd.read_csv(prediction_file_path)
        prediction = prediction_df.iloc[0]['user_id']
        return prediction

    def is_super_user(self):
        '''A user is considered to be a super user if the user has sessions time > 60 mins in a week'''
        pass

    def get_user_summary(self, time_period=None):
        # todo: if time_period is None: take aall data since first session, else filter
    
        num_sessions = self.get_num_sessions()
        first_session_dt = self.get_first_session_dt()
        latest_session_dt = self.get_latest_session_dt()
        session_avg_duration = self.get_session_avg_duration()
        favourite_device = self.get_favourite_device()
        devices_used = self.get_devices_used()
        avg_round_trip_time = self.get_avg_round_trip_time()
        avg_frames_per_second = self.get_avg_frames_per_second()
        avg_dropped_frames = self.get_avg_dropped_frames()
        avg_bitrate = self.get_avg_bitrate()
        num_failed_sessions = self.get_num_failed_sessions()
        #expected_next_session_duration = self.get_expected_next_session_duration()
        #super_user = self.is_super_user()
        self.summary = f'User {self.user_id} summary:\n ' \
                        f'Number of sessions: {first_session_dt} \n' \
                        f'Date of first session: {num_sessions} \n' \
                        f'Date of most recent session: {latest_session_dt} \n' \
                        f'Average time spent per session: {session_avg_duration} \n' \
                        f'Most frequently used device: {favourite_device} \n' \
                        f'Devices used: {devices_used} \n' \
                        f'Average: \n' \
                        f'\tRound trip time (RTT): {avg_round_trip_time} \n' \
                        f'\tFrames per Second: {avg_frames_per_second} \n' \
                        f'\tDropped Frames: {avg_dropped_frames} \n' \
                        f'\tBitrate: {avg_bitrate} \n' \
                        f'Total number of bad sessions (predicted using ML model): {num_failed_sessions} \n' \
                        #f'Estimated next session time: {expected_next_session_duration} \n' \
                        #f'Super user or Not (a user who has sessions time more than 60 min in a week): {super_user} \n'
        return self.summary

    


if __name__ == '__main__':
    system = System(data_dir='./src/backend/data', csv_filename='db.csv') #todo argparse

    while True:
        action = int(input(f'Choose one operation from below :\n' \
                           f'1 : Get status for the past 7 days\n' \
                           f'2 : Print user summary\n' \
                           f'3 : Predict user next session duration\n' \
                           f'4 : Fetch new data and update users data and ML model\n' \
                           f'5 : Get top 5 users based on time spent gaming\n' \
                           f'6 : Exit program\n'
                        ))
        if action == 1:
            system.get_status_past_week()
        elif action == 2:
            system.print_user_summary()
        elif action == 3:
            system.predict_user_next_session_duration()
        elif action == 4:
            system.update()
        elif action == 5:
            system.get_top_users()
        elif action == 6:
            system.exit_program()
            break






