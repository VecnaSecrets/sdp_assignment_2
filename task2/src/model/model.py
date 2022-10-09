from statistics import mode
import numpy as np
import pandas as pd
from pathlib import Path
import random
import time

class Model():

    inference_history = []

    def __init__(self) -> None:
        print('Model is created!!!')
        self.run()

    def train(self, data_path):
        print(f'Gonna train!!!')
        # todo
        train_data = pd.read_csv(Path(data_path))
        print(f'train data size: {len(train_data.index)}')

    def updates_present(self):
        # todo: check if needs updates
        # return self.db.size() > self.cur_db_size
        return False

    def run(self):
        print(f'Gonna run the model!!!')
        self.data_dir = Path('./data')
        
        while True:
            if self.updates_present():
                    print(f'updates present, need to train')
                    data_path = Path('./data/users_new_data.csv')
                    self.train(data_path)

            for input_csv_path in (self.data_dir / 'model_input').iterdir():
                input_df = pd.read_csv(input_csv_path)
                user_id = input_df.iloc[0]['user_id']

                prediction = self.inference(user_id)

                output_df = pd.DataFrame({'user_id': [prediction]})
                output_csv_filename = input_csv_path.name
                output_df.to_csv(self.data_dir / 'model_output' / output_csv_filename)

                self.inference_history.append(user_id)
                input_csv_path.unlink()
                time.sleep(2)

            time.sleep(3)

    def inference(self, user_id):
        i = random.randint(0, 10)
        # todo: query from db user's prev sessions
        # and return average session duration or ML predictions
        return i


model = Model()
model.run()