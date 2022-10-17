import catboost
import numpy as np
import pandas as pd
from pathlib import Path
import time
import os
from pickle import dump, load

#from joblib import dump, load

#prefix = './'
prefix = './src/model/'
flag = False

class Model():
    data_input_path = prefix + 'model_input/'
    data_output_path = prefix + 'model_output/'
    data_model_path = prefix + 'models/'
    #inference_history = []

    def __init__(self) -> None:
        os.makedirs(os.path.dirname(self.data_input_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.data_output_path), exist_ok=True)
        print('Model is created!!!')

        with open(self.data_model_path + "stream_model.pkl", "rb") as f:
            self.stream_model = load(f)
        with open(self.data_model_path + "time_model.pkl", "rb") as f:
            self.time_model = load(f)
        self.run()


    def train(self, data_path):
        print(f'Gonna train!!!')
        # todo
        train_data = pd.read_csv(data_path)
        print(data_path.split('/')[-1].split("_")[0])
        if data_path.split('/')[-1].split("_")[0] == 'users':
            x_train, y_train = self.pipeline_users(train_data)
            with open(self.data_model_path + "time_model.pkl", "wb") as f:
                dump(self.time_model, f)
            return True

        if data_path.split('/')[-1].split("_")[0] == 'sessions':
            return True

        print("Incorrect file name")
        return False

    def predict(self, data_path):
        print("predicting")
        data = pd.read_csv(Path(data_path))
        if data_path.split('/')[-1].split("_")[0] == 'users':
            data_clean, _ = self.pipeline_users(data)
            data["next_session"] = self.time_model.predict(data_clean)
            return data[["client_user_id", "next_session"]]

        if data_path.split('/')[-1].split("_")[0] == 'sessions':
            data_clean = self.pipeline_sessions(data)
            data["stream_quality"] = self.stream_model.predict(data_clean)
            return data[["session_id", "stream_quality"]]

        print("File name is not correct")
        return False
    """
    def updates_present(self):
        # todo: check if needs updates
        # return self.db.size() > self.cur_db_size
        return len(os.listdir(self.data_dir)) > 0
    """

    def run(self):
        print(f'Gonna run the model!!!')

        while True:
            for input_csv_path in os.listdir(self.data_input_path):
                print(f'updates present, need to train')
                self.train(self.data_input_path + input_csv_path)

                prediction = self.predict(self.data_input_path + input_csv_path)
                prediction.to_csv(self.data_output_path + input_csv_path, index=False)

                Path(self.data_input_path + input_csv_path).unlink()
            time.sleep(3)



    def pipeline_users(self, df: pd.DataFrame):
        """Applies a custom pipeline to the passed dataset.
        Parameters
        ----------
        df : pd.DataFrame
            dataset to be transformed
        Returns
        -------
        Tuple[pd.DataFrame, pd.Series]
            X and y
        """

        df = df.reset_index(drop=True)
        df = df.replace(to_replace=np.nan, value=0)

        # Data Engineering
        df = df.drop(
            labels=[
                "client_user_id",
                "devices",
                "android_entry",
                "last_session",
                "first_session",
            ],
            axis=1,
        )

        # Split
        X = df.drop(labels=["stream_quality", "next_session"], axis=1)
        y = df.total_hours

        return X, y
    def pipeline_sessions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applies a custom pipeline to the passed dataset.
        Parameters
        ----------
        df : pd.DataFrame
            dataset to be transformed
        Returns
        -------
        pd.DataFrame
            X
        """

        df = df.reset_index(drop=True)
        df = df.replace(to_replace=np.nan, value=0)

        # Data Engineering
        df = df.drop(
            labels=[
                "client_user_id",
                "session_id",
                "session_end",
                "session_start",
                "stream_quality",
                "next_session",
                "device",
                "avg_bitrate",
                "std_bitrate",
            ],
            axis=1,
        )
        df.rename(
            columns={
                "avg_fps": "fps_mean",
                "std_fps": "fps_std",
                "avg_dropped_frames": "dropped_frames_mean",
                "std_dropped_frames": "dropped_frames_std",
                "max_dropped_frames": "dropped_frames_max",
                "avg_rtt": "rtt_mean",
                "std_rtt": "rtt_std",
            }, inplace=True
        )
        df = df[
            [
                "fps_mean",
                "fps_std",
                "rtt_mean",
                "rtt_std",
                "dropped_frames_mean",
                "dropped_frames_std",
                "dropped_frames_max",
            ]
        ]
        return df




"""
with open(prefix + 'models/stream_model.pkl', "wb+") as f:
    pickle.dump(stream_model(), f)

with open(prefix + 'models/time_model.pkl', "wb+") as f:
    pickle.dump(time_model(), f)
"""


print("Hello world")
model = Model()

