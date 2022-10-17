from glob import glob
from os import path

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from category_encoders import OneHotEncoder
from joblib import dump, load
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor

if __name__ == "__main__":
    # Dataset

    DATASET_PATH = "SSD2022AS2"

    # TODO: Add path to the .csv file here
    df = pd.read_csv(filepath_or_buffer=path.join(DATASET_PATH, ...))

    df.timestamp = pd.to_datetime(df.timestamp)

    # Pipeline

    df = df.groupby(by=["client_user_id", "session_id"]).aggregate(
        {
            "dropped_frames": [np.mean, np.std, np.max],
            "FPS": [np.mean, np.std],
            "bitrate": [np.mean, np.std],
            "RTT": [np.mean, np.std],
            "timestamp": [np.min, np.max, np.ptp],
            "device": [np.unique],
        },
        as_index=False,
    )

    df.columns = ["_".join(column).lower() for column in df.columns.to_flat_index()]

    df = df.reset_index()

    df.timestamp_ptp = df.timestamp_ptp.dt.total_seconds() / 3600
    df.device_unique = df.device_unique.apply(func=lambda x: ", ".join(x))

    df = df.replace(to_replace=np.nan, value=0)

    # Data Engineering

    df = df.drop(
        labels=["client_user_id", "session_id", "timestamp_amin", "timestamp_amax"],
        axis=1,
    )

    df = OneHotEncoder(cols=["device_unique"], use_cat_names=True).fit_transform(X=df)

    # Split

    X = df.drop(labels=["timestamp_ptp"], axis=1)
    y = df.timestamp_ptp

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1)

    # Session Time
    # Model

    # cat_boost_regressor = CatBoostRegressor()
    cat_boost_regressor = load("CatBoostRegressor.joblib")
    cat_boost_regressor.fit(X=X_train, y=y_train)

    y_pred = cat_boost_regressor.predict(data=X_test)

    # xgb_regressor = XGBRegressor()
    # xgb_regressor.fit(X=X_train, y=y_train)

    # y_pred = xgb_regressor.predict(X=X_test)
