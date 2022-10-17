from typing import Tuple

import numpy as np
import pandas as pd
from category_encoders import OneHotEncoder


def pipeline(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    """Applies custom pipeline to the passed dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        dataframe to transform

    Returns
    -------
    Tuple[pd.DataFrame, pd.Series]
        X and y
    """

    # Dataset

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

    return (df.drop(labels=["timestamp_ptp"]), df.timestamp_ptp)
