from typing import Tuple

import numpy as np
import pandas as pd
from category_encoders import OneHotEncoder


def pipeline(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
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
    # df = df.groupby(by=["client_user_id", "session_id"]).aggregate(
    #     avg_fps=pd.NamedAgg(column="FPS", aggfunc=np.mean),
    #     std_fps=pd.NamedAgg(column="FPS", aggfunc=np.std),
    #     avg_bitrate=pd.NamedAgg(column="bitrate", aggfunc=np.mean),
    #     std_bitrate=pd.NamedAgg(column="bitrate", aggfunc=np.std),
    #     avg_dropped_frames=pd.NamedAgg(column="dropped_frames", aggfunc=np.mean),
    #     std_dropped_frames=pd.NamedAgg(column="dropped_frames", aggfunc=np.std),
    #     max_dropped_frames=pd.NamedAgg(column="dropped_frames", aggfunc=np.max),
    #     avg_rtt=pd.NamedAgg(column="RTT", aggfunc=np.mean),
    #     std_rtt=pd.NamedAgg(column="RTT", aggfunc=np.std),
    #     device=pd.NamedAgg(column="device", aggfunc=lambda x: ", ".join(x.unique())),
    #     total_hours=pd.NamedAgg(
    #         column="timestamp", aggfunc=lambda x: np.ptp(x).total_seconds() / 3600
    #     ),
    #     session_end=pd.NamedAgg(column="timestamp", aggfunc=np.max),
    #     session_start=pd.NamedAgg(column="timestamp", aggfunc=np.min),
    # )

    # df["stream_quality"] = 0.0
    # df["next_session"] = 0.0

    df = df.reset_index()

    df = df.replace(to_replace=np.nan, value=0)

    # Data Engineering

    df = df.drop(
        labels=[
            "client_user_id",
            "session_id",
            "session_end",
            "session_start",
        ],
        axis=1,
    )

    df = OneHotEncoder(cols=["device"], use_cat_names=True).fit_transform(X=df)

    # Split

    X = df.drop(labels=["stream_quality", "next_session"], axis=1)
    y = df.stream_quality

    return X, y
