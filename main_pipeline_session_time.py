from typing import Tuple

import numpy as np
import pandas as pd


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
    # df = df.groupby(by=["client_user_id"]).aggregate(
    #     num_sessions=pd.NamedAgg(column="session_id", aggfunc=np.count_nonzero),
    #     avg_dropped_frames=pd.NamedAgg(column="dropped_frames", aggfunc=np.mean),
    #     avg_fps=pd.NamedAgg(column="FPS", aggfunc=np.mean),
    #     std_fps=pd.NamedAgg(column="FPS", aggfunc=np.std),
    #     avg_bitrate=pd.NamedAgg(column="bitrate", aggfunc=np.mean),
    #     std_bitrate=pd.NamedAgg(column="bitrate", aggfunc=np.std),
    #     avg_rtt=pd.NamedAgg(column="RTT", aggfunc=np.mean),
    #     std_rtt=pd.NamedAgg(column="RTT", aggfunc=np.std),
    #     devices=pd.NamedAgg(column="device", aggfunc=lambda x: ", ".join(x.unique())),
    #     windows_entry=pd.NamedAgg(
    #         column="device", aggfunc=lambda x: np.sum(x == "Windows")
    #     ),
    #     mac_entry=pd.NamedAgg(column="device", aggfunc=lambda x: np.sum(x == "Mac")),
    #     android_entry=pd.NamedAgg(
    #         column="device", aggfunc=lambda x: np.sum(x == "Android")
    #     ),
    #     total_hours=pd.NamedAgg(
    #         column="timestamp", aggfunc=lambda x: np.ptp(x).total_seconds() / 3600
    #     ),
    #     last_session=pd.NamedAgg(column="timestamp", aggfunc=np.max),
    #     first_session=pd.NamedAgg(column="timestamp", aggfunc=np.min),
    # )

    # df["stream_quality"] = 0.0
    # df["next_session"] = 0.0

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
    y = df.next_session

    return X, y
