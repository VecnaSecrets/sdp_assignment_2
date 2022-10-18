import numpy as np
import pandas as pd


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
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
        }
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

    # Split

    X = df

    return X
