from typing import Tuple

import numpy as np
import pandas as pd


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

    df = df.drop(
        labels=[
            "Unnamed: 0",
            "client_user_id",
            "devices",
            "last_session",
            "first_session",
            "stream_quality",
        ],
        axis=1,
    )

    df = df.replace(to_replace=np.nan, value=0)

    return (df.drop(labels=["next_session"]), df.next_session)
