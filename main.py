# %%
import math
from glob import glob
from os import path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from catboost import CatBoostRegressor
from sklearn import svm
from sklearn.decomposition import PCA
from sklearn.ensemble import RandomForestRegressor
# from pandas_profiling import ProfileReport
from sklearn.impute import SimpleImputer
from sklearn.linear_model import (ElasticNetCV, LassoCV, LinearRegression,
                                  RidgeCV)
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import MinMaxScaler, PolynomialFeatures
from xgboost import XGBRegressor


# %%
sns.set_theme(style="whitegrid", font="Verdana")

# %%
%matplotlib inline


# %% [markdown]
# # Functions

# %%
def show_heatmap(X: pd.DataFrame) -> None:
    """Plots heatmap of a dataset.

    Parameters
    ----------
    X : pd.DataFrame
        data frame
    """
    figure = plt.figure(figsize=(12, 12), dpi=120)
    subplot = figure.add_subplot()

    sns.heatmap(X.corr(method="spearman"), annot=True, linewidths=0.5, square=True)

    plt.show()


# %%
def show_boxplot(X: pd.DataFrame) -> None:
    """Plots boxplots of each feature in a dataset.

    Parameters
    ----------
    X : pd.DataFrame
        data frame
    """
    columns = 4
    rows = math.ceil(X.shape[1] / columns)

    figure = plt.figure(figsize=(rows * columns * 2, rows * columns * 2), dpi=120)
    figure.subplots_adjust(wspace=0.4, hspace=0.2)

    for index, column in enumerate(iterable=X.columns):
        subplot = figure.add_subplot(rows, columns, index + 1)

        boxplot = subplot.boxplot(x=column, data=X)
        subplot.set_title(label=column)

    plt.show()


# %%
def show_pairplot(X: pd.DataFrame, target_label: str, alpha: int = 0.1) -> None:
    """Plots pair plots of a dataset.

    Parameters
    ----------
    X : pd.DataFrame
        data frame
    target_label : str
        target column label
    alpha : float = 0.01
        transparency coefficient
    """
    row_index = 1

    rows = math.ceil(X.shape[1] / 2) + 1
    columns = 3

    plot_color = "red"
    mean_color = "lightgray"

    data_frame_columns = X.columns[:-1]

    figure = plt.figure()

    figure.subplots_adjust(wspace=0.4, hspace=0.4)
    figure.set_size_inches(w=16, h=32)
    figure.set_dpi(val=120)

    for index in range(len(data_frame_columns)):
        index *= 2

        if index + 1 < len(data_frame_columns):
            subplot = figure.add_subplot(rows, columns, row_index)
            subplot.set(
                xlabel=data_frame_columns[index], ylabel=data_frame_columns[index + 1]
            )
            subplot.grid(False)
            subplot.scatter(
                x=X[data_frame_columns[index]],
                y=X[data_frame_columns[index + 1]],
                c=plot_color,
                alpha=alpha,
            )
            subplot.axvline(x=np.mean(X[data_frame_columns[index]]), c=mean_color)
            subplot.axhline(y=np.mean(X[data_frame_columns[index + 1]]), c=mean_color)

            subplot = figure.add_subplot(rows, columns, row_index + 2)
            subplot.set(xlabel=data_frame_columns[index + 1], ylabel=target_label)
            subplot.grid(False)
            subplot.scatter(
                x=X[data_frame_columns[index + 1]],
                y=X[target_label],
                c=plot_color,
                alpha=alpha,
            )
            subplot.axvline(x=np.mean(X[data_frame_columns[index + 1]]), c=mean_color)

        if index < len(data_frame_columns):
            subplot = figure.add_subplot(rows, columns, row_index + 1)
            subplot.set(xlabel=data_frame_columns[index], ylabel=target_label)
            subplot.grid(False)
            subplot.scatter(
                x=X[data_frame_columns[index]],
                y=X[target_label],
                c=plot_color,
                alpha=alpha,
            )
            subplot.axvline(x=np.mean(X[data_frame_columns[index]]), c=mean_color)

        row_index += columns

    # Lines indicate mean values
    plt.show()


# %%
def show_linear_metrics(y_true: np.array, y_pred: np.array) -> None:
    """Prints linear metrics.

    Parameters
    ----------
    y_true : np.array
        y true
    y_pred : np.array
        y predicted
    """
    print("Mean Absolute Error:", mean_absolute_error(y_true=y_true, y_pred=y_pred))
    print("Mean Squared Error:", mean_squared_error(y_true=y_true, y_pred=y_pred))
    print(
        "Root Mean Squared Error:",
        mean_squared_error(y_true=y_true, y_pred=y_pred, squared=False),
    )
    print("R^2 Score:", r2_score(y_true=y_true, y_pred=y_pred))


# %% [markdown]
# # [Dataset](https://drive.google.com/drive/folders/1nfrYxDm7TLzls9pedZbLX5rP4McVDWDe)

# %%
DATASET_PATH = "SSD2022AS2"


# %%
csv_files = glob(pathname=path.join(DATASET_PATH, "*.csv"), recursive=True)


# %%
dfs = list()

for csv_file in csv_files:
    dfs.append(pd.read_csv(filepath_or_buffer=csv_file))


# %%
df = pd.concat(objs=dfs).reset_index(drop=True)


# %%
df.timestamp = pd.to_datetime(df.timestamp)


# %%
df

# %% [markdown]
# # Stream Quality

# %% [markdown]
# # Next Session Time

# %%
df_ = df.copy()

# %%
df = df_.groupby(by=["client_user_id", "session_id"]).aggregate(
    {   "dropped_frames": [np.mean, np.std, np.max],
        "FPS": [np.mean, np.std],
        "bitrate": [np.mean, np.std],
        "RTT": [np.mean, np.std],
        "timestamp": [np.ptp, np.min],
    }, as_index=False
)
df

# %%
df = df_.groupby(by=["client_user_id", "session_id"], as_index=False).aggregate(
    {   "dropped_frames": [np.mean, np.std, np.max],
        "FPS": [np.mean, np.std],
        "bitrate": [np.mean, np.std],
        "RTT": [np.mean, np.std],
        "timestamp": [np.ptp, np.min],
        }
)
df.columns = ["_".join(column).lower() for column in df.columns.to_flat_index()]
#df.loc[df["client_user_id_"] == df["client_user_id_"].iloc[4]].shape[0]
#df.apply(lambda x: (df.loc[df["client_user_id_"]] == x["client_user_id_"]).shape[0], axis=0)
df["timestamp_ptp"] = df["timestamp_ptp"].apply(lambda x: x.total_seconds())
df["user_ptp_avg"] = df.apply(lambda x: np.average(df["timestamp_ptp"].loc[df["client_user_id_"] == x["client_user_id_"]]), axis=1)
def f(x, df):
    subset = df.loc[df["client_user_id_"] == x["client_user_id_"]].sort_values(by = ["timestamp_ptp"])
    for index, row in subset.iterrows():
        if x["timestamp_amin"] < row["timestamp_amin"]:
            return row["timestamp_ptp"]
    return None

df["user_next_ptp"] = df.apply(lambda x: f(x, df), axis=1)
df

# %%
df.columns = ["_".join(column).lower() for column in df.columns.to_flat_index()]
df

# %%
df = df.reset_index(drop=True)


# %%
df.timestamp_ptp = df.timestamp_ptp.dt.total_seconds()


# %%
df = df.drop_duplicates()


# %%
# df.to_csv(path_or_buf="output.csv")


# %%
# ProfileReport(df=df).to_file(output_file="df.html")


# %%
df

# %% [markdown]
# # Data Engineering

# %%
df.describe()


# %%
df.isna().sum()


# %%
df[df.isna().any(axis=1)]


# %%
# df[:] = SimpleImputer().fit_transform(X=df)

df = df.replace(to_replace=np.nan, value=0)


# %%
show_heatmap(X=df)


# %%
show_boxplot(X=df)


# %%
show_pairplot(X=df, target_label="timestamp_ptp")


# %%
df[df.columns[:-1]] = MinMaxScaler().fit_transform(X=df[df.columns[:-1]])


# %% [markdown]
# # Split

# %%
df_train, df_test = train_test_split(df, test_size=0.1)

df_train = df_train.reset_index(drop=True)
df_test = df_test.reset_index(drop=True)


# %%
# Outliers
# quantile = df_train.quantile(q=0.75)
# coef = 50

# for column in df_train.columns:
#     df_train = df_train.query(f"{column} < {quantile[column] * coef}")


# %%
df_train.shape


# %%
# X = X.drop(labels=["fps_mean", "fps_std", "rtt_mean", "rtt_std"], axis=1)


# %%
# X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1)

X_train = df_train.drop(labels=["timestamp_ptp"], axis=1)
X_test = df_test.drop(labels=["timestamp_ptp"], axis=1)

y_train = df_train.timestamp_ptp
y_test = df_test.timestamp_ptp


# %% [markdown]
# # Model

# %%
# pipeline = make_pipeline(PolynomialFeatures(), LassoCV(max_iter=10000))

# param_grid = {"polynomialfeatures__degree": np.arange(2, 5)}

# polynomial_regression = GridSearchCV(
#     estimator=pipeline,
#     param_grid=param_grid,
#     scoring="r2",
#     cv=10,
#     return_train_score=True,
# )
# polynomial_regression.fit(X=X_train, y=y_train)

# y_pred = polynomial_regression.predict(X=X_test)

# print("Degree:", polynomial_regression.best_params_["polynomialfeatures__degree"])


# %%
# pipeline = make_pipeline(PolynomialFeatures(degree=2), LassoCV(max_iter=10000))
# pipeline.fit(X=X_train, y=y_train)

# y_pred = pipeline.predict(X=X_test)


# %%
# ridge_cv = RidgeCV()
# ridge_cv.fit(X=X_train, y=y_train)

# y_pred = ridge_cv.predict(X=X_test)


# %%
# random_forest = RandomForestRegressor()

# random_forest.fit(X=X_train, y=y_train)
# y_pred = random_forest.predict(X=X_test)


# %%
cb = CatBoostRegressor()
cb.fit(X=X_train, y=y_train)

y_pred = cb.predict(data=X_test)


# %%
# xgb = XGBRegressor()
# xgb.fit(X=X_train, y=y_train)

# y_pred = xgb.predict(X=X_test)


# %%
# svr = svm.SVR()

# svr.fit(X=X_train, y=y_train)
# y_pred = svr.predict(X=X_test)


# %%
# elastic_net_cv = ElasticNetCV()
# elastic_net_cv.fit(X=X_train, y=y_train)

# y_pred = elastic_net_cv.predict(X=X_test)


# %%
show_linear_metrics(y_true=y_test, y_pred=y_pred)


# %%
figure = plt.figure(figsize=(6, 6), dpi=120)
subplot = figure.add_subplot()

subplot.scatter(x=np.arange(len(y_test)), y=y_test, c="red", label="y_test")
subplot.scatter(x=np.arange(len(y_pred)), y=y_pred, c="blue", label="y_true")

plt.legend()
plt.show()


# %% [markdown]
# # Total Number of Bad Sessions

# %%



# %%
