import numpy as np
import pandas as pd


# t2 = pd.read_csv("task/Task_2.csv")
t3 = pd.read_csv("task/Task_3.csv")
corr = t3.drop(columns=["V3", "V4", "V5", "V14"]).corr().abs()

# Вибираємо лише верхній трикутник матриці, щоб не дублювати пари
upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))

# Фільтруємо пари з кореляцією > 0.95
pairs = [
    (column, row, upper.loc[row, column])
    for column in upper.columns
    for row in upper.index
    if upper.loc[row, column] > 0.95
]

for p in pairs:
    print(f"{p[0]} та {p[1]}: {p[2]}")
