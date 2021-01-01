import matplotlib.pyplot  as plt
from mpl_toolkits import mplot3d
import pandas as pd
import numpy as np


df = pd.read_csv('Anti-Correlated/Anticorrelated10000_3d.csv', sep=',')
# data = df.values
# data = df[["0","1"]].values
# plt.scatter(data[:, 0], data[:, 1])
# plt.show()

threedee = plt.figure().gca(projection='3d')
threedee.scatter(df[["0"]], df[["1"]], df[["2"]])
plt.show()
