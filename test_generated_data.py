import matplotlib.pyplot  as plt
import pandas as pd
import numpy as np


df = pd.read_csv('uniform/uniform1000.csv', sep=',')
# data = df.values
data = df[["0","1"]].values
plt.scatter(data[:, 0], data[:, 1])
plt.show()