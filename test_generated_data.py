import matplotlib.pyplot  as plt
import pandas as pd
import numpy as np


df = pd.read_csv('src/main/Resource/Normal.csv', sep=',')
# data = df.values
data = df[["0","1"]].values
plt.scatter(data[:, 0], data[:, 1])
plt.show()