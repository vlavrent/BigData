import matplotlib.pyplot  as plt
import pandas as pd
import numpy as np


df = pd.read_csv('correlated/correlated10000.csv', sep=',')

data = df.values
plt.scatter(data[:, 0], data[:, 1])
plt.show()