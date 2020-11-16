import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from mpl_toolkits.mplot3d import Axes3D


#===========================Normal Distribution============================
np.random.seed(9)
N=10000
n = 3

#=======2D=======
mu, sigma = 3,0.5
#=======3D========
#mu, sigma = 3,0.6

data = np.random.normal(mu,sigma,size=(N,n))


#=========================Plot 2D data====================================
#plt.scatter(data[:,0],data[:,1])


#=========================Plot 3D data====================================
fig = plt.figure()
ax = Axes3D(fig)
ax.scatter(data[:, 0],data[:,1],data[:,2])

#=====================Saving Data========================================
data = pd.DataFrame(data)
#data.to_csv("Normal.csv")
#plt.savefig("Normal.png")
