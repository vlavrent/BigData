import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from mpl_toolkits.mplot3d import Axes3D


#===========================Normal Distribution============================
np.random.seed(9)
N=10000


#=======2D=======
var1 = 0.1
var2 = 0.1
cov  = -0.09
cov_matrix = [[var1,cov],[cov,var2]]
mean = [1.5,1.5]
#=======3D========
#var1 = 0.1
#var2 = 0.1
#var3 = 0.1
#cov  = -0.07
#cov_matrix = [[var1,cov,cov],[cov,var2,cov],[cov,cov,var3]]
#mean = [1.5,1.5,1.5]

data = np.random.multivariate_normal(mean,cov_matrix,size=N)


#=========================Plot 2D data====================================
plt.scatter(data[:,0],data[:,1])
plt.show()


#=========================Plot 3D data====================================
#fig = plt.figure()
#ax = Axes3D(fig)
#ax.scatter(data[:, 0],data[:,1],data[:,2])

#=====================Saving Data========================================
data = pd.DataFrame(data)
#data.to_csv("Anticorrelated.csv")
#plt.savefig("Anticorrelated.png")
