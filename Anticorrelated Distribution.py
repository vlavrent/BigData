import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from mpl_toolkits.mplot3d import Axes3D


#===========================Normal Distribution============================
#np.random.seed(9)
N=50000


#=======2D=======
var1 = 60
var2 = 60
cov  = -53
cov_matrix = [[var1,cov],[cov,var2]]
mean = [50,50]
#=======3D========
#var1 = 50
#var2 = 50
#var3 = 50
#cov  = -22
#cov_matrix = [[var1,cov,cov],[cov,var2,cov],[cov,cov,var3]]
#mean = [50,50,50]
#=======4D========
#var1 = 50
#var2 = 50
#var3 = 50
#var4 = 50
#cov  = -16
#cov_matrix = [[var1,cov,cov,cov],[cov,var2,cov,cov],[cov,cov,var3,cov],[cov,cov,cov,var4]]
#mean = [50,50,50,50]

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
num = []
for i in range(1,N+1):
    num.append(i)
    

    

data["id"] = num
print(data)
data.to_csv("Anticorrelated50000_2d.csv",index=False)
#plt.savefig("Anticorrelated.png")
