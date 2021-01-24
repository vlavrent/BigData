import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from mpl_toolkits.mplot3d import Axes3D


#===========================Normal Distribution============================
np.random.seed()
N= 50000
n = 4

#=======2D=======
#mu, sigma = 50,9
#=======3D========
mu, sigma = 50,8
#=======4D========
#mu, sigma = 50,8

data = np.random.normal(mu,sigma,size=(N,n))


#=========================Plot 2D data====================================
#plt.scatter(data[:,0],data[:,1])


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
data.to_csv("Normal50000_4d.csv",index=False)
#plt.savefig("Normal.png")
