import joblib
import time
from statistics import mean
import numpy as np
from sklearn.ensemble import RandomForestClassifier

sf = 1
t = 1
times = 3

# time.sleep(10)
print("start!!!")

# data = 'random'
data = 'house_16H'
arr = np.load(f'./data/{data}_{sf}G.npy')
X = arr

print(X.shape)


model_path0 = './model_0_24/dt_reg.joblib'
model = joblib.load(model_path0)
# 设置线程数
model[1].n_jobs = t

cost = []

for _ in range(times):
    print('start!!!')
    start = time.time()
    pred = model.predict(X)
    end = time.time()
    print('result:', pred[0:10])
    print('shape:', pred.shape)
    print('sum:', pred.sum())
    print(end - start)
    cost.append(end - start)

print('mean', mean(cost))
