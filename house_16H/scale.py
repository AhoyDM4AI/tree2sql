# coding = utf-8
# -*- coding:utf-8 -*-
import pandas as pd

df = pd.read_csv('./data/house_16H.csv')
X = df.drop('price', axis=1)
y = df['price']
X.to_csv('./data/house_16H_1G.csv', index=False)

# # TO 1G
# df = pd.read_csv('./house_16H.csv')
# X = df.drop('price', axis=1)
# y = df['price']
# sf = 342
# X2 = X
# for i in range(sf - 1):
#     X2 = pd.concat([X2, X])
#     print(X2.shape)
# X2.to_csv(f'./house_16H_1G.csv', index=False)

# X = pd.read_csv('./house_16H_1G.csv')

# sf_list = [5, 10, 15, 20]

# for sf in sf_list:
#     X2 = X
#     for i in range(sf - 1):
#         X2 = pd.concat([X2, X])
#         print(X2.shape)
#     X2.to_csv(f'./house_16H_{sf}G.csv', index=False)
