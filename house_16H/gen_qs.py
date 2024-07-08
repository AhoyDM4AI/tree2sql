import joblib
from sklearn import tree
from sklearn.tree import DecisionTreeRegressor
import numpy as np
import bitarray
from typing import Tuple

model_name = './model_0_24/dt_reg'
model_path = f'{model_name}.joblib'
model: DecisionTreeRegressor = joblib.load(model_path)[1]

n_leaves = model.get_n_leaves()
print(n_leaves)

template = bitarray.bitarray([True] * n_leaves)
# print(template)
# print(template.tobytes())

n_nodes = model.tree_.node_count
children_left = model.tree_.children_left
children_right = model.tree_.children_right
feature = model.tree_.feature
threshold = model.tree_.threshold
values = model.tree_.value

names = ['P1','P5p1','P6p2','P11p4','P14p9','P15p1','P15p3','P16p2','P18p2','P27p4','H2p2','H8p2','H10p1','H13p1','H18pA','H40p4']

bits_nodes = []
bits_values = [0] * n_leaves

def is_leaf(node_id) -> bool:
    if node_id == -1:
        return False
    return children_left[node_id] == -1 and children_right[node_id] == -1


def dfs(node_id: int, start_idx: int) -> int:

    l_child_id = children_left[node_id]
    r_child_id = children_right[node_id]

    if is_leaf(node_id):
        bits_values[start_idx] = values[node_id][0][0]
        return 1
    else:
        l_n = dfs(l_child_id, start_idx)
        r_n = dfs(r_child_id, start_idx + l_n)
        arr = bitarray.bitarray([1] * n_leaves)
        arr[start_idx: start_idx + l_n] = False
        
        node = (node_id, f'feature_{feature[node_id]}', names[feature[node_id]], threshold[node_id], arr)
        bits_nodes.append(node)
        # print(node)
        
        return l_n + r_n

dfs(0, 0)

import pandas as pd

# nodes = [[node[2], node[3], str(node[4])[10: -2]] for node in bits_nodes]
# df1 = pd.DataFrame(nodes, columns=['feature', 'threshold', 'bits'])
# df1.to_csv(f'{model_name}_nodes.csv', index=False)

from collections import defaultdict
nodes_dict = defaultdict(list)
for node in bits_nodes:
    feature = node[2]
    threshold = node[3]
    bit = str(node[4])[10: -2]
    nodes_dict[feature].append([threshold, bit])

for (feature, nodes) in nodes_dict.items():
    nodes.sort(key=lambda x: x[0], reverse=True)

for (feature, nodes) in nodes_dict.items():
    df1 = pd.DataFrame(nodes, columns=['threshold', 'bits'])
    df1.to_csv(f'{model_name}_nodes_{feature}.csv', index=False)


vals = [[i, v] for i, v in enumerate(bits_values)]
df2 = pd.DataFrame(vals, columns=['id', 'value'])
df2.to_csv(f'{model_name}_values.csv', index=False)

print()
