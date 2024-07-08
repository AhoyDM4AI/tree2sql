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

nodes = []

def is_leaf(node_id) -> bool:
    if node_id == -1:
        return False
    return children_left[node_id] == -1 and children_right[node_id] == -1

zero_nodes = []

def dfs(node_id: int, parent_id: int, is_left: bool):

    l_child_id = children_left[node_id]
    r_child_id = children_right[node_id]

    if is_leaf(node_id):
        node = (node_id, parent_id, is_left, '', '', values[node_id][0][0])
        nodes.append(node)
        global zero_nodes
        if values[node_id][0][0] == 0:
            global zero_nodes
            zero_nodes.append(node)
        return
    else:
        dfs(l_child_id, node_id, True)
        dfs(r_child_id, node_id, False)
        
        node = (node_id, parent_id, is_left, f'feature_{feature[node_id]}', names[feature[node_id]], threshold[node_id])
        nodes.append(node)
        # print(node)        

dfs(0, -1, False)

tmp = [node for node in nodes if node[3] == '' and node[5] == 0]

print()

import pandas as pd

new_nodes = [[node[1], node[0], node[2], node[4], node[5]] for node in nodes]

# sort by node id, pid, is_left
new_nodes = sorted(new_nodes, key=lambda x: (x[0], x[1], x[2]))

df = pd.DataFrame(new_nodes, columns=['pid', 'id', 'is_left', 'feature', 'value'])
df.to_csv(f'{model_name}.csv', index=False)

tmp = [node for node in new_nodes if node[3] == '' and node[4] == 0]

print()