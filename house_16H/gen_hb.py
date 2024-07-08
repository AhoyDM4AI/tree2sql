import joblib
from sklearn import tree
from sklearn.tree import DecisionTreeRegressor
import numpy as np
import bitarray
from typing import Tuple, List

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

# names = ['P1','P5p1','P6p2','P11p4','P14p9','P15p1','P15p3','P16p2','P18p2','P27p4','H2p2','H8p2','H10p1','H13p1','H18pA','H40p4']

node_id_map = {}

A: List[Tuple[int, int]] = []  # features * not_leaf_nodes
B: List[Tuple[int, float]] = []       # 1 * not_leaf_nodes
C: List[Tuple[int, int, float]] = []  # not_leaf_nodes * leaf_nodes
DE: List[Tuple[int, int, float]] = []       # leaf_nodes * 1
# D: List[Tuple[int, int]] = []         # 1 * leaf_nodes
# E: List[Tuple[int, float]] = []       # leaf_nodes * 1

def is_leaf(node_id) -> bool:
    if node_id == -1:
        return False
    return children_left[node_id] == -1 and children_right[node_id] == -1


def mapping(node_id: int, idx: List[int]):
    if is_leaf(node_id):
        if node_id_map.get(node_id) is None:
            node_id_map[node_id] = idx[1]
            idx[1] += 1
    if node_id_map.get(node_id) is None:
        node_id_map[node_id] = idx[0]
        idx[0] += 1
        mapping(children_left[node_id], idx)
        mapping(children_right[node_id], idx)


mapping(0, [0, 0])


"""
path: List[Tuple[bool   , int                    ]]
                 is_left, node_id (after mapping)
"""
def dfs(node_id: int, is_left: bool, path: List[Tuple[bool, int]]):
    path.append((is_left, node_id_map[node_id]))

    l_child_id = children_left[node_id]
    r_child_id = children_right[node_id]

    if is_leaf(node_id):
        # C
        j = node_id_map[node_id]
        left_count = 0
        for n in range(len(path) - 1):
            i = path[n][1]
            is_left = path[n + 1][0]
            if is_left:
                left_count += 1
                C.append((i, j, 1))
            else:
                C.append((i, j, -1))

        # DE
        DE.append((j, left_count, values[node_id][0][0]))
    else:
        # A
        i = feature[node_id]
        j = node_id_map[node_id]
        A.append((i, j))

        # B
        B.append((j, threshold[node_id]))

        # recursive
        dfs(l_child_id, True, path)
        dfs(r_child_id, False, path)

    path.pop()


dfs(0, False, [])


import pandas as pd

A.sort(key=lambda x: (x[0], x[1]))
df_A = pd.DataFrame(A, columns=['feature_id', 'node_id'])
df_A.to_csv(f'{model_name}_A.csv', index=False)

B.sort(key=lambda x: x[0])
df_B = pd.DataFrame(B, columns=['node_id', 'threshold'])
df_B.to_csv(f'{model_name}_B.csv', index=False)

C.sort(key=lambda x: (x[0], x[1]))
df_C = pd.DataFrame(C, columns=['node_id', 'leaf_id', 'sign'])
df_C.to_csv(f'{model_name}_C.csv', index=False)

DE.sort(key=lambda x: x[0])
df_DE = pd.DataFrame(DE, columns=['leaf_id', 'left_count', 'value'])
df_DE.to_csv(f'{model_name}_DE.csv', index=False)

zero_leaf = [de[0] for de in DE if de[1] == 0]
df_zero_leaf = pd.DataFrame(zero_leaf, columns=['leaf_id'])
df_zero_leaf.to_csv(f'{model_name}_zero_leaf.csv', index=False)

print()
