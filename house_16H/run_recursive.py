# coding = utf-8
# -*- coding:utf-8 -*-
import duckdb
import time
from statistics import mean
import json


names = ['P1','P5p1','P6p2','P11p4','P14p9','P15p1','P15p3','P16p2','P18p2','P27p4','H2p2','H8p2','H10p1','H13p1','H18pA','H40p4']

duckdb.sql("""
create table tree(
    pid int,
    id int,
    is_left bool,
    feature varchar,
    value float,

    primary key (pid, id, is_left)
);
""")
duckdb.sql('copy tree from \'model_0_24/dt_reg.csv\';')
duckdb.sql('select count(*) from tree;').show()
duckdb.sql("select count(*) from tree where feature is Null;").show()

thread = 1
optimize = True
write = False
sf = 1
times = 3

duckdb.sql("CREATE SEQUENCE data_id START 1;")

duckdb.sql("""
create table house_16H (
id int primary key DEFAULT nextval('data_id'),
P1 FLOAT,
P5p1 FLOAT,
P6p2 FLOAT,
P11p4 FLOAT,
P14p9 FLOAT,
P15p1 FLOAT,
P15p3 FLOAT,
P16p2 FLOAT,
P18p2 FLOAT,
P27p4 FLOAT,
H2p2 FLOAT,
H8p2 FLOAT,
H10p1 FLOAT,
H13p1 FLOAT,
H18pA FLOAT,
H40p4 FLOAT
);
""")

# data = 'random'
data = 'house_16H'
duckdb.sql(f"""INSERT INTO house_16H (P1,P5p1,P6p2,P11p4,P14p9,P15p1,P15p3,P16p2,P18p2,P27p4,H2p2,H8p2,H10p1,H13p1,H18pA,H40p4) select * FROM './data/{data}_{sf}G.csv'
-- limit 10
;""")
# duckdb.sql(f"COPY house_16H FROM '../test_lmx_reg/house_16H_{sf}G.csv';")

duckdb.sql("select count(*) from house_16H;").show()
# duckdb.sql("select * from house_16H limit 10;").show()

if not optimize:
    duckdb.sql("PRAGMA disable_optimizer;")

duckdb.sql(f"SET threads={thread};")

duckdb.sql("SET explain_output = 'all';")
duckdb.sql("PRAGMA enable_profiling='json';")

cost = []
for i in range(times):

    # best one
    sql_v1 = """
explain 
analyze
select id, (
    WITH RECURSIVE tree_hierarchy (id, feature, value) AS (
            SELECT id, feature, value
            FROM tree
            WHERE tree.pid = -1
        UNION ALL
            SELECT tree.id, tree.feature, tree.value
            FROM tree, tree_hierarchy
            WHERE tree.pid = tree_hierarchy.id and (
                (tree_hierarchy.feature = 'P1' and tree.is_left = (house_16H.P1 <= tree_hierarchy.value)) or
                (tree_hierarchy.feature = 'P5p1' and tree.is_left = (house_16H.P5p1 <= tree_hierarchy.value)) or
                (tree_hierarchy.feature = 'P6p2' and tree.is_left = (house_16H.P6p2 <= tree_hierarchy.value)) or
                (tree_hierarchy.feature = 'P11p4' and tree.is_left = (house_16H.P11p4 <= tree_hierarchy.value)) or
                (tree_hierarchy.feature = 'P14p9' and tree.is_left = (house_16H.P14p9 <= tree_hierarchy.value)) or
                (tree_hierarchy.feature = 'P15p1' and tree.is_left = (house_16H.P15p1 <= tree_hierarchy.value)) or
                (tree_hierarchy.feature = 'P15p3' and tree.is_left = (house_16H.P15p3 <= tree_hierarchy.value)) or
                (tree_hierarchy.feature = 'P16p2' and tree.is_left = (house_16H.P16p2 <= tree_hierarchy.value)) or
                (tree_hierarchy.feature = 'P18p2' and tree.is_left = (house_16H.P18p2 <= tree_hierarchy.value)) or
                (tree_hierarchy.feature = 'P27p4' and tree.is_left = (house_16H.P27p4 <= tree_hierarchy.value)) or
                (tree_hierarchy.feature = 'H2p2' and tree.is_left = (house_16H.H2p2 <= tree_hierarchy.value)) or
                (tree_hierarchy.feature = 'H8p2' and tree.is_left = (house_16H.H8p2 <= tree_hierarchy.value)) or
                (tree_hierarchy.feature = 'H10p1' and tree.is_left = (house_16H.H10p1 <= tree_hierarchy.value)) or
                (tree_hierarchy.feature = 'H13p1' and tree.is_left = (house_16H.H13p1 <= tree_hierarchy.value)) or
                (tree_hierarchy.feature = 'H18pA' and tree.is_left = (house_16H.H18pA <= tree_hierarchy.value)) or
                (tree_hierarchy.feature = 'H40p4' and tree.is_left = (house_16H.H40p4 <= tree_hierarchy.value))
            )
        )
    SELECT value
    FROM tree_hierarchy where feature is Null
) from house_16H
-- order by id
;
"""

    plan = duckdb.sql(sql_v1).fetchall()

    plan = plan[0][1]

    json_plan = json.loads(plan)
    cost.append(json_plan['timing'])
    print(json_plan['timing'])

    if write:
        with open(f'./model_0_24/recursive_plan_sf{sf}_{int(time.time())}.json', 'w') as f1:
            f1.write(plan)

print('mean', mean(cost))
