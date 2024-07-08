# coding = utf-8
# -*- coding:utf-8 -*-
import duckdb
import time
from statistics import mean
import json


names = ['P1','P5p1','P6p2','P11p4','P14p9','P15p1','P15p3','P16p2','P18p2','P27p4','H2p2','H8p2','H10p1','H13p1','H18pA','H40p4']

duckdb.sql("""
create table matrix_a(
    feature_id int,
    node_id int,

    primary key (feature_id, node_id)
);
""")
duckdb.sql('copy matrix_a from \'model_0_24/dt_reg_A.csv\';')
duckdb.sql('select count(*) from matrix_a;').show()

duckdb.sql("""
create table matrix_b(
    node_id int,
    threshold float,

    primary key (node_id)
);
""")
duckdb.sql('copy matrix_b from \'model_0_24/dt_reg_B.csv\';')
duckdb.sql('select count(*) from matrix_b;').show()

duckdb.sql("""
create table matrix_c(
    node_id int,
    leaf_id int,
    sign int,

    primary key (node_id, leaf_id)
);
""")
duckdb.sql('copy matrix_c from \'model_0_24/dt_reg_C.csv\';')
duckdb.sql('select count(*) from matrix_c;').show()

duckdb.sql("""
create table matrix_de(
    leaf_id int,
    left_count int,
    value float,

    primary key (leaf_id)
);
""")
duckdb.sql('copy matrix_de from \'model_0_24/dt_reg_DE.csv\';')
duckdb.sql('select count(*) from matrix_de;').show()

duckdb.sql("""
create table zero_leaf(
    leaf_id int,
    primary key (leaf_id)
);
""")
duckdb.sql('copy zero_leaf from \'model_0_24/dt_reg_zero_leaf.csv\';')
duckdb.sql('select count(*) from zero_leaf;').show()



thread = 1
optimize = True
write = False
sf = 1
times = 1

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

select
    step_3.data_id as data_id,
    matrix_de.value as value
from
(
select
    data_id,
    leaf_id,
    sum(sign) as sign
from
(

select
    step_2.data_id as data_id,
    matrix_c.leaf_id as leaf_id,
    sum(matrix_c.sign) as sign
from
(
select
    step_1.data_id as data_id,
    matrix_b.node_id as node_id
from
(
select
    house_16H.id as data_id,
    matrix_a.node_id as node_id,
    (
    case matrix_a.feature_id
    when 0 then house_16H.P1
    when 1 then house_16H.P5p1
    when 2 then house_16H.P6p2
    when 3 then house_16H.P11p4
    when 4 then house_16H.P14p9
    when 5 then house_16H.P15p1
    when 6 then house_16H.P15p3
    when 7 then house_16H.P16p2
    when 8 then house_16H.P18p2
    when 9 then house_16H.P27p4
    when 10 then house_16H.H2p2
    when 11 then house_16H.H8p2
    when 12 then house_16H.H10p1
    when 13 then house_16H.H13p1
    when 14 then house_16H.H18pA
    else house_16H.H40p4
    end
    ) as threshold
from house_16H
join matrix_a
on true
) step_1
join matrix_b
on
    step_1.node_id = matrix_b.node_id
    and step_1.threshold <= matrix_b.threshold
) step_2
join matrix_c
on step_2.node_id = matrix_c.node_id
group by step_2.data_id, matrix_c.leaf_id

union all
select
    house_16H.id as data_id,
    zero_leaf.leaf_id as leaf_id,
    0 as sign
from house_16H
join zero_leaf
on true

)
group by data_id, leaf_id
) step_3
join matrix_de
on
    step_3.leaf_id = matrix_de.leaf_id
    and step_3.sign = matrix_de.left_count

-- order by data_id
;
"""

    plan = duckdb.sql(sql_v1).fetchall()

    plan = plan[0][1]

    json_plan = json.loads(plan)
    cost.append(json_plan['timing'])
    print(json_plan['timing'])

    if write:
        with open(f'./model_0_24/hb_plan_sf{sf}_{int(time.time())}.json', 'w') as f1:
            f1.write(plan)

print('mean', mean(cost))
