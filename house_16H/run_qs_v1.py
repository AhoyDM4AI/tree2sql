# coding = utf-8
# -*- coding:utf-8 -*-
import duckdb
import time
from statistics import mean
import json

duckdb.read_csv('model_0_24/dt_reg_nodes.csv')
duckdb.read_csv('model_0_24/dt_reg_values.csv')

duckdb.sql("create table dt_reg_nodes (feature VARCHAR, threshold FLOAT, bits BIT);")
duckdb.sql('copy dt_reg_nodes from \'model_0_24/dt_reg_nodes.csv\';')
duckdb.sql('CREATE INDEX f_idx ON dt_reg_nodes (feature);')
# duckdb.sql('select * from dt_reg_nodes;').show()

duckdb.sql("create table dt_reg_values (id int, value FLOAT);")
duckdb.sql('copy dt_reg_values from \'model_0_24/dt_reg_values.csv\';')
# duckdb.sql('select * from dt_reg_values;').show()


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

duckdb.sql(f"""INSERT INTO house_16H (P1,P5p1,P6p2,P11p4,P14p9,P15p1,P15p3,P16p2,P18p2,P27p4,H2p2,H8p2,H10p1,H13p1,H18pA,H40p4) select * FROM './data/house_16H_{sf}G.csv'
-- limit 10
;""")
# duckdb.sql(f"COPY house_16H FROM '../test_lmx_reg/house_16H_{sf}G.csv';")

duckdb.sql("select count(*) from house_16H;").show()
duckdb.sql("select * from house_16H limit 10;").show()

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
select a.id as id, b.value as value from
(
select id, (
select bit_position('1'::BIT, bit_and(bits)) - 1 from dt_reg_nodes where 
                    (feature = 'P1' and threshold < house_16H.P1) or
                    (feature = 'P5p1' and threshold < house_16H.P5p1) or
                    (feature = 'P6p2' and threshold < house_16H.P6p2) or
                    (feature = 'P11p4' and threshold < house_16H.P11p4) or
                    (feature = 'P14p9' and threshold < house_16H.P14p9) or
                    (feature = 'P15p1' and threshold < house_16H.P15p1) or
                    (feature = 'P15p3' and threshold < house_16H.P15p3) or
                    (feature = 'P16p2' and threshold < house_16H.P16p2) or
                    (feature = 'P18p2' and threshold < house_16H.P18p2) or
                    (feature = 'P27p4' and threshold < house_16H.P27p4) or
                    (feature = 'H2p2' and threshold < house_16H.H2p2) or
                    (feature = 'H8p2' and threshold < house_16H.H8p2) or
                    (feature = 'H10p1' and threshold < house_16H.H10p1) or
                    (feature = 'H13p1' and threshold < house_16H.H13p1) or
                    (feature = 'H18pA' and threshold < house_16H.H18pA) or
                    (feature = 'H40p4' and threshold < house_16H.H40p4)
) as bit from house_16H
) a join dt_reg_values b on a.bit = b.id
-- order by a.id
;
"""

    # really slow
    sql_v2 = """
explain 
analyze
select aa.id as id, bb.value as value
from
    (select a.id as id, bit_position('1'::BIT, bit_and(b.bits)) - 1 as bit
    from house_16H a
    join dt_reg_nodes b on
    (
    (b.feature = 'P1' and b.threshold < a.P1) or
    (b.feature = 'P5p1' and b.threshold < a.P5p1) or
    (b.feature = 'P6p2' and b.threshold < a.P6p2) or
    (b.feature = 'P11p4' and b.threshold < a.P11p4) or
    (b.feature = 'P14p9' and b.threshold < a.P14p9) or
    (b.feature = 'P15p1' and b.threshold < a.P15p1) or
    (b.feature = 'P15p3' and b.threshold < a.P15p3) or
    (b.feature = 'P16p2' and b.threshold < a.P16p2) or
    (b.feature = 'P18p2' and b.threshold < a.P18p2) or
    (b.feature = 'P27p4' and b.threshold < a.P27p4) or
    (b.feature = 'H2p2' and b.threshold < a.H2p2) or
    (b.feature = 'H8p2' and b.threshold < a.H8p2) or
    (b.feature = 'H10p1' and b.threshold < a.H10p1) or
    (b.feature = 'H13p1' and b.threshold < a.H13p1) or
    (b.feature = 'H18pA' and b.threshold < a.H18pA) or
    (b.feature = 'H40p4' and b.threshold < a.H40p4)
    )
    group by a.id) aa
    join dt_reg_values bb
on aa.bit = bb.id
-- order by aa.id
;
"""

    # also slow
    sql_v3 = """
explain 
analyze
select aa.id as id, bb.value as value
from
    (select id, bit_position('1':: BIT, bit_and(bit)) - 1 as bit from
    (
    select a.id as id, bit_and(b.bits) as bit from house_16H a join dt_reg_nodes b on b.feature = 'P1' and b.threshold < a.P1 group by a.id
    union all
    select a.id as id, bit_and(b.bits) as bit from house_16H a join dt_reg_nodes b on b.feature = 'P5p1' and b.threshold < a.P5p1 group by a.id
    union all
    select a.id as id, bit_and(b.bits) as bit from house_16H a join dt_reg_nodes b on b.feature = 'P6p2' and b.threshold < a.P6p2 group by a.id
    union all
    select a.id as id, bit_and(b.bits) as bit from house_16H a join dt_reg_nodes b on b.feature = 'P11p4' and b.threshold < a.P11p4 group by a.id
    union all
    select a.id as id, bit_and(b.bits) as bit from house_16H a join dt_reg_nodes b on b.feature = 'P14p9' and b.threshold < a.P14p9 group by a.id
    union all
    select a.id as id, bit_and(b.bits) as bit from house_16H a join dt_reg_nodes b on b.feature = 'P15p1' and b.threshold < a.P15p1 group by a.id
    union all
    select a.id as id, bit_and(b.bits) as bit from house_16H a join dt_reg_nodes b on b.feature = 'P15p3' and b.threshold < a.P15p3 group by a.id
    union all
    select a.id as id, bit_and(b.bits) as bit from house_16H a join dt_reg_nodes b on b.feature = 'P16p2' and b.threshold < a.P16p2 group by a.id
    union all
    select a.id as id, bit_and(b.bits) as bit from house_16H a join dt_reg_nodes b on b.feature = 'P18p2' and b.threshold < a.P18p2 group by a.id
    union all
    select a.id as id, bit_and(b.bits) as bit from house_16H a join dt_reg_nodes b on b.feature = 'P27p4' and b.threshold < a.P27p4 group by a.id
    union all
    select a.id as id, bit_and(b.bits) as bit from house_16H a join dt_reg_nodes b on b.feature = 'H2p2' and b.threshold < a.H2p2 group by a.id
    union all
    select a.id as id, bit_and(b.bits) as bit from house_16H a join dt_reg_nodes b on b.feature = 'H8p2' and b.threshold < a.H8p2 group by a.id
    union all
    select a.id as id, bit_and(b.bits) as bit from house_16H a join dt_reg_nodes b on b.feature = 'H10p1' and b.threshold < a.H10p1 group by a.id
    union all
    select a.id as id, bit_and(b.bits) as bit from house_16H a join dt_reg_nodes b on b.feature = 'H13p1' and b.threshold < a.H13p1 group by a.id
    union all
    select a.id as id, bit_and(b.bits) as bit from house_16H a join dt_reg_nodes b on b.feature = 'H18pA' and b.threshold < a.H18pA group by a.id
    union all
    select a.id as id, bit_and(b.bits) as bit from house_16H a join dt_reg_nodes b on b.feature = 'H40p4' and b.threshold < a.H40p4 group by a.id
    )
    group by id
    ) aa
    join dt_reg_values bb
on aa.bit = bb.id
-- order by aa.id
;
"""
    
    sql_v4 = """
-- explain 
-- analyze
select id, (select value
        from dt_reg_values
        where dt_reg_values.id =
        (select bit_position('1':: BIT, bit_and(bits)) - 1 from dt_reg_nodes where
        (feature = 'P1' and threshold < house_16H.P1) or
        (feature = 'P5p1' and threshold < house_16H.P5p1) or
        (feature = 'P6p2' and threshold < house_16H.P6p2) or
        (feature = 'P11p4' and threshold < house_16H.P11p4) or
        (feature = 'P14p9' and threshold < house_16H.P14p9) or
        (feature = 'P15p1' and threshold < house_16H.P15p1) or
        (feature = 'P15p3' and threshold < house_16H.P15p3) or
        (feature = 'P16p2' and threshold < house_16H.P16p2) or
        (feature = 'P18p2' and threshold < house_16H.P18p2) or
        (feature = 'P27p4' and threshold < house_16H.P27p4) or
        (feature = 'H2p2' and threshold < house_16H.H2p2) or
        (feature = 'H8p2' and threshold < house_16H.H8p2) or
        (feature = 'H10p1' and threshold < house_16H.H10p1) or
        (feature = 'H13p1' and threshold < house_16H.H13p1) or
        (feature = 'H18pA' and threshold < house_16H.H18pA) or
        (feature = 'H40p4' and threshold < house_16H.H40p4)
        )
        ) as value
from house_16H;
"""

    sql_v5 = """
explain
analyze
select a.id as id, b.value as value
from
    (
    select house_16H.id as id, (
    select bit_position('1':: BIT, bit_and(bits)) - 1 from
    (
    select bit_and(bits) as bits from dt_reg_nodes where (feature = 'P1' and threshold < house_16H.P1)
    union all
    select bit_and(bits) as bits from dt_reg_nodes where (feature = 'P5p1' and threshold < house_16H.P5p1)
    union all
    select bit_and(bits) as bits from dt_reg_nodes where (feature = 'P6p2' and threshold < house_16H.P6p2)
    union all
    select bit_and(bits) as bits from dt_reg_nodes where (feature = 'P11p4' and threshold < house_16H.P11p4)
    union all
    select bit_and(bits) as bits from dt_reg_nodes where (feature = 'P14p9' and threshold < house_16H.P14p9)
    union all
    select bit_and(bits) as bits from dt_reg_nodes where (feature = 'P15p1' and threshold < house_16H.P15p1)
    union all
    select bit_and(bits) as bits from dt_reg_nodes where (feature = 'P15p3' and threshold < house_16H.P15p3)
    union all
    select bit_and(bits) as bits from dt_reg_nodes where (feature = 'P16p2' and threshold < house_16H.P16p2)
    union all
    select bit_and(bits) as bits from dt_reg_nodes where (feature = 'P18p2' and threshold < house_16H.P18p2)
    union all
    select bit_and(bits) as bits from dt_reg_nodes where (feature = 'P27p4' and threshold < house_16H.P27p4)
    union all
    select bit_and(bits) as bits from dt_reg_nodes where (feature = 'H2p2' and threshold < house_16H.H2p2)
    union all
    select bit_and(bits) as bits from dt_reg_nodes where (feature = 'H8p2' and threshold < house_16H.H8p2)
    union all
    select bit_and(bits) as bits from dt_reg_nodes where (feature = 'H10p1' and threshold < house_16H.H10p1)
    union all
    select bit_and(bits) as bits from dt_reg_nodes where (feature = 'H13p1' and threshold < house_16H.H13p1)
    union all
    select bit_and(bits) as bits from dt_reg_nodes where (feature = 'H18pA' and threshold < house_16H.H18pA)
    union all
    select bit_and(bits) as bits from dt_reg_nodes where (feature = 'H40p4' and threshold < house_16H.H40p4)
    )
    ) as bit from house_16H
    ) a join dt_reg_values b
on a.bit = b.id
-- order by a.id
;
"""

    plan = duckdb.sql(sql_v1).fetchall()

    plan = plan[0][1]

    json_plan = json.loads(plan)
    cost.append(json_plan['timing'])
    print(json_plan['timing'])

    if write:
        with open(f'./model_0_24/qs_plan_sf{sf}_{int(time.time())}.json', 'w') as f1:
            f1.write(plan)

print('mean', mean(cost))
