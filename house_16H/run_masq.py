# coding = utf-8
# -*- coding:utf-8 -*-
import duckdb
import time
from statistics import mean
import json

thread = 1
optimize = True
write = False
sf = 1
times = 3


# time.sleep(6)
print("start!!!")

duckdb.sql("""
create table house_16H (
"P1" FLOAT,
"P5p1" FLOAT,
"P6p2" FLOAT,
P11p4 FLOAT,
P14p9 FLOAT,
P15p1 FLOAT,
P15p3 FLOAT,
P16p2 FLOAT,
"P18p2" FLOAT,
"P27p4" FLOAT,
"H2p2" FLOAT,
"H8p2" FLOAT,
"H10p1" FLOAT,
"H13p1" FLOAT,
"H18pA" FLOAT,
"H40p4" FLOAT
);
""")

# data = 'random'
data = 'house_16H'
duckdb.sql(f"COPY house_16H FROM './data/{data}_{sf}G.csv';")

duckdb.sql("select count(*) from house_16H;").show()

if not optimize:
    duckdb.sql("PRAGMA disable_optimizer;")

duckdb.sql(f"SET threads={thread};")

duckdb.sql("SET explain_output = 'all';")
duckdb.sql("PRAGMA enable_profiling='json';")

cost = []
for i in range(times):
    with open('./model_0_24/dt_reg.sql', 'r') as f:
        s = f.read()

        
        start = time.time()
        plan = duckdb.sql(f"""
                         explain analyze
        {s}
        """).fetchall()
        end = time.time()
        print('estimated', end - start)

    plan = plan[0][1]

    json_plan = json.loads(plan)
    cost.append(json_plan['timing'])
    print(json_plan['timing'])

    if write:
        with open(f'./model_0_24/masq_plan_sf{sf}_{int(time.time())}.json', 'w') as f1:
            f1.write(plan)

print('mean', mean(cost))
