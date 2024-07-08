# tree2sql

```
.
|-- README.md
|-- gen_sql_house_16H_dt.py   # case when 生成（使用masq）
|-- house_16H
|   |-- csv2npy.py
|   |-- data
|   |-- draw.py
|   |-- fig
|   |-- gen_hb.py             # hummingbird 生成
|   |-- gen_qs.py             # quickscorer 生成
|   |-- gen_qs_v1.py
|   |-- gen_recursive.py      # with recursive 生成
|   |-- model_0_24            # 模型、生成的 csv 和 sql
|   |-- run_hb.py             # hummingbird 运行
|   |-- run_masq.py           # case when 运行
|   |-- run_qs.py             # quickscorer 运行
|   |-- run_qs_v1.py
|   |-- run_recursive.py      # with recursive 运行
|   |-- run_sklearn.py        # sklearn 运行
|   |-- scale.py
|   |-- train.py
|   `-- tree_print.py
|-- model_transformer         # masq 代码
`-- requirements.txt
```