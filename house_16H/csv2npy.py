import numpy as np
import pandas as pd

file = './data/house_16H_1G'
df = pd.read_csv(f'{file}.csv')
np.save(file, df.values.astype(np.float32))
