# coding = utf-8
# -*- coding:utf-8 -*-

""" 加载数据集 """
import pandas as pd

trees = 100
max_depth = 5

df = pd.read_csv('./data/house_16H.csv')
df = df.head(10000)


""" 标准化 """
from sklearn.compose import ColumnTransformer

X = df.drop('price', axis=1)
y = df['price']

column_transformer = ColumnTransformer(
    transformers=[],
    remainder='passthrough'
)

column_transformer.fit(X.values)

""" 训练 """
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import cross_val_score
from sklearn.tree import DecisionTreeRegressor

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

X_train = X_train.values
X_train_std = column_transformer.transform(X_train.copy())
X_test = X_test.values
X_test_std = column_transformer.transform(X_test.copy())
y_train = y_train.values
y_test = y_test.values

regressors = [
    ("RandomForestRegressor", RandomForestRegressor(n_estimators = trees, max_depth=max_depth)),
    ("GradientBoostingRegressor", GradientBoostingRegressor(n_estimators = trees, max_depth=max_depth)),
    ("DecisionTreeRegressor", DecisionTreeRegressor(max_depth=max_depth)),
]

regressor_tuple = regressors[2]
regressor = regressor_tuple[1]

regressor.fit(X_train_std, y_train)

training_score = cross_val_score(regressor, X_train_std, y_train, cv=5)
print("Regressors: ", regressor.__class__.__name__, "Has a training score of", round(training_score.mean(), 2))


""" pipeline """
from sklearn.pipeline import Pipeline

model = Pipeline(
    steps=[
        ('preprocessor', column_transformer),
        (regressor_tuple[0], regressor)
    ]
)

training_score = cross_val_score(model, X_train, y_train, cv=5)
print("Regressors: ", regressor.__class__.__name__, "Has a training score of", round(training_score.mean(), 2))

print('depth', regressor.get_depth())
print('leaves', regressor.get_n_leaves())
print('nodes', regressor.tree_.node_count)

""" save """
import joblib

model_path = f'./model_0_24/dt_reg'
joblib.dump(model, model_path + ".joblib")
