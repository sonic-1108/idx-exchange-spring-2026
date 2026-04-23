import pandas as pd

df = pd.read_csv('CRMLSSold202602.csv')

print(df.head())
print(df.columns)
print(df.describe())
