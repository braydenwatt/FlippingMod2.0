import sqlite3
import pandas as pd
import json
import xgboost as xgb
from numpy import absolute
from sklearn import datasets
from sklearn import metrics
from sklearn.model_selection import train_test_split, RepeatedKFold, cross_val_score
import matplotlib.pyplot as plt
import seaborn as sns
from xgboost import XGBRegressor


def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def flatten_data(x_series):
    flattened = []

    for row in x_series:
        try:
            data = json.loads(row)
            flat = flatten_dict(data)
            flattened.append(flat)
        except (json.JSONDecodeError, TypeError):
            flattened.append({})  # Handle malformed or missing JSON

    return pd.DataFrame(flattened)

# Connect to your SQLite database
con = sqlite3.connect("auctions.db")

# Load the entire table into a DataFrame
df = pd.read_sql_query("SELECT * FROM auctions WHERE bin = 1", con)

# Close connection (not needed after data is loaded)
con.close()

# Group the rows by item ID
grouped = df.groupby("id")

# Example: Loop through each item group
for item_id, group_df in grouped:
    y = group_df.iloc[:, 1].values
    x = group_df.iloc[:, -1].values
    print(x)
