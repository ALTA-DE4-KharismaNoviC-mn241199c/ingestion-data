import pandas as pd

#Read csv file
df = pd.read_csv("../dataset/yellow_tripdata_2020-07.csv", sep=",")
print(df)

#Rename all the columns with snake_case format
df.columns = df.columns.str.lower().str.replace(' ', '_')

print("Columns after conversion to snake_case:")
print(df.columns)

#Select 10 top of highest number of dataframe
columns_to_show = [
    'vendorid', 'passenger_count', 'trip_distance', 'payment_type', 
    'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 
    'improvement_surcharge', 'total_amount', 'congestion_surcharge'
]
df_multiple_col = df[columns_to_show]
print("Select multiple columns:")
print(df_multiple_col.head(10))

#Cast the data type to the appropriate value
df['vendorid'] = df['vendorid'].astype('category')
df['passenger_count'] = df['passenger_count'].astype('float')
df['trip_distance'] = df['trip_distance'].astype('float')
df['payment_type'] = df['payment_type'].astype('category')
df['fare_amount'] = df['fare_amount'].astype('float')
df['extra'] = df['extra'].astype('float')
df['mta_tax'] = df['mta_tax'].astype('float')
df['tip_amount'] = df['tip_amount'].astype('float')
df['tolls_amount'] = df['tolls_amount'].astype('float')
df['improvement_surcharge'] = df['improvement_surcharge'].astype('float')
df['total_amount'] = df['total_amount'].astype('float')
df['congestion_surcharge'] = df['congestion_surcharge'].astype('float')

top_passenger_df = df.nlargest(10, 'passenger_count')[columns_to_show]

print("Top 10 rows based on highest passenger_count:")
print(top_passenger_df)