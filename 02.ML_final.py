import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")
import plotly.offline as pyoff
import plotly.graph_objs as go
from keras.layers import Dense
from keras.models import Sequential
from keras.layers import LSTM
from sklearn.preprocessing import MinMaxScaler
import mysql.connector
import pymysql
from sqlalchemy import create_engine

'''
Connect with Database and merge time table and sales table
'''
mydb = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Douzi123!',
    database='premier_products',
    port=3306)
# Input data
file_masters = 'Sales_Fact.xlsx'
file_incoming = 'Time.xlsx'

#Output data
file_out = 'Time_Sales.xlsx'

# data frame masters and incoming
df_masters = pd.read_excel(file_masters)
df_incoming = pd.read_excel(file_incoming)

# Merge (left join)
df_join_left = pd.merge(df_masters, df_incoming, how='left', on=['OrderID'])
df_join_left.to_excel(file_out, index=False)

# Times_Series Sales ML input data
df_time_sales = pd.read_excel(file_out, header=0)

'''
STEP 1: Data Wrangling, convert time to datetime and sum the sales for every month
'''
#Select only time and sales
df_time_sales = df_time_sales.loc[ : , ['OrderDate', 'Sales'] ]
df_time_sales['OrderDate'] = pd.to_datetime(df_time_sales['OrderDate'])
df_time_sales['OrderDate'] = pd.to_datetime(df_time_sales['OrderDate'], format='%m/%y').dt.strftime('%Y-%m')

# #aggregate our data at the monthly level and sum up the sales column
# #convert date field from string to datetime

df_time_sales['Sales'] = df_time_sales['Sales'].astype(float)

# #groupby date and sum the sales
df_time_sales = df_time_sales.groupby('OrderDate').Sales.sum().reset_index()
# # Drop Null value
df_time_sales = df_time_sales.dropna()
print(df_time_sales.head(30))
#After check,  it is not stationary data

#plot monthly sales
plot_data = [
    go.Scatter(
        x=df_time_sales['OrderDate'],
        y=df_time_sales['Sales'],
    )
]
plot_layout = go.Layout(
        title='Montly Sales'
    )
fig = go.Figure(data=plot_data, layout=plot_layout)
# pyoff.iplot(fig)

'''
STEP 2: Build feature set, supervised data, diff sales
Use previous monthly sales to forecast the next, create columns from lag_1 to lag_12 for diff sales

'''
#create dataframe for transformation from time series to supervised
# Calculate， sales diff
df_diff = df_time_sales.copy()
#add previous sales to the next row
df_diff['prev_sales'] = df_diff['Sales'].shift(1)
#drop the null values and calculate the difference
df_diff = df_diff.dropna()
df_diff['diff'] = (df_diff['Sales'] - df_diff['prev_sales'])
# print(df_diff.head(10))
df_supervised = df_diff.drop(['prev_sales'], axis=1)
#adding lags
for inc in range(1, 13):
    field_name = 'lag_' + str(inc)
    df_supervised[field_name] = df_supervised['diff'].shift(inc)
#drop null values
df_supervised = df_supervised.dropna().reset_index(drop=True)
# print(df_supervised.head(10))

'''
STEP3: LSTM Model building
Scaler to normalise the feature range from -1,1, as variables that measure at diff scales do not contribute equally for fitting model
'''
#import MinMaxScaler and create a new dataframe for LSTM model
df_model = df_supervised.drop(['Sales','OrderDate'],axis=1)
# Split train and test set
# As the test set, selected the last 6 months’ sales.
train_set, test_set = df_model[0:-6].values, df_model[-6:].values

#apply Min Max Scaler
scaler = MinMaxScaler(feature_range=(-1, 1))
scaler = scaler.fit(train_set)
# reshape training set
train_set = train_set.reshape(train_set.shape[0], train_set.shape[1])
train_set_scaled = scaler.transform(train_set)
# reshape test set
test_set = test_set.reshape(test_set.shape[0], test_set.shape[1])
test_set_scaled = scaler.transform(test_set)

# create feature and label sets from scaled datasets
X_train, y_train = train_set_scaled[:, 1:], train_set_scaled[:, 0:1]
X_train = X_train.reshape(X_train.shape[0], 1, X_train.shape[1])
X_test, y_test = test_set_scaled[:, 1:], test_set_scaled[:, 0:1]
X_test = X_test.reshape(X_test.shape[0], 1, X_test.shape[1])

#  fit our LSTM model
model = Sequential()
model.add(LSTM(4, batch_input_shape=(1, X_train.shape[1], X_train.shape[2]), stateful=True))
model.add(Dense(1))
model.compile(loss='mean_squared_error', optimizer='adam')
model.fit(X_train, y_train, batch_size=1, verbose=1, shuffle=False)

y_pred = model.predict(X_test, batch_size=1)
# print(y_pred, y_test)

# inverse transformation for scaling to sales figure
#reshape y_pred
y_pred = y_pred.reshape(y_pred.shape[0], 1, y_pred.shape[1])
#rebuild test set for inverse transform
pred_test_set = []
for index in range(0,len(y_pred)):
    # print(np.concatenate([y_pred[index],X_test[index]],axis=1))
    pred_test_set.append(np.concatenate([y_pred[index],X_test[index]],axis=1))
#reshape pred_test_set
pred_test_set = np.array(pred_test_set)
pred_test_set = pred_test_set.reshape(pred_test_set.shape[0], pred_test_set.shape[2])
#inverse transform
pred_test_set_inverted = scaler.inverse_transform(pred_test_set)

#create dataframe that shows the predicted sales
result_list = []
sales_dates = list(df_time_sales[-6:].OrderDate)
act_sales = list(df_time_sales[-6:].Sales)
for index in range(0,len(pred_test_set_inverted)):
    result_dict = {}
    result_dict['pred_value'] = int(pred_test_set_inverted[index][0] + act_sales[index])
    result_dict['OrderDate'] = sales_dates[index]
    result_list.append(result_dict)
df_result = pd.DataFrame(result_list)
print(df_result)
# print(df_result, df_time_sales)

'''
STEP 4: Plot predict vs actual
'''
# merge with actual sales dataframe
df_sales_pred = pd.merge(df_time_sales, df_result, on='OrderDate', how='left')
file_sales_actual_pred = 'sales_actual_pred.xlsx'
df_sales_pred.to_excel(file_sales_actual_pred, index=False)

# Export to SQL
engine = create_engine('mysql+pymysql://root:Douzi123!@localhost/premier_products?charset=utf8')

try:
    df = pd.read_excel(file_out)
    df.to_sql('sales_actual_pred',con=engine,if_exists='replace',index=False)
except Exception as e:
    print(e)

# df_sales_pred = pd.merge(df_time_sales, df_sales_pred, on='OrderDate', how='left')
# print(df_sales_pred)

# plot actual and predicted
plot_data = [
    go.Bar(
        x=df_sales_pred['OrderDate'][0:],
        y=df_sales_pred['Sales'],
        name='actual'
    ),
    go.Bar(
        x=df_sales_pred['OrderDate'],
        y=df_sales_pred['pred_value'],
        name='predicted'
    )

]
plot_layout = go.Layout(
    title='Sales Prediction'
)
fig = go.Figure(data=plot_data, layout=plot_layout)
pyoff.iplot(fig)