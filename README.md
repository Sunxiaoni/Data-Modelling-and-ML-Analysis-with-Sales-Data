# LSTM Sales Prediction in Data Modelling

#### Walmart sales data taking from open source, including product, customer, timestamp and location, etc information. 

The tasks in first, data modelling including, data cleaning, transformation, divide into dimension and feature tables with star schema. 

Secondly implement LSTM machine learning algorithm to predict the price for the next 6 month and check for accuaracy rate.

#### Data pipeline 

<img width="1493" alt="Screenshot 2021-05-26 at 11 59 37" src="https://user-images.githubusercontent.com/61825187/119641549-14a95180-be1a-11eb-907d-966bc0a6bf81.png">

There are two python files attached: 
One is for the ETL pipeline including connection with MySQL database, set up the files sturture, data cleaning and transformation based on use case requirements.

The second python file is the monthly sales prediction with LSTM and visulisation with matplotlib python library.
