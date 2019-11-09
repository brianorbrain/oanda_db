# oanda_db
Oanda Candle Database

Posting a little script that I have for downloading forex candles.  It will look at each pair:timeframe and download
candles as needed.  If there are no candles yet, it will pull 250 of them.  If the candles do exist already, it will start 
from the last complete candle and move forward downloading as needed.

I have been using this database to pull candle data into Pandas Dataframes to play with and experiment on any and all correlations.

I had been using a MySQL database to store everything.

To use you need to first initialize the database metadata, add login info the the updater loop and run it.

The beauty of the script is that it changes the string time from the ISO format in JSON over to a python datetime object
that can be stored nicely in a MySQL database.

I had been using it on a cron loop to keep the database up to date.

With 1 minute data, the database will get to millions of lines before too long. 

In the DB connect file, there is a dataframe select function.  This function is indespensable for analyzing data.

I will update the files when I have time to clean up the data.
