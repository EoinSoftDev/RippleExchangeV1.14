RippleTS webapp

Coded as part of a college project. This web-app repeatedly queries the rippleAPI (an online currency) with user specified date parameters and options and gathers the data, writes it to a historical mySQL DBMS. The data is used by in-mem Spark Dataframes along with a new library for timeseries data (ClouderaTS) which creates a timeseriesRDD used in an ARIMA model to do some basic predictive modeling for the selected data. Front-end is  a JSP with as D3.js graph, back end is a java program.
