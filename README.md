The `PyWarp` module provides functions which ease the interaction with the [Warp 10](https://warp10.io/) Time Series Platform.

The functions it provides can be used to fetch data from a Warp 10 instance into a [Pandas](https://pandas.pydata.org) [dataframe](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) or a [Spark](https://spark.apache.org) [dataframe](https://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes). A function is also provided for loading data from [HFiles](https://blog.senx.io/introducing-hfiles-cloud-native-infinite-storage-for-time-series-data/) into a Spark dataframe.

Finally a function allows the conversion of native Warp 10 *wrappers* into a Pandas dataframe.