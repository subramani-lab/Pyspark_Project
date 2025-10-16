#READ FILE FROM CSV

#METHOD-1

fire_df = spark.read\
            .format("csv")\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .load("/Volumes/workspace/default/fire_department/Fire_Department_and_Emergency_Medical_Services_Dispatched_Calls_for_Service_20251015.csv")

#METHOD -2 short cut method
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("My test app").getOrCreate()
store = spark.read.csv("/Volumes/workspace/default/superstore/Sample_Superstore.csv", header=True, inferSchema=True)

#READ DATA FROM DATA FRAME

			1.dataframe.show(10) --> It's return data in a original format

			2.Display(dataframe)

			3.Read from databricks datasource
								df = spark.sql("SELECT * FROM samples.accuweather.historical_daynight_metric")
									display(df)

			4.Conver DF into table
			If we have cluster setuped
				use --> creteGlobleTempView('xxxx')
				To Read Change notbook to SQL --> select * from global_temp.xxx

			5. If we using free version for databricks
			 	use--> fire_df.createOrReplaceTempView("fire_service")
				To Read Change notbook to SQL --> select * from fire_service
				
