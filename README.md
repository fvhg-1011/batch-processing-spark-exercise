# Batch Processing with Pyspark 101

This simple pyspark code was one of the assignment from data-eng bootcamp and one of my learnin process to get hands-on dirty.

The database postgres that used in here is referenced from [here](https://github.com/thosangs/dibimbing_spark_airflow)

Here is simple how to run 
1. install pyspark first(recomendded for using virtualenv)
2. run this command `spark-submit --driver-class-path postgresql-42.2.18.jar test-retail.py` (somehow in my device, the .jar files cant be run if put inside the sparksession config so this is the other way how to run the pyspark files)

Code Explanation<br>
1. retail_data is basically how we create connection to the postgredb(it needs to be load by using the .jar files) and it was read databases public with retail table  
2. There are two analysis that have been created:<br>
	a.agg_ouput(agg_df) -> that count how many countries on the retail table and the data will be saved inside agg_ouput.csv folders<br>
	b.churn_output(churn_df) -> count how many person that potentially churn and the data will be saved inside churn_output.csv folders    

	for the churn_ouput here is the simple explanation:<br>
	--> in this df, it was assumend that invoice_date represents date and customerid represents the unique customers(one customer)<br>
	--> for churn_df it was grouped by customerid and then calucalte the  count of order_count also find the maximum value of last_order_date (the recent buying)
	-> and than assume of the churn_threshold as 
	
