import pyspark.sql.functions as f
from pyspark.sql.functions import mean as _mean, stddev as _stddev, col
df = spark.read.csv("/FileStore/tables/house_pricing.csv", header="true", inferSchema="true")
#/FileStore/tables/house_pricing.csv
bounds = {
    c: dict(
        zip(["q1", "q3"], df.approxQuantile(c, [0.25, 0.75], 0))
    )
    for c in df.columns
}
for c in bounds:
    iqr = bounds[c]['q3'] - bounds[c]['q1']
    bounds[c]['lower'] = bounds[c]['q1'] - (iqr * 1.5)
    bounds[c]['upper'] = bounds[c]['q3'] + (iqr * 1.5)
bounded_df = df.select(
    "*",
    *[
        f.when(
            f.col(c).between(bounds[c]['lower'], bounds[c]['upper']),
            0
        ).otherwise(1).alias(c+"_outliers") 
        for c in df.columns
    ]
)
#Storing outlier values in daabricks
bounded_df.filter(bounded_df.CRIM_outliers != 0).coalesce(1).write.mode('overwrite').option("header", "true").csv("/FileStore/tables/bounds_value_outlier/")
bounded_df.filter(bounded_df.ZN_outliers !=0).select('ZN_outliers','ZN').show()
bounded_df.filter(bounded_df.INDUS_outliers !=0).select('INDUS_outliers','INDUS').show()
bounded_df.filter(bounded_df.CHAS_outliers !=0).select('CHAS_outliers','CHAS').show()
bounded_df.filter(bounded_df.NOX_outliers !=0).select('NOX_outliers','NOX').show()
bounded_df.filter(bounded_df.RM_outliers !=0).select('RM_outliers','RM').show()

'''
Spark not support visualization to data.
here i used databricks display method to visualize the contrnt

'''
display(bounded_df.select('CRIM_outliers','ZN_outliers','INDUS_outliers','CHAS_outliers','NOX_outliers','RM_outliers','AGE_outliers','DIS_outliers','RAD_outliers','TAX_outliers','PTRATIO_outliers','B_outliers','LSTAT_outliers','PRICE_outliers'))

#Calculating Z - score 
val =df.select(df.INDUS.cast("int"))
df_stats = df.select(
    _mean(df.INDUS.cast("double")).alias('mean'),
    _stddev(df.INDUS.cast("double")).alias('std'),
).collect()
#Add score_INDUS in dataframe
mean = df_stats[0]['mean']
std = df_stats[0]['std']
score_INDUS = df.withColumn("z score_INDUS", df.INDUS.cast("double") - mean / std)
score_INDUS.show()