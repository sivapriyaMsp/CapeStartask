import pyspark.sql.functions as psf
from pyspark.sql.types import ArrayType, StructField, StructType, StringType
##Getting file from input location
##Storing into dataframe

def CheckStop(mystr):
  if any(x in mystr for x in Stop_words):
      return mystr.replace(x,"----")
articles = spark.read.csv("/FileStore/tables/articles.csv", header="true", inferSchema="true")

#Removing html tags from Article.Description,Full_Article
#Concat Heading,Article_description and Full_Article using space and trim the white spaces
change_articles = articles.\
withColumn("Article.Description", psf.regexp_replace("`Article.Description`", "<[^>]+>", "")).\
withColumn("Full_Article",psf.regexp_replace("Full_Article", "<[^>]+>", "")).\
withColumn("Preprocessed_Text",trim(concat(col("Heading"),lit(' '),col("`Article.Description`"),lit(' '),col("Full_Article"))))
#Removing Puncuaion from dataframe
change_articles = change_articles.withColumn("Preprocessed_Text",regexp_replace("Preprocessed_Text", "[.]", ""))

#Removing stop words from Preprocessed_Text
Stop_words = ['me','then','i','he','she','them','their']
Changed_article = change_articles.withColumn('Preprocessed_Text_removedStopWords', psf.regexp_extract('Preprocessed_Text', '(?=^|\s)(' + '|'.join(Stop_words) + ')(?=\s|$)', 0))
#Changed_article.selectExpr('Preprocessed_Text1','Preprocessed_Text').show()
Changed_article.coalesce(1).write.mode('overwrite').option("header", "true").csv("/FileStore/tables/Changed_articles/")
unknown_articles = spark.read.csv("/FileStore/tables/unknown_articles-1.csv",header="true",inferSchema="true")
unknown_articleRDD = unknown_articles.rdd.map(list)
#print predict content type in each URL in CSV  
def processRecord(x) :
  for j in x:
    r = requests.get(j)
    data = r.content
    return x,data
print(unknown_articleRDD.map(lambda x : processRecord(x)).collect())