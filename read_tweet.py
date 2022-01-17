import time
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover
from pyspark.streaming import StreamingContext

#Socket info
PORT = 8499
IP = "localhost"


#create Spark session
appName = "app"
spark = SparkSession \
    .builder \
    .appName(appName) \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


lines = spark.readStream.format("socket").option("host", IP).option("port", PORT).load()


def foreach_batch_function(df, epoch_id):
    try:
        text=df.collect()[0].value
        print(text)
        df2 = spark.createDataFrame([{"Sentiment": "1","tweetText":text}])
        df2.show()

    except:
        print(ValueError)



lines.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()
