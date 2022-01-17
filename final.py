#import modules
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover
from pyspark.streaming import StreamingContext


#Socket info
PORT = 8499
IP = "localhost"

sc = SparkContext('local[2]',"NetworkWordCount")
spark = SparkSession(sc)

#create Spark session
appName = "Projet Benhlima - Oujari Hicha - Ezzin Houssam - Alilou iliass - Imad Barrahou "
spark = SparkSession.builder.appName(appName).config("spark.some.config.option", "some-value").getOrCreate()

#lire data from csv
#data = spark.read.csv('./data/tweet-train.csv', inferSchema=True, header=True)
data = spark.read.csv('./data/myData.csv', inferSchema=True, header=True)

#split le texte de tweet en mots + supprimer les mots non significatifs
tokenizer = Tokenizer(inputCol="tweetText", outputCol="words")
tokenizedTrain = tokenizer.transform(data)
swr = StopWordsRemover(inputCol=tokenizer.getOutputCol(),outputCol="listOfWords")
SwRemovedTrain = swr.transform(tokenizedTrain)


#conversion des mots en une liste numérique pour facilite l'apprentissage par un model d'automatisation
hashTF = HashingTF(inputCol=swr.getOutputCol(), outputCol="features")
numeriqueTraningData = hashTF.transform(SwRemovedTrain).select('Sentiment', 'listOfWords', 'features')


#Entraînez notre modele de classificateur à l'aide de données d'entrainement
lr = LogisticRegression(labelCol="Sentiment", featuresCol="features", maxIter=10, regParam=0.01)
model = lr.fit(numeriqueTraningData)
print("==> Resultat : Entrainement est termine avec succes!")

lines = spark.readStream.format("socket").option("host", IP).option("port", PORT).load()

def foreach_batch_function(df, epoch_id):
    try:
        text=df.collect()[0].value
        df2 = spark.createDataFrame([{"tweetText":text}])
        df2.show()

        tokenizedTest = tokenizer.transform(df2)
        SwRemovedTest = swr.transform(tokenizedTest)
        numeriqueTestingData = hashTF.transform(SwRemovedTest).select('tweetText','listOfWords', 'features')
        
        #Calculer la précision
        prediction = model.transform(numeriqueTestingData)
        predictionFinal = prediction.select("tweetText", "prediction")
        predictionFinal.show(n=10)
        
    except:
        print(ValueError)


lines.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()