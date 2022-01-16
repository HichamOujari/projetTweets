#import modules
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover


sc = SparkContext('local')
spark = SparkSession(sc) 

#create Spark session
appName = "Projet Benhlima - Oujari Hicha - Ezzin Houssam - Alilou iliass - Imad Barrahou "
spark = SparkSession.builder.appName(appName).config("spark.some.config.option", "some-value").getOrCreate()


#lire data from csv
data = spark.read.csv('./data/tweet-train.csv', inferSchema=True, header=True)

#split le texte de tweet en mots + supprimer les mots non significatifs
tokenizer = Tokenizer(inputCol="tweetText", outputCol="words")
tokenizedTrain = tokenizer.transform(data)
swr = StopWordsRemover(inputCol=tokenizer.getOutputCol(),outputCol="listOfWords")
SwRemovedTrain = swr.transform(tokenizedTrain)
SwRemovedTrain.show(truncate=False, n=10)


#conversion des mots en une liste numérique pour facilite l'apprentissage par un model d'automatisation
hashTF = HashingTF(inputCol=swr.getOutputCol(), outputCol="features")
numeriqueTraningData = hashTF.transform(SwRemovedTrain).select('Sentiment', 'listOfWords', 'features')
numeriqueTraningData.show(truncate=False, n=10)


#Entraînez notre modele de classificateur à l'aide de données d'entrainement
lr = LogisticRegression(labelCol="Sentiment", featuresCol="features", maxIter=10, regParam=0.01)
model = lr.fit(numeriqueTraningData)
print("==> Resultat : Entrainement est termine avec succes!")

#Tester le modele avec les donnees de tests
#testingData = spark.read.csv('./data/new/fromStream.csv', inferSchema=True, header=True)
testingData = spark.read.csv('./data/tweet-test.csv', inferSchema=True, header=True)
tokenizedTest = tokenizer.transform(testingData)
SwRemovedTest = swr.transform(tokenizedTest)
numeriqueTestingData = hashTF.transform(SwRemovedTest).select('Sentiment', 'listOfWords', 'features')
numeriqueTestingData.show(truncate=False, n=10)

#Calculer la précision
prediction = model.transform(numeriqueTestingData)
predictionFinal = prediction.select("listOfWords", "prediction", "Sentiment")
predictionFinal.show(n=10)

correctPrediction = predictionFinal.filter(predictionFinal['prediction'] == predictionFinal['Sentiment']).count()
totalData = predictionFinal.count()

print(" - Data total :", totalData)
print(" - Total des predictions correcte:", correctPrediction)
print(" - Pourcentage:", correctPrediction/totalData)