spark
# Se importan las librerias necesarias 

import datetime
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, concat_ws
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF
# Se lee la informacion del bucket
df = spark.read.csv("s3://segundo-1001168599/headlines/final/", header=True, inferSchema=True)
# Mostramos los primeros diez registros
df.show(10)
# Mostramos los tipos de datos de las columnas
df.printSchema()
# Eliminamos los valores nulos
df = df.na.drop()
regex_tokenizer_1 = RegexTokenizer(inputCol="Nombre", outputCol="nombre_tk", pattern="\\W")
regex_tokenizer_2 = RegexTokenizer(inputCol=" Categoria", outputCol="categoria_tk", pattern="\\W")
regex_tokenizer_3 = RegexTokenizer(inputCol=" Link", outputCol="link_tk", pattern="\\W")
regex_tokenizer_4 = RegexTokenizer(inputCol="periodico", outputCol="periodico_tk", pattern="\\W")
stopwords_remover_1 = StopWordsRemover(inputCol="nombre_tk", outputCol="nombre_stop")
stopwords_remover_2 = StopWordsRemover(inputCol="categoria_tk", outputCol="categoria_stop")
stopwords_remover_3 = StopWordsRemover(inputCol="link_tk", outputCol="link_stop")
stopwords_remover_4 = StopWordsRemover(inputCol="periodico_tk", outputCol="periodico_stop")
count_vectorizer_1 = CountVectorizer(inputCol="nombre_stop", outputCol="nombre_features")
count_vectorizer_2 = CountVectorizer(inputCol="categoria_stop", outputCol="categoria_features")
count_vectorizer_3 = CountVectorizer(inputCol="link_stop", outputCol="link_features")
count_vectorizer_4 = CountVectorizer(inputCol="periodico_stop", outputCol="periodico_features")
idf_1 = IDF(inputCol="nombre_features", outputCol="features_nombre")
idf_2 = IDF(inputCol="categoria_features", outputCol="features_categoria")
idf_3 = IDF(inputCol="link_features", outputCol="features_link")
idf_4 = IDF(inputCol="periodico_features", outputCol="features_periodico")
# Creamos el pipeline de procesamiento para la transformacion de los datos
pipeline = Pipeline(stages=[regex_tokenizer_1, regex_tokenizer_2, regex_tokenizer_3, regex_tokenizer_4,
                            stopwords_remover_1, stopwords_remover_2, stopwords_remover_3, stopwords_remover_4,
                            count_vectorizer_1, count_vectorizer_2, count_vectorizer_3, count_vectorizer_4,
                            idf_1, idf_2, idf_3, idf_4])
# Aplica el pipeline a los datos
model = pipeline.fit(df)
new_data = model.transform(df)
# Convierte las columnas en una cadena de texto
array_to_string_udf = udf(lambda arr: ' '.join(str(x) for x in arr), StringType())
new_data = new_data.withColumn("nombre_features", array_to_string_udf(new_data["features_nombre"]))
new_data = new_data.withColumn("categoria_features", array_to_string_udf(new_data["features_categoria"]))
new_data = new_data.withColumn("link_features", array_to_string_udf(new_data["features_link"]))
new_data = new_data.withColumn("periodico_features", array_to_string_udf(new_data["features_periodico"]))
# Muestra los resultados
new_data.select("nombre_features","categoria_features","link_features","periodico_features").show()
job.commit()