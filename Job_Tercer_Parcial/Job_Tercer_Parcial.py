spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import VectorAssembler 
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import unix_timestamp, from_unixtime, col

from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import LogisticRegression
df = spark.read.csv("s3://segundo-1001168599/headlines/final/", header=True, inferSchema=True)
# Mostramos los primeros diez registros
df.show(10)
# Mostramos los tipos de datos de las columnas
df.printSchema()
df = df.na.drop()
tokenizer_1 = Tokenizer(inputCol="Nombre", outputCol="nombre_tk")
tokenizer_2 = Tokenizer(inputCol=" Categoria", outputCol="categoria_tk")
tokenizer_3 = Tokenizer(inputCol=" Link", outputCol="link_tk")
tokenizer_4 = Tokenizer(inputCol="periodico", outputCol="periodico_tk")

# df_tokenizado = tokenizer.transform(df)
hashingTF_1 = HashingTF(inputCol="nombre_tk", outputCol="features_n")
hashingTF_2 = HashingTF(inputCol="categoria_tk", outputCol="features_c")
hashingTF_3 = HashingTF(inputCol="link_tk", outputCol="features_l")
hashingTF_4 = HashingTF(inputCol="periodico_tk", outputCol="features_p")

# df_tf = hashingTF.transform(df_tokenizado)
pipeline = Pipeline(stages=[tokenizer_1,tokenizer_2,tokenizer_3,tokenizer_4,hashingTF_1,hashingTF_2,hashingTF_3,hashingTF_4])
model = pipeline.fit(df)
df_5 = model.transform(df)
df_5.select("features_n","features_c","features_l","features_p").show()
job.commit()