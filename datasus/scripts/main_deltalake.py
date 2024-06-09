from minio import Minio
from minio.error import S3Error
from pyspark.sql import SparkSession
import pyspark
from delta import *
import logging

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logging.basicConfig(encoding='utf-8', level=logging.INFO)
    # client = Minio("10.100.100.61:9000", secure=False,
    #                access_key="EsVj1dnneqMciIYcBbhR",
    #                secret_key="oolW2uY06jXIoE2oMNeE8MSgskzuz14HaFYdhEh7"
    #                )

    spark = SparkSession.builder.master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,"
                                       "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.hadoop.fs.s3a.access.key", "XbXYqaZYnHyJt11oUUpH") \
        .config("spark.hadoop.fs.s3a.secret.key", "vgOM9AZqwt9Oa2GPPVPJ4Eizn47HJT2JQN37sJQd") \
        .config("spark.hadoop.fs.s3a.endpoint", "play.min.io:9000") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    logger.info('vai criar o arquivo...')
    # create a delta table format
    spark.range(5).write.format("delta").save("s3a://primeiro/delta2")

    # write_format.save("s3a://datasusdo/delta1")
    logger.info('arquivo criado!')
    logger.info('vai ler do arquivo...')
    # read a delta table on minio oor S3
    spark.read.format("delta").load("s3a://primeiro/delta2").show()
