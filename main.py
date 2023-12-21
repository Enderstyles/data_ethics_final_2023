from pyspark.sql import SparkSession

import alaziz as a
import zharas as z
import yernur as y


def main():
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel(logLevel="WARN")
    df_2019 = spark.read.csv("../Jan_2019_ontime.csv", header=True)
    df_2020 = spark.read.csv("../Jan_2020_ontime.csv", header=True)

    y.means(df_2019)
    z.distribution_beta(df_2019, df_2020)
    a.correlation(df_2019, df_2020)


if __name__ == "__main__":
    main()
