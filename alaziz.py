import pandas as pd
import pyspark.sql.functions as f
import pyspark.sql.types as t
import pyspark.ml.feature as mlf
import pyspark.ml.stat as mls
import matplotlib.pyplot as plt
import seaborn as sns


def correlation(df_2019, df_2020):
    corr_matrix_pandas1 = corr_between_delay_on_dep_and_arr(df_2019)
    corr_matrix_pandas2 = corr_between_delay_on_dep_and_arr(df_2020)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 8))
    sns.heatmap(corr_matrix_pandas1, ax=ax1, annot=True)
    sns.heatmap(corr_matrix_pandas2, ax=ax2, annot=True)
    plt.show()


def corr_between_delay_on_dep_and_arr(df):
    selected = df.select("DEP_DEL15", "ARR_DEL15", 'DISTANCE')

    selected = selected.withColumn("DEP_DEL15", f.col("DEP_DEL15").cast(t.IntegerType()))
    selected = selected.withColumn("ARR_DEL15", f.col("ARR_DEL15").cast(t.IntegerType()))
    selected = selected.withColumn("DISTANCE", f.col("DISTANCE").cast(t.IntegerType()))

    sample = selected.sample(fraction=0.5, seed=48)
    sample = sample.dropna(how="any")
    # convert = sample.toPandas().corr()
    #
    vector_assembler = mlf.VectorAssembler(inputCols=sample.columns, outputCol="features")
    data_vector = vector_assembler.setHandleInvalid("skip").transform(sample).select("features")
    data_vector.show()

    matrix = mls.Correlation.corr(data_vector, "features").collect()[0][0]
    corr_matrix = matrix.toArray().tolist()
    columns = ['DEP_DEL15', 'ARR_DEL15', 'DISTANCE']
    return pd.DataFrame(corr_matrix, columns=columns, index=columns)
    # return convert


def corr_between_days_and_cancel(df):
    selected = df.select("DAY_OF_WEEK", "CANCELLED")
    selected = selected.withColumn("DAY_OF_WEEK", f.col("DAY_OF_WEEK").cast(t.StringType()))
    selected = selected.withColumn("CANCELLED", f.col("CANCELLED").cast(t.IntegerType()))
    sample = selected.sample(fraction=0.1, seed=48)

    indexer = mlf.StringIndexer(inputCol="DAY_OF_WEEK", outputCol="Day_of_week_index")
    indexer_model = indexer.fit(sample)
    indexed_df = indexer_model.transform(sample)

    encoder = mlf.OneHotEncoder(inputCols=["Day_of_week_index"], outputCols=["DAY_OF_WEEK_Encoded"])
    encoded_df = encoder.fit(indexed_df).transform(indexed_df)
    encoded_df.show()
    encoded_df = encoded_df.withColumn("DAY_OF_WEEK", f.col("DAY_OF_WEEK").cast(t.IntegerType()))

    selected_columns = ['DAY_OF_WEEK_Encoded']
    selected_df = encoded_df.select(*selected_columns)

    # Assemble the selected columns
    vector_assembler = mlf.VectorAssembler(inputCols=selected_columns, outputCol="corr_vector")
    data_vector = vector_assembler.transform(selected_df).select("corr_vector")
    data_vector.show()
    # Generate correlation matrix
    matrix = mls.Correlation.corr(data_vector, "corr_vector").collect()[0][0]
    corr_matrix = matrix.toArray().tolist()

    # Convert the correlation matrix to a Pandas DataFrame
    columns = ['1', '2', '3', '4', '5', '6']
    return pd.DataFrame(corr_matrix, columns=columns, index=columns)
