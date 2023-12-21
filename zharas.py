import pyspark.sql.functions as f


def distribution_beta(df_2019, df_2020):
    summary_df1 = df_2019.describe()
    summary_df2 = df_2020.describe()

    # 2. Missing Values
    missing_values_df1 = df_2019.select([f.count(f.when(f.isnan(c) | f.isnull(c), c)).alias(c) for c in df_2019.columns])
    missing_values_df2 = df_2020.select([f.count(f.when(f.isnan(c) | f.isnull(c), c)).alias(c) for c in df_2020.columns])

    # 3. Unique Values
    unique_values_df1 = df_2019.agg(*(f.countDistinct(f.col(c)).alias(c) for c in df_2019.columns))
    unique_values_df2 = df_2020.agg(*(f.countDistinct(f.col(c)).alias(c) for c in df_2020.columns))

    # Display or Save the results
    summary_df1.show()
    summary_df2.show()

    missing_values_df1.show()
    missing_values_df2.show()

    unique_values_df1.show()
    unique_values_df2.show()
