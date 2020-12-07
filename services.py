from pyspark.sql import Window
from pyspark.sql.functions import split, udf, concat, col, lit, month, to_date, quarter, when
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


def read_data(spark, file_path, file_format, delimiter):
    file_df = spark.read.format(file_format).option("header", "true").option("inferSchema", "true").option("delimiter", delimiter).load(file_path)
    return file_df


def crime_changes(input_df):
    input_df = input_df.groupBy("primary_type", "year").count()
    input_df.orderBy("primary_type","year")

    # input_df = input_df.toPandas()
    # #Graph plot.
    # plt.figure(figsize=(16, 9))
    # sns.lineplot(x=input_df["year"], y=input_df["count"], data=input_df)
    # plt.show()
    return input_df

def arrest_over_time(input_df):
    input_df = input_df.groupBy("arrest", "year").count()
    input_df.orderBy("arrest", "year")

    #Graph plot.
    # input_df = input_df.toPandas()
    # plt.figure(figsize=(16, 9))
    # sns.lineplot(x=input_df["year"], y=input_df["count"], data=input_df)
    # plt.show()
    return input_df


def get_time(input_time, zone):
    hour = int(input_time.split(":")[0])
    one_hr_interval = ""
    if (hour == 12):
        one_hr_interval = str(hour) + "-" + str(1) + " " + zone
    else:
        if (hour == 11):
            if (zone == "AM"):
                one_hr_interval = str(hour) + "-" + str(hour + 1) + " " + "PM"
            if (zone == "PM"):
                one_hr_interval = str(hour) + "-" + str(hour + 1) + " " + "AM"
        else:
            one_hr_interval = str(hour) + "-" + str(hour + 1) + " " + zone
    return one_hr_interval


def get_1hr_interval(input_time):
    time = input_time.split(" ")[0]
    zone = input_time.split(" ")[1]
    return get_time(time, zone)

interval_udf = udf(get_1hr_interval)


def crime_by_1hour(crime_df):
    split_col = split(crime_df['date'], ' ')
    crime_df1 = crime_df.withColumn('date1', split_col[0])
    crime_df2 = crime_df1.withColumn('time', split_col[1])
    crime_df3 = crime_df2.withColumn('time_zone', split_col[2])
    #crime_df3.show()
    crime_df4 = crime_df3.withColumn("time_final", concat(col("time"), lit(" "), col("time_zone")))
    #crime_df4.show()
    crime_df5 = crime_df4.withColumn("one_hr_interval", interval_udf(crime_df4["time_final"]))
    #crime_df5.show()
    crime_df6 = crime_df5.groupBy("one_hr_interval", "primary_type").count().orderBy(col("count").desc())
    #crime_df6.show()

    # #Graph plot.
    # crime_df6 = crime_df6.toPandas()
    # plt.figure(figsize=(16, 9))
    # sns.lineplot(x=crime_df6["one_hr_interval"], y=crime_df6["count"], data=crime_df6)
    # plt.show()
    return crime_df6

def crime_by_place_1hour(crime_df):
    split_col = split(crime_df['date'], ' ')
    crime_df1 = crime_df.withColumn('date1', split_col[0])
    crime_df2 = crime_df1.withColumn('time', split_col[1])
    crime_df3 = crime_df2.withColumn('time_zone', split_col[2])
    #crime_df3.show()
    crime_df4 = crime_df3.withColumn("time_final", concat(col("time"), lit(" "), col("time_zone")))
    #crime_df4.show()
    crime_df5 = crime_df4.withColumn("one_hr_interval", interval_udf(crime_df4["time_final"]))
    #crime_df5.show()
    crime_df6 = crime_df5.groupBy("one_hr_interval", "location_description").count().orderBy(col("count").desc())
    #crime_df6.show()


    # #Graph plot.
    # crime_df6 = crime_df6.toPandas()
    # plt.figure(figsize=(16, 9))
    # sns.barplot(x=crime_df6["location_description"], y=crime_df6["count"], data=crime_df6)
    # plt.show()
    return crime_df6

def precautions_advise(precaurion_df):
    split_col = split(precaurion_df['date'], ' ')
    crime_df1 = precaurion_df.withColumn('date1', split_col[0])
    crime_df2 = crime_df1.withColumn('time', split_col[1])
    crime_df3 = crime_df2.withColumn('time_zone', split_col[2])
    #crime_df3.show()
    crime_df4 = crime_df3.withColumn("time_final", concat(col("time"), lit(" "), col("time_zone")))
    #crime_df4.show()
    crime_df5 = crime_df4.withColumn("one_hr_interval", interval_udf(crime_df4["time_final"]))
    #crime_df5.show()
    crime_df6 = crime_df5.groupBy("one_hr_interval", "location_description", "beat").count().orderBy(col("count").desc())
    #crime_df6.show()


    # #Graph plot.
    # crime_df6 = crime_df6.toPandas()
    # plt.figure(figsize=(16, 9))
    # sns.barplot(x=crime_df6["beat"], y=crime_df6["count"], data=crime_df6)
    # plt.show()
    return crime_df6

#Windows Lag
#Which contiguous months show largest variation in crime?
def months_largest_variations(month_df):
    split_col = split(month_df['Date'], ' ')
    df1 = month_df.withColumn('date1', split_col[0])

    date_df = df1.withColumn("ut", to_date(col("date1"), 'MM/dd/yyyy'))
    date_df_month = date_df.withColumn("month", month(col("ut")))
    crime_by_month_df = date_df_month.groupBy("month").count()

    window_spec = Window().partitionBy().orderBy(F.col("month"))
    lag_df = crime_by_month_df.withColumn("prev_month_crime_count", F.lag(col("count"), 1).over(window_spec))
    lag_df = lag_df.withColumn("prev_month", F.lag(col("month"), 1, 0).over(window_spec))
    lag_df = lag_df.withColumn("contigous_month", concat(col("prev_month"), lit("-"), col("month")))
    # lag_df = lag_df.drop("prev_month","month")
    month_diff_crimes = lag_df.withColumn("diff_crime_count", col("count") - col("prev_month_crime_count")).orderBy(col("diff_crime_count").desc())


    # #Graph plot.
    # month_diff_crimes = month_diff_crimes.toPandas()
    # plt.figure(figsize=(16, 9))
    # sns.barplot(x=month_diff_crimes["month"], y=month_diff_crimes["diff_crime_count"], data=month_diff_crimes)
    # plt.show()
    return month_diff_crimes

#Which quarter was the most peaceful compared to the previous quarter?
def quarter_was_peaceful(quarter_df):
    split_col = split(quarter_df['Date'], ' ')
    df1 = quarter_df.withColumn('date1', split_col[0])

    date_df = df1.withColumn("ut", to_date(col("date1"), 'MM/dd/yyyy'))
    quarter_df = date_df.withColumn("month", month(col("ut")))

    quarter_df = quarter_df.withColumn("quarter", quarter(col("ut")))
    crimes_by_quarter = quarter_df.groupBy("quarter").count()

    window_spec = Window().partitionBy().orderBy(F.col("quarter"))
    lag_df = crimes_by_quarter.withColumn("prev_quarter_crime_count", F.lag(col("count"), 1).over(window_spec))
    lag_df = lag_df.withColumn("prev_quarter", F.lag(col("quarter"), 1, 0).over(window_spec))
    #lag_df = lag_df.drop("prev_quarter")
    crimes_by_quarter = lag_df.withColumn("peaceful_quarter", when(col("count") < col("prev_quarter_crime_count"),"Peaceful Quarter")
                                          .otherwise("Not Peaceful Quarter")).where(col("quarter")!=1)\
        .orderBy(col("quarter"))


    # #Graph plot.
    # crimes_by_quarter = crimes_by_quarter.toPandas()
    # plt.figure(figsize=(10, 5))
    # sns.barplot(x=crimes_by_quarter["peaceful_quarter"], y=crimes_by_quarter["quarter"], data=crimes_by_quarter)
    # plt.show()
    # return crimes_by_quarter

#Are there any trends in the crimes being committed?
def trends_in_the_crimes(trend_df):
    trend_df = trend_df.groupBy("primary_type").count()
    window_spec = Window().partitionBy().orderBy(F.col("count"))
    trend_df = trend_df.withColumn("trend_crimes", F.lag(col("count")).over(window_spec)).orderBy(col("count").desc())

    # #Graph plot.
    # trend_df = trend_df.toPandas()
    # plt.figure(figsize=(10, 5))
    # sns.barplot(x=trend_df["primary_type"], y=trend_df["count"], data=trend_df)
    # plt.show()
    return trend_df

#Which locations are these frequent crimes being committed to?
def location_frequent_crimes(location_df):
    location_df = location_df.groupBy("location_description").count()
    window_spec = Window().partitionBy().orderBy(F.col("count"))
    location_df = location_df.withColumn("location_frequent_crimes", F.lag(col("count")).over(window_spec)).orderBy(col("count").desc())

    # #Graph plot.
    # location_df = location_df.toPandas()
    # plt.figure(figsize=(10, 5))
    # sns.barplot(x=location_df["location_description"], y=location_df["count"], data=location_df)
    # plt.show()
    return location_df


#Are there certain high crime locations for certain crime
def high_crime_locations(hight_crime_df):
    high_crime_df = hight_crime_df.groupBy("primary_type", "location_description").count()
    window_spec = Window().partitionBy('location_description').orderBy(F.col("count"))
    high_crime_df = high_crime_df.withColumn("high_crime_count", F.lag(col("count")).over(window_spec)).orderBy(col("count").desc())

    # #Graph plot.
    # high_crime_df = high_crime_df.toPandas()
    # plt.figure(figsize=(10, 5))
    # sns.barplot(x=high_crime_df["location_description"], y=high_crime_df["count"], data=high_crime_df)
    # plt.show()
    return high_crime_df
