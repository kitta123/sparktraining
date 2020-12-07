from pyspark.sql import SparkSession
from services import read_data
import pandas as pd
from services import crime_changes, arrest_over_time, get_1hr_interval, crime_by_1hour,crime_by_place_1hour, precautions_advise
from services import trends_in_the_crimes, location_frequent_crimes, high_crime_locations, months_largest_variations, quarter_was_peaceful


spark = SparkSession.builder.master("local[*]").getOrCreate()
file_path = "C:\\Users\\vmadmin123\\PycharmProjects\\chicago_crime_data_analysis\\data\\Crimes_-_2001_to_Present.csv"

# crime_df = pd.read_csv(file_path)
# print(crime_df)

delimiter = ","
file_format = "csv"

# reading a file and convert to dataframe
crime_df = read_data(spark, file_path, file_format, delimiter)
# print("now of rows in a file is {} ".format(crime_df.count()))

# convert header to lowercase and replace space with underscore
crime_df = crime_df.toDF(*[i.lower() for i in crime_df.columns])
crime_df = crime_df.toDF(*[i.replace(" ", "_") for i in crime_df.columns])
# print(crime_df.show())

# crime changes over a time
# crime_changes_df = crime_changes(crime_df)
# print(crime_changes_df.count())

# crime changes over a time
# arrest_changes_df = arrest_over_time(crime_df)
# print(arrest_changes_df.show())


#What is the most crimes happens during 1 Hr interval
#crime_count_in_hour = crime_by_1hour(crime_df)
#print(crime_count_in_hour.show())

#Where the most crime happens during 1 Hr interval
# crime_place_in_hour = crime_by_place_1hour(crime_df)
# print(crime_place_in_hour.show())

#precautions_advise
#precautions = precautions_advise(crime_df)
#print(precautions.show())


#Windows functions

#months_largest_variations
#months_crime = months_largest_variations(crime_df)
#print(months_crime.show())

#quarter_was_peaceful
#quarter_peaceful = quarter_was_peaceful(crime_df)
#print(quarter_peaceful.show())

#trends of crimes
# trends_crimes_commited = trends_in_the_crimes(crime_df)
# print(trends_crimes_commited.show())

# location_frequent_crimes_commited.
# location_frequent_crimes_commited = location_frequent_crimes(crime_df)
# print(location_frequent_crimes_commited.show())

# Are there certain high crime locations for certain crime
certain_high_crime_locations = high_crime_locations(crime_df)
print(certain_high_crime_locations.show())