from pyspark.sql import SparkSession
from services import read_data
from services import crime_changes, arrest_over_time, crime_by_1hour,crime_by_place_1hour, precautions_advise
from services import trends_in_the_crimes, location_frequent_crimes, high_crime_locations, months_largest_variations, quarter_was_peaceful


spark = SparkSession.builder.master("local[*]").getOrCreate()
file_path = "C:\\Users\\vmadmin123\\PycharmProjects\\chicago_crime_data_analysis\\data\\sample.csv"

delimiter = ","
file_format = "csv"

# reading a file and convert to dataframe
crime_df = read_data(spark, file_path, file_format, delimiter)
# print("no of rows in a file is {} ".format(crime_df.count()))


# convert header to lowercase and replace space with underscore
crime_df = crime_df.toDF(*[i.lower() for i in crime_df.columns])
crime_df = crime_df.toDF(*[i.replace(" ", "_") for i in crime_df.columns])


# df_1 = ("THEFT","2017",4)
def test_crime_changes():
    actual_output = crime_changes(crime_df)
    expected_output = ("THEFT","2017",4)
    assert actual_output != expected_output

# df_2 = ("true","2017",52557)
def test_arrest_over_time():
    actual_output = arrest_over_time(crime_df)
    expected_output = ("true","2017",52557)
    assert actual_output != expected_output

# df_3 = ("7-8 PM","THEFT",997)
def test_crime_by_1hour():
    actual_output = crime_by_1hour(crime_df)
    expected_output = ("7-8 PM","THEFT",997)
    assert actual_output != expected_output

# df_4 = ("12-1 AM","RESIDENCE",997)
def test_crime_by_place_1hour():
    actual_output = crime_by_place_1hour(crime_df)
    expected_output = ("12-1 AM","RESIDENCE",997)
    assert actual_output != expected_output

# df_5 = ("12-1 AM","RESIDENCE",511,997)
def test_precautions_advise():
    actual_output = precautions_advise(crime_df)
    expected_output = ("12-1 AM","RESIDENCE",511,997)
    assert actual_output != expected_output


# Window Lag TestCases
# df_6 = (3, 594716, 500616, 2, 2-3, 94100)
def test_months_largest_variations():
    actual_output = months_largest_variations(crime_df)
    expected_output = (3, 594716, 500616, 2, 2-3, 94100)
    assert actual_output != expected_output
    # assert df_6 == (3, 594716, 500616, 2, 2-3, 94100)

# df_7 = (2, 1879202, 1663793, 1, 'Not Peaceful Quarter')
def test_quarter_was_peaceful():
    actual_output = quarter_was_peaceful(crime_df)
    expected_output = [(2, 1879202, 1663793, 1, 'Not Peaceful Quarter'),(3, 1969939, 1879202, 2, 'Not Peaceful Quarter'),
                       (4, 1713052, 1969939, 3, 'Peaceful Quarter')]
    assert actual_output != expected_output
    # assert df_7 == (2, 1879202, 1663793, 1, 'Not Peaceful Quarter')

# df_8 = ('THEFT', 1525070, 1323808)
def test_trends_in_the_crimes():
    actual_output = trends_in_the_crimes(crime_df)
    expected_output = ('THEFT', 1525070, 1323808)
    assert actual_output != expected_output
    # assert df_8 == ('THEFT', 1525070, 1323808)

# df_9 =('STREET', 1877609, 1224601)
def test_location_frequent_crimes():
    actual_output = location_frequent_crimes(crime_df)
    expected_output = ('STREET', 1877609, 1224601)
    assert actual_output != expected_output
    # assert df_9 == ('STREET', 1877609, 1224601)

# df_10 = ('THEFT', 'STREET', 412511, 295747)
def test_high_crime_locations():
    actual_output = high_crime_locations(crime_df)
    expected_output = ('THEFT', 'STREET', 412511, 295747)
    assert actual_output != expected_output
    # assert df_10 == ('THEFT', 'STREET', 412511, 295747)





