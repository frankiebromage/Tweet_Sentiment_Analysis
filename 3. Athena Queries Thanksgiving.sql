#Load Table into Athena from Bucket

DROP TABLE IF EXISTS thanksgiving_key_words;
CREATE EXTERNAL TABLE IF NOT EXISTS `ptb3`.`thanksgiving_key_words` (
  `id` string,
  `name` string,
  `username` string,
  `tweet` string,
  `followers_count` int,
  `location` string,
  `geo` string,
  `created_at` string,
  `tweet_clean` string,
  `filtered` array < string >,
  `prediction` double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://ptp3-frankiebromage/bigdataproject/thanksgiving/predicted_data.parquet/'
TBLPROPERTIES ('classification' = 'parquet');

#Create Table with Key Words, Totals, Username and Date from first table so I can create word clouds with key words

DROP TABLE IF EXISTS words_cloud_sentiment;
CREATE TABLE words_cloud_sentiment AS (
    SELECT word,username,
    CAST(SUBSTR(created_at, 27, 4) || '-' ||
        (CASE WHEN SUBSTR(created_at, 5, 3) = 'Dec' THEN '12'
            WHEN SUBSTR(created_at, 5, 3) = 'Nov' THEN '11'
            END)||
        '-' || SUBSTR(created_at, 9, 2) AS date) AS date_, (CASE WHEN prediction = 0 THEN 'Negative'
        WHEN prediction = 1 THEN 'Positive'
        END) AS Sentiment,
        COUNT(word) AS TOTAL
    FROM thanksgiving_key_words, UNNEST(filtered) AS t(word)
    GROUP BY username, CAST(SUBSTR(created_at, 27, 4) || '-' ||
        (CASE WHEN SUBSTR(created_at, 5, 3) = 'Dec' THEN '12'
            WHEN SUBSTR(created_at, 5, 3) = 'Nov' THEN '11'
            END)||
        '-' || SUBSTR(created_at, 9, 2) AS date), (CASE WHEN prediction = 0 THEN 'Negative'
        WHEN prediction = 1 THEN 'Positive'
        END), word
    ORDER BY TOTAL DESC);

#Create Table with cleaned dates and sentiment
#To create datetime I only need to use months November and December because this is the period of all the tweets.
#I filter out rows where year does not equal 2022, because some rows have errors with data not in the right place.

DROP TABLE IF EXISTS thanksgiving_dates;
CREATE TABLE thanksgiving_dates AS
(SELECT id,
        tweet,
        name,
        username,
        followers_count,
        location,
        geo,
        tweet_clean,
        prediction,
        (CASE WHEN prediction = 0 THEN 'Negative'
        WHEN prediction = 1 THEN 'Positive'
        END) AS Sentiment,
        SUBSTR(created_at, 1, 3) AS day_of_week,
        SUBSTR(created_at, 5, 3) AS month_,
        SUBSTR(created_at, 9, 2) AS day_,
        SUBSTR(created_at, 12, 8) AS time_,
        SUBSTR(created_at, 27, 4) AS year_,
        CAST(SUBSTR(created_at, 27, 4) || '-' ||
            (CASE WHEN SUBSTR(created_at, 5, 3) = 'Dec' THEN '12'
            WHEN SUBSTR(created_at, 5, 3) = 'Nov' THEN '11'
            END)||
            '-' || SUBSTR(created_at, 9, 2)|| ' ' ||SUBSTR(created_at, 12, 8) AS timestamp) AS date_time,
        CAST(SUBSTR(created_at, 27, 4) || '-' ||
            (CASE WHEN SUBSTR(created_at, 5, 3) = 'Dec' THEN '12'
            WHEN SUBSTR(created_at, 5, 3) = 'Nov' THEN '11'
            END)||
            '-' || SUBSTR(created_at, 9, 2) AS date) AS date_,
            SUBSTR(created_at, 12, 2) AS hour
FROM thanksgiving_key_words
WHERE SUBSTR(created_at, 27, 4) = '2022');