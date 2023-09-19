# Tweet_Sentiment_Analysis
An end to end data pipeline project analyzing the sentiments of tweets about Thanksgiving.

In this project, tweets were scraped to an S3 bucket in AWS using Kinesis Firehose

1. I utilized PySpark using the Databricks environment to clean and transform the raw tweet data. I then used vader to create sentiments for a portion of the tweets, and saved these back into an S3 bucket.
2. I used the cleaned tweets sample to create a logistic regression model to predict the sentiment of the remaining tweets.
3. Using Amazon Athena, I used SQL to create tables that were used to create a dashboard using QuickSight.
