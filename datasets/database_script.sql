CREATE DATABASE ckme136;


CREATE EXTERNAL TABLE ckme136.tweet_sentiments (
tweetid BIGINT,
text STRING,
sentiment STRING,
positive DOUBLE,
negative DOUBLE,
neutral DOUBLE,
mixed DOUBLE
) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://ckme136.capstone.twitter/sentiment'

CREATE EXTERNAL TABLE ckme136.tweet_raw (
tweetid BIGINT,
text STRING
) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://ckme136.capstone.twitter/raw'


CREATE EXTERNAL TABLE ckme136.tweet_entities (
tweetid BIGINT,
entity STRING,
type STRING,
score DOUBLE
) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://ckme136.twitter/entity';
