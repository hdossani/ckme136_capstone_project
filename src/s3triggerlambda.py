import boto3
import json
import os


def lambda_handler(event, context):
    s3_resource = boto3.resource('s3')
    comprehend = boto3.client(service_name='comprehend', region_name='us-east-2')
    firehose_client = boto3.client('firehose',
                                   aws_access_key_id=os.environ['accessKey'],
                                   aws_secret_access_key=os.environ['secretAccessKey']
                                   )
    rawbucket = s3_resource.Bucket('ckme136.twitter.raw')
    if event:
        for file in rawbucket.objects.all():
            print(file.key)
            rawdata = file.get()['Body'].read().decode('utf-8')
            tweets = rawdata.split("\n")
            for tweet in tweets:
                try:
                    tweet_str = json.loads(tweet)
                    tweet_json = json.loads(tweet_str)
                    tweet_text = tweet_json["text"]
                    tweet_lang = tweet_json["lang"]
                    print(tweet_lang + "," + tweet_text)
                    # Determine sentiment
                    sentiment_str = comprehend.detect_sentiment(Text=tweet_text, LanguageCode=tweet_lang)

                    sentiment_json = {"id": tweet_json["id"],
                                      "text": tweet_json["text"],
                                      "sentiment": sentiment_str["Sentiment"],
                                      "positive": sentiment_str["SentimentScore"]["Positive"],
                                      "negative": sentiment_str["SentimentScore"]["Negative"],
                                      "neutral": sentiment_str["SentimentScore"]["Neutral"],
                                      "mixed": sentiment_str["SentimentScore"]["Mixed"]}

                    response = firehose_client.put_record(DeliveryStreamName='twitterSentimentStream',
                                                          Record={
                                                              'Data': json.dumps(sentiment_json) + '\n'
                                                          }
                                                          )
                    # print(response)
                    # Determine entity
                    entity_str = comprehend.detect_entities(Text=tweet_text, LanguageCode=tweet_lang)
                    print(json.dumps(entity_str))
                except json.decoder.JSONDecodeError:
                    print('Error')
    # TODO implement
    return "Hello World"
