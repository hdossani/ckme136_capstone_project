import boto3
import json
import os
import re


def lambda_handler(event, context):
    s3_resource = boto3.resource('s3')
    comprehend = boto3.client(service_name='comprehend', region_name='us-east-1')
    firehose_client = boto3.client('firehose',
                                   aws_access_key_id=os.environ['accessKey'],
                                   aws_secret_access_key=os.environ['secretAccessKey']
                                   )
    rawbucket = s3_resource.Bucket('ckme136.capstone.twitter')
    if event:
        for file in rawbucket.objects.all():
            rawdata = file.get()['Body'].read().decode('utf-8')
            tweets = rawdata.split("\n")
            for tweet in tweets:
                try:
                    tweet_str = json.loads(tweet)
                    tweet_text = tweet_str.get("text", "Neutral Text")
                    tweet_id = tweet_str.get("tweetid", -999)
                    tweet_lang = "en"
                    # clean up the text to remove username and RT
                    tweet_text = re.sub(r"RT @[\w:]*", "", tweet_text)
                    tweet_text = re.sub(r"@[\w]*", "", tweet_text)
                    # remove links
                    tweet_text = re.sub(r"(http|https)(.*)(?<!')(?<!\")", "", tweet_text)
                    # remove emojis, symbols
                    tweet_text = re.sub(r"[^\x00-\x7F]+", "", tweet_text)
                    # print(tweet_text)
                    # Determine sentiment
                    sentiment_str = comprehend.detect_sentiment(Text=tweet_text, LanguageCode=tweet_lang)
                    sentiment_json = dict(tweetid=tweet_id, text=tweet_text, sentiment=sentiment_str["Sentiment"],
                                          positive=sentiment_str["SentimentScore"]["Positive"],
                                          negative=sentiment_str["SentimentScore"]["Negative"],
                                          neutral=sentiment_str["SentimentScore"]["Neutral"],
                                          mixed=sentiment_str["SentimentScore"]["Mixed"])
                    sentimentresponse = firehose_client.put_record(DeliveryStreamName='twitterSentimentStream',
                                                                   Record={
                                                                       'Data': json.dumps(sentiment_json) + '\n'
                                                                   }
                                                                   )

                    # Determine entity
                    entity_str = comprehend.detect_entities(Text=tweet_text, LanguageCode=tweet_lang)
                    for entity in entity_str["Entities"]:
                        entity_json = {
                            "tweetid": tweet_id,
                            "entity": entity["Text"],
                            "type": entity["Type"],
                            "score": entity["Score"]}
                        responseentity = firehose_client.put_record(DeliveryStreamName='twitterEntityStream',
                                                                    Record={
                                                                        'Data': json.dumps(entity_json) + '\n'
                                                                    }
                                                                    )
                        print(tweet_text)
                except json.decoder.JSONDecodeError as e:
                    print('Error' + str(e))
    return ""
