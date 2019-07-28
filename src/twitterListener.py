from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import json
import boto3
import credentials


class TwitterStreamListener(StreamListener):
    def __init__(self, time_limit=60):
        self.start_time = time.time()
        self.limit = time_limit
        self.file = open("/Users/hirendossani/git/ckme136_capstone_project/src/resources/tweets.json", "w+")
        super(TwitterStreamListener, self).__init__()

    def on_data(self, data):
        if (time.time() - self.start_time) < self.limit:
            try:
                rawtweet = json.loads(data)
                tweet_text = rawtweet.get("text", "Neutral Text")
                tweet_id = rawtweet.get("id", -999)

                json_data = {
                            "tweetid": tweet_id,
                            "text": tweet_text
                             }
                with open("/Users/hirendossani/git/ckme136_capstone_project/src/resources/tweets.json", "a") as tf:
                    tf.write(json.dumps(json_data) + '\n')

                firehose_client = boto3.client('firehose',
                                           aws_access_key_id=credentials.AWS_ACCESS_KEY_ID,
                                           aws_secret_access_key=credentials.AWS_SECRET_ACCESS_KEY
                                           )
                response = firehose_client.put_record(DeliveryStreamName='twitterDeliverStream',
                                                  Record={
                                                      'Data': json.dumps(json_data) + '\n'
                                                  }
                                                  )
                print(rawtweet)
                return True
            except ValueError as e:
                print("Error")
                return True
        else:
            return False


auth = OAuthHandler(credentials.CONSUMERAPIKEY, credentials.CONSUMERAPISECRETKEY)
auth.set_access_token(credentials.APIACCESSTOKEN, credentials.APIACCESSTOKENSECRET)
twitterStream = Stream(auth, TwitterStreamListener())
twitterStream.filter(languages=["en"], track=['Trudeau'])
