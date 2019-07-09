from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import json
import boto3

cosumerAPIKey = 'cosumerAPIKey'
consumerAPISecretKey = 'consumerAPISecretKey'
apiAccessToken = 'apiAccessToken'
apiAccessTokenSecret = 'apiAccessTokenSecret'


class TwitterStreamListener(StreamListener):
    def __init__(self, time_limit=60):
        self.start_time = time.time()
        self.limit = time_limit
        super(TwitterStreamListener, self).__init__()

    def on_data(self, data):
        if (time.time() - self.start_time) < self.limit:

            json_data: str = json.dumps(data) + '\n'

            firehose_client = boto3.client('firehose',
                                           aws_access_key_id='aws_access_key_id',
                                           aws_secret_access_key='aws_secret_access_key'
                                           )
            response = firehose_client.put_record(DeliveryStreamName='twitterDeliveryStream',
                                                  Record={
                                                      'Data': json_data
                                                  }
                                                  )
            print(response)
            return True
        else:
            return False


auth = OAuthHandler(cosumerAPIKey, consumerAPISecretKey)
auth.set_access_token(apiAccessToken, apiAccessTokenSecret)
twitterStream = Stream(auth, TwitterStreamListener())
twitterStream.filter(languages=["en", "fr", "es"], track=["car", "food", "music", "sports"])
