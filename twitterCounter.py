from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
global count
# Go to http://dev.twitter.com and create an app.
# The consumer key and secret will be generated for you after
consumer_key="XQ3goPVzBAsRmk0ku0VEg"
consumer_secret="Y5TKarbUU93xuQYxSXENOu60SV1DihJ1KYYXR3Pqbw"

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token=" 310790307-RMhk3JyEl2rcqpzKLZL1W7UzvbJgp7NXE7CKQevP"
access_token_secret=" DSbYuKJbkBai9w0E8P0vRv3dKFhCBiah9Hb3lptwsgoRi"
count = 0

class StdOutListener(StreamListener):
	def on_data(self, data):
			global count
			count += 1
			print count
			return True
    def on_error(self, status):
			print status

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    
    stream = Stream(auth, l)
    stream.filter(track=['$aapl'])

