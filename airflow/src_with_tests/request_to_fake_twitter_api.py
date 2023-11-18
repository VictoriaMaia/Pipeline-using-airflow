import pendulum
import requests
import json


TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

end_time = pendulum.now().strftime(TIMESTAMP_FORMAT)
start_time = pendulum.today().subtract(days = -3).strftime(TIMESTAMP_FORMAT)

query = "datascience"

tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

url = "https://labdados.com/2/tweets/search/recent?"
query_and_fields = f"query={query}&{tweet_fields}&{user_fields}"
begin_and_end_time = f"&start_time={start_time}&end_time={end_time}"

url_raw = url + query_and_fields + begin_and_end_time

response = requests.get(url_raw)

json_response = response.json()
print(json.dumps(json_response, indent=4, sort_keys=True))

while "next_token" in json_response.get("meta",{}):
    next_token = json_response["meta"]["next_token"]
    url = f"{url_raw}&next_token={next_token}"
    response = requests.get(url)
    json_response = response.json()
    print(json.dumps(json_response, indent=4, sort_keys=True))