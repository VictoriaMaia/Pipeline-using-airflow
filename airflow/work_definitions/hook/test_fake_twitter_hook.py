from airflow.providers.http.hooks.http import HttpHook
import requests
import pendulum
import json

class FakeTwitterHookTEST(HttpHook):

    def __init__(self, query, start_time, end_time, conn_id=None):
        self.query = query
        self.start_time = start_time
        self.end_time = end_time
        self.conn_id = conn_id or "fake_twitter_default"

        super().__init__(http_conn_id = self.conn_id)

    def create_url(self) -> str:
        tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

        url = f"{self.base_url}/2/tweets/search/recent?"
        query_and_fields = f"query={self.query}&{tweet_fields}&{user_fields}"
        begin_and_end_time = f"&start_time={self.start_time}&end_time={self.end_time}"

        return url + query_and_fields + begin_and_end_time
    
    def connect_to_endpoint(self, url, session):
        self.log.info(f"URL: {url}")

        req = requests.Request(method="GET", url=url)
        prepare = session.prepare_request(req)

        return self.run_and_check(session, prepare, {})
    
    def paginate(self, url, session) -> list:
        json_responses_list = []
        response = self.connect_to_endpoint(url, session)

        json_response = response.json()
        json_responses_list.append(json_response)

        while "next_token" in json_response.get("meta", {}):
            next_token = json_response["meta"]["next_token"]
            new_page_url = f"{url}&next_token={next_token}"
            
            response = self.connect_to_endpoint(new_page_url, session)
            json_response = response.json()

            json_responses_list.append(json_response)

        return json_responses_list
    
    def run(self) -> list:
        session = self.get_conn()
        url = self.create_url()

        return self.paginate(url, session)
    

if __name__ == "__main__":
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    end_time = pendulum.now().strftime(TIMESTAMP_FORMAT)
    start_time = pendulum.today().subtract(days = -3).strftime(TIMESTAMP_FORMAT)
    query = "datascience"

    for pg in FakeTwitterHookTEST(query, start_time, end_time).run():
        print(json.dumps(pg, indent=4, sort_keys=True))