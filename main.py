import requests
import json
from elasticsearch import Elasticsearch


class TwitterScaper(object):
    def __init__(self):
        self.credential_file = "credentials.json"
        self.rule_file = "rule_file.json"
        self.credentials = {}
        self.auth = {}
        self.rules = {}
        self.es_client = Elasticsearch("http://elasticsearch:9200/")

    def insert_es(self, doc):
        resp = self.es_client.index(index="tweets", document=doc)
        print(resp["result"])

    def authenticate(self):
        self.auth = {
            "Authorization": f"Bearer {self.credentials['bearer_token']}",
            "User-Agent": "v2FilteredStreamPython"
        }

    def get_all_rules(self):
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream/rules", headers=self.auth
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
            )
        print(json.dumps(response.json()))
        return response.json()

    def define_all_rules(self):
        # You can adjust the rules if needed
        # sample_rules = [
        #     {"value": "dog has:images", "tag": "dog pictures"},
        #     {"value": "cat has:images -grumpy", "tag": "cat pictures"},
        # ]
        payload = {"add": self.rules["rules"]}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            headers=self.auth,
            json=payload,
        )
        if response.status_code != 201:
            raise Exception(
                "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
            )
        print(json.dumps(response.json()))

    def start_stream(self):
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream", headers=self.auth, stream=True,
        )
        print(response.status_code)
        if response.status_code != 200:
            raise Exception(
                "Cannot get stream (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        for response_line in response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                print(json.dumps(json_response, indent=4, sort_keys=True))
                self.insert_es(json_response)

    def read_credentials(self):
        credential_file = open(self.credential_file, "r")
        self.credentials = json.loads(credential_file.read())
        credential_file.close()

    def read_rules(self):
        rules_file = open(self.rule_file, "r")
        self.rules = json.loads(rules_file.read())
        rules_file.close()

    def run(self):
        self.read_credentials()
        self.read_rules()
        self.authenticate()
        self.define_all_rules()
        self.start_stream()


if __name__ == '__main__':
    ts = TwitterScaper()
    ts.run()
