import os

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
        self.es_client = Elasticsearch("http://ec2-3-237-11-168.compute-1.amazonaws.com:9200/")
        # self.mapping = '''
        # {
        #   "mappings":{
        #     "logs_june":{
        #       "_timestamp":{
        #         "enabled":"true"
        #       },
        #       "properties":{
        #         "logdate":{
        #           "type":"date",
        #           "format":"dd/MM/yyy HH:mm:ss"
        #         }
        #       }
        #     }

        #   }
        # }'''
        self.es_client.indices.create(index="tweets", ignore=400)

    def insert_es(self, doc):
        _doc = {
            "text": doc["data"]["text"],
            "id": doc["data"]["id"],
            "author_id": doc["data"]["author_id"],
            "created_at": doc["data"]["created_at"],
            "username": "",
            "link": ""
        }

        if "includes" in doc and "users" in doc["includes"]:
            for u in doc["includes"]["users"]:
                if u["id"] == _doc["author_id"]:
                    _doc["username"] = u["username"]
                    _doc["link"] = "https://twitter.com/%s/status/%s/" % (u["username"], doc["data"]["id"])

        resp = self.es_client.index(index="tweets", body=_doc)
        print(resp["result"])
        print("inserted: %s" % _doc)

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
        if response.status_code != 201 and response.status_code != 200:
            print(
                "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
            )
        print(json.dumps(response.json()))

    def undefine_all_rules(self):
        rules = self.get_all_rules()
        if "data" not in rules:
            return []

        ids = list(map(lambda rule: rule["id"], rules["data"]))
        payload = {"delete": {"ids": ids}}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            headers=self.auth,
            json=payload
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        print(json.dumps(response.json()))

    def start_stream(self):
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream?user.fields=username&expansions=author_id&tweet.fields=created_at", headers=self.auth, stream=True,
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
                # print(json.dumps(json_response, sort_keys=True))
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
        self.undefine_all_rules()
        self.define_all_rules()
        self.start_stream()


if __name__ == '__main__':
    ts = TwitterScaper()
    ts.run()
