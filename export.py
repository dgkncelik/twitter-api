from elasticsearch import Elasticsearch, exceptions
import json, time, csv

start_time = time.time()
DOMAIN = "ec2-3-237-11-168.compute-1.amazonaws.com"
PORT = 9200
host = str(DOMAIN) + ":" + str(PORT)
client = Elasticsearch(host)

try:
    info = json.dumps(client.info(), indent=4)
    print("Elasticsearch client info():", info)
except exceptions.ConnectionError as err:
    print("\nElasticsearch info() ERROR:", err)
    print("\nThe client host:", host, "is invalid or cluster is not running")
    client = None

if client is not None:
    all_indices = client.indices.get_alias("tweets")
    doc_count = 0
    for num, index in enumerate(all_indices):
        match_all = {
            "size": 100,
            "query": {
                "match_all": {}
            }
        }

        # make a search() request to get all docs in the index
        resp = client.search(
            index=index,
            body=match_all,
            scroll='2s'  # length of time to keep search context
        )

        # keep track of pass scroll _id
        old_scroll_id = resp['_scroll_id']
        csv_file = open('mycsvfile.csv', 'w')
        my_dict = {
            "text": "",
            "id": "",
            "author_id": "",
            "created_at": "",
            "username": "",
            "link": ""
        }
        csv_writer = csv.DictWriter(csv_file, my_dict.keys())
        csv_writer.writeheader()
        # use a 'while' iterator to loop over document 'hits'
        while len(resp['hits']['hits']):

            # make a request using the Scroll API
            resp = client.scroll(
                scroll_id=old_scroll_id,
                scroll='2s'  # length of time to keep search context
            )

            # check if there's a new scroll ID
            if old_scroll_id != resp['_scroll_id']:
                print("NEW SCROLL ID:", resp['_scroll_id'])

            # keep track of pass scroll _id
            old_scroll_id = resp['_scroll_id']

            # print the response results
            print("\nresponse for index:", index)
            print("_scroll_id:", resp['_scroll_id'])
            print('response["hits"]["total"]["value"]:', resp["hits"]["total"]["value"])

            # iterate over the document hits for each 'scroll'
            for doc in resp['hits']['hits']:
                print("\n", doc['_id'], doc['_source'])
                doc_count += 1
                csv_writer.writerow(doc['_source'])
                print("DOC COUNT:", doc_count)
        csv_file.close()
    # print the total time and document count at the end
    print("\nTOTAL DOC COUNT:", doc_count)
# print the elapsed time
print("TOTAL TIME:", time.time() - start_time, "seconds.")
