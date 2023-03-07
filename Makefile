start-new-container:
	sudo docker run --name elasticsearch --net elastic -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" -t docker.elastic.co/elasticsearch/elasticsearch:8.6.2

start-elastic:
	sudo docker start ${id}

process-data:
	cd src/library; python3 processing.py process_data

get-exact-match:
	cd src/library; python3 processing.py exact_match $(exact_match)

get-fuzzy-match:
	cd src/library; python3 processing.py fuzzy_match $(fuzzy_match)

delete-index:
	cd src/library; python3 processing.py delete_index
