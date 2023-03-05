start-elastic:
	sudo docker start ${id}

process-data:
	cd src/library; python3 processing.py normal_data

process-data-training:
	cd src/library; python3 processing.py training_data

get-exact-match:
	cd src/library; python3 processing.py exact_match $(exact_match)

get-fuzzy-match:
	cd src/library; python3 processing.py fuzzy_match $(fuzzy_match)
