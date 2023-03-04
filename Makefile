start-elastic:
	sudo docker start ${id}

process-data:
	cd src/process; python3 process_csv.py normal_data

proccess-data-training:
	cd src/process; python3 process_csv.py training_data

get-exact-match:
	cd src/process; python3 process_csv.py exact_match $(exact_match)

get-fuzzy-match:
	cd src/process; python3 process_csv.py fuzzy_match $(fuzzy_match)
