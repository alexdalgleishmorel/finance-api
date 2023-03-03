start-elastic:
	sudo docker start ffb39ba3d4a792656489a83ef8f89b33a7d7f225cbfb282efe7f55b64d397e31

stop-elastic:
	sudo docker stop ffb39ba3d4a792656489a83ef8f89b33a7d7f225cbfb282efe7f55b64d397e31

process-data:
	cd src/process; python3 process_csv.py

proccess-data-training:
	cd src/process; python3 process_csv.py training
