.PHONY: build

build: 
	@docker build . -t=spark-standalone-cluster-base --file=./docker/cluster-base/Dockerfile
	@docker build . -t=spark-standalone-spark-base --file=./docker/spark-base/Dockerfile
	@docker build . -t=spark-standalone-spark-master --file=./docker/spark-master/Dockerfile
	@docker build . -t=spark-standalone-spark-worker --file=./docker/spark-worker/Dockerfile
	@docker build . -t=spark-standalone-jupyterlab --file=./docker/jupyterlab/Dockerfile
