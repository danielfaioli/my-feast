.PHONY: build

repository := danielfaioli
version := 0.1.0

build: 
	@docker build . -t=spark-standalone-cluster-base:latest -t=spark-standalone-cluster-base:${version}  --file=./docker/cluster-base/Dockerfile
	@docker build . -t=spark-standalone-spark-base:latest -t=spark-standalone-spark-base:${version} --file=./docker/spark-base/Dockerfile
	@docker build . -t=spark-standalone-spark-master:latest -t=spark-standalone-spark-master${version} --file=./docker/spark-master/Dockerfile
	@docker build . -t=spark-standalone-spark-worker:latest -t=spark-standalone-spark-worker:${version} --file=./docker/spark-worker/Dockerfile
	@docker build . -t=spark-standalone-jupyterlab:latest -t=spark-standalone-jupyterlab:${version} --file=./docker/jupyterlab/Dockerfile

# push:
# 	TODO: Add automation to push images to docker hub.