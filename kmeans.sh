#!/bin/bash

curl --data-binary @tap-engines/target/tap-engines-0.0.1-SNAPSHOT.jar localhost:8090/jars/tap
curl -d "" 'localhost:8090/contexts/tap-context?num-cpu-cores=4&mem-per-node=512m'


curl --data 'FileReader{inputFile="hdfs://hadoop-m/tmp/kmeans_data.txt",format="CSV",output0="tap:nrdd0"}' \
  'localhost:8090/jobs?appName=tap&classPath=tap.engine.FileReader&context=tap-context&sync=true'

curl --data 'SummaryStats{input0="tap:nrdd0"}' \
  'localhost:8090/jobs?appName=tap&classPath=tap.engine.SummaryStats&context=tap-context&sync=true'

curl --data "ClusterAnalysis { \
  method = \"KMeans\", \
  input0 = \"tap:nrdd0\", \
  numClusters = 2, \
  numIterations = 10 \
}" \
'localhost:8090/jobs?appName=tap&classPath=tap.engine.ClusterAnalysis&context=tap-context&sync=true'

curl --data "KMeansPredict { \
  input0 = \"tap:nrdd0\", \
  output0 = \"tap:nrdd1\" \
}" \
'localhost:8090/jobs?appName=tap&classPath=tap.engine.KMeansPredict&context=tap-context&sync=true'

curl --data 'TakeSample{input0="tap:nrdd1",count=100,seed=12345}' \
  'localhost:8090/jobs?appName=tap&classPath=tap.engine.TakeSample&context=tap-context&sync=true'

curl --data 'CatN{input0="tap:nrdd1",n=100}' \
  'localhost:8090/jobs?appName=tap&classPath=tap.engine.CatN&context=tap-context&sync=true'


curl -X DELETE 'localhost:8090/contexts/tap-context'
