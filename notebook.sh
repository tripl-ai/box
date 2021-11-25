docker rmi -f box:latest
docker build . -t box:latest
docker run --rm \
-it \
-p 8888:8888 \
-e INPUT=/parquet \
-v $(pwd)/tpch/parquet:/parquet \
box:latest jupyter notebook --ip='*' --NotebookApp.token='' --NotebookApp.password=''