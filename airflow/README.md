

## Build Airflow Dev Container

```shell
make build
```

## Start Container

```shell
make run
```

The above command automatically mounts things for you correctly. Currently it
will mount all the dags found in dewey/dags for you. We will likely move that
dag directory into this airflow direcotry in the future.

## See what Airflow is doing

```shell
docker logs -f airflow
```

## Run Airflow commands in the container

```shell
./airflow.sh trigger_dag update_ndc
```


