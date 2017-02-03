

## Start Container

```shell
make run
```

The above command will fetch the latest docker image build for you and then
start it automatically mounting things for you correctly. Currently it
will mount all the dags found in dewey/dags for you. We will likely move that
dag directory into this airflow direcotry in the future.

## See what Airflow is doing

```shell
docker logs -f airflow
```

## Run Airflow commands in the container

`airflow.sh` is provided as a handy way to execute the airflow command inside
your running container.

```shell
./airflow.sh trigger_dag update_ndc
```

## Build Airflow Dev Container

If you would like to build a new container or test changes to the dev
environment, make your changes then:

```shell
make build
```

If you would like to push your chnages up to the registry for everyone else to
use:

```shell
make push
```


