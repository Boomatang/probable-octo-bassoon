# probable-octo-bassoon

Usage order
1. set up the cluster
1. run the collector
1. create the cluster load
1. generate the graphs

## Set up the cluster
```sh
kubectl apply -k resources/httpbin
```

The resources that are used for the kuadrant-operator deployment may need to be bumped to a higher level.

## Running the collector
```sh
python probable_octo_bassoon/collector.py config_sample.toml
```

## Creating the cluster load
```sh
python -m probable_octo_bassoon config_sample.toml --pause
```
The `--pause` stops the run before the cleanup starts.

## Generate the graphs
```sh
python probable_octo_bassoon/graph.py probable_octo_bassoon/database.sqlite
```

