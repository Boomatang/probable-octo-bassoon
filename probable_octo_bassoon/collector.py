import datetime
import logging
import os
import sys

import kopf
import yaml


def get_logger(logger_name=None):
    if logger_name is None:
        logger_name = "locust_operator"
    logger = logging.getLogger(logger_name)
    logging.basicConfig(
        level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logger


log = get_logger("collector")


@kopf.on.resume(kind="Limitador")
@kopf.on.update(kind="Limitador", field="spec")
def limitador(body, **kargs):
    """Save a copy of the limitador CR on start up and when the spec changes."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    data = dict(body)
    data["metadata"]["managedFields"] = None
    cr_yaml = yaml.dump(data)
    filename = f"limitador/limitador_{timestamp}.yaml"
    with open(filename, "w") as f:
        f.write(cr_yaml)
    log.info("Saved copy of limitador CR")

    return None


@kopf.on.resume(kind="WasmPlugin")
@kopf.on.create(kind="WasmPlugin")
@kopf.on.update(kind="WasmPlugin", field="spec")
def WasmPlugin(name, body, **kargs):
    """Save a copy of the limitador CR on start up and when the spec changes."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    data = dict(body)
    data["metadata"]["managedFields"] = None
    cr_yaml = yaml.dump(data)
    filename = f"wasmplugin/{name}_{timestamp}.yaml"
    with open(filename, "w") as f:
        f.write(cr_yaml)
    log.info("Saved copy of wasmplugin")

    return None


def create_directory():
    if os.path.isdir("limitador"):
        log.info("using existing directory limitador to save the results")
    else:
        log.info("creating directory limitador to save the results")
        os.mkdir("limitador")

    if os.path.isdir("wasmplugin"):
        log.info("using existing directory wasmplugin to save the results")
    else:
        log.info("creating directory wasmplugin to save the results")
        os.mkdir("wasmplugin")


def run():
    create_directory()
    kopf.run(clusterwide=True)


if __name__ == "__main__":
    run()
