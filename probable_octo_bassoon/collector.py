import logging
import sys
from datetime import datetime

import kopf
from pony.orm import Database, Optional, PrimaryKey, db_session

db = Database()


class Event(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Optional(str)
    namespace = Optional(str)
    kind = Optional(str)
    timestamp = Optional(datetime, default=lambda: datetime.now())
    changed = Optional(str)
    enfocred = Optional(str, nullable=True)
    accepted = Optional(str, nullable=True)
    ready = Optional(str, nullable=True)


db.bind(provider="sqlite", filename="database.sqlite", create_db=True)
db.generate_mapping(create_tables=True)


def get_logger(logger_name=None):
    if logger_name is None:
        logger_name = "locust_operator"
    logger = logging.getLogger(logger_name)
    logging.basicConfig(
        level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logger


log = get_logger("collector")


@kopf.on.resume(kind="AuthPolicy", param="AuthPolicy")
@kopf.on.create(kind="AuthPolicy", param="AuthPolicy")
@kopf.on.update(kind="AuthPolicy", field="status", param="AuthPolicy")
@kopf.on.resume(kind="RateLimitPolicy", param="RateLimitPolicy")
@kopf.on.create(kind="RateLimitPolicy", param="RateLimitPolicy")
@kopf.on.update(kind="RateLimitPolicy", field="status", param="RateLimitPolicy")
@kopf.on.resume(kind="Limitador", param="Limitador")
@kopf.on.create(kind="Limitador", param="Limitador")
@kopf.on.update(kind="Limitador", field="status", param="Limitador")
@db_session
def status_watch(name, namespace, status, param, **kargs):
    policies = ["RateLimitPolicy", "AuthPolicy"]
    # e = Event(name=name, namespace=namespace, kind=param, change="spec")
    e = Event()
    e.name = name
    e.namespace = namespace
    e.kind = param
    e.changed = "status"

    if "conditions" not in status:
        log.error("conditions not in the status block")
        return None

    if param in policies:
        for condition in status["conditions"]:
            if condition["type"] == "Accepted":
                e.accepted = str(condition["status"])
            elif condition["type"] == "Enforced":
                e.enfocred = str(condition["status"])

    for condition in status["conditions"]:
        if condition["type"] == "Ready":
            e.ready = str(condition["status"])
    log.info(e)
    return None


@kopf.on.resume(kind="WasmPlugin", param="WasmPlugin")
@kopf.on.create(kind="WasmPlugin", param="WasmPlugin")
@kopf.on.update(kind="WasmPlugin", field="spec", param="WasmPlugin")
@kopf.on.resume(kind="AuthPolicy", param="AuthPolicy")
@kopf.on.create(kind="AuthPolicy", param="AuthPolicy")
@kopf.on.update(kind="AuthPolicy", field="spec", param="AuthPolicy")
@kopf.on.resume(kind="RateLimitPolicy", param="RateLimitPolicy")
@kopf.on.create(kind="RateLimitPolicy", param="RateLimitPolicy")
@kopf.on.update(kind="RateLimitPolicy", field="spec", param="RateLimitPolicy")
@kopf.on.resume(kind="Limitador", param="Limitador")
@kopf.on.create(kind="Limitador", param="Limitador")
@kopf.on.update(kind="Limitador", field="spec", param="Limitador")
@kopf.on.update(
    kind="ConfigMap",
    field="data",
    param="ConfigMap",
    labels={"app": "limitador", "limitador-resource": "limitador"},
)
@kopf.on.create(
    kind="ConfigMap",
    field="data",
    param="ConfigMap",
    labels={"app": "limitador", "limitador-resource": "limitador"},
)
@db_session
def spec_watch(name, namespace, status, param, **kargs):
    policies = ["RateLimitPolicy", "AuthPolicy"]
    # e = Event(name=name, namespace=namespace, kind=param, change="spec")
    e = Event()
    e.name = name
    e.namespace = namespace
    e.kind = param
    e.changed = "spec"

    if param in policies:
        if "conditions" not in status:
            log.error("conditions not in the status block")
            return None
        for condition in status["conditions"]:
            if condition["type"] == "Accepted":
                e.accepted = condition["status"]
            elif condition["type"] == "Enforced":
                e.enfocred = condition["status"]

        log.info(e)
        return None

    if param == "WasmPlugin":
        log.info(e)
        return None

    if param == "ConfigMap":
        e.changed = "data"
        log.info(e)
        return None

    for condition in status["conditions"]:
        if condition["type"] == "Ready":
            e.ready = condition["status"]
    log.info(e)
    return None


# @kopf.on.resume(kind="Limitador")
# @kopf.on.update(kind="Limitador", field="spec")
# def limitador(body, **kargs):
#     """Save a copy of the limitador CR on start up and when the spec changes."""
#     timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
#     data = dict(body)
#     data["metadata"]["managedFields"] = None
#     cr_yaml = yaml.dump(data)
#     filename = f"limitador/limitador_{timestamp}.yaml"
#     with open(filename, "w") as f:
#         f.write(cr_yaml)
#     log.info("Saved copy of limitador CR")
#
#     return None
#
#
# @kopf.on.resume(kind="WasmPlugin")
# @kopf.on.create(kind="WasmPlugin")
# @kopf.on.update(kind="WasmPlugin", field="spec")
# def WasmPlugin(name, body, **kargs):
#     """Save a copy of the limitador CR on start up and when the spec changes."""
#     timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
#     data = dict(body)
#     data["metadata"]["managedFields"] = None
#     cr_yaml = yaml.dump(data)
#     filename = f"wasmplugin/{name}_{timestamp}.yaml"
#     with open(filename, "w") as f:
#         f.write(cr_yaml)
#     log.info("Saved copy of wasmplugin")
#
#     return None


# def create_directory():
#     if os.path.isdir("limitador"):
#         log.info("using existing directory limitador to save the results")
#     else:
#         log.info("creating directory limitador to save the results")
#         os.mkdir("limitador")
#
#     if os.path.isdir("wasmplugin"):
#         log.info("using existing directory wasmplugin to save the results")
#     else:
#         log.info("creating directory wasmplugin to save the results")
#         os.mkdir("wasmplugin")


def run():
    # create_directory()
    kopf.run(clusterwide=True)


if __name__ == "__main__":
    run()
