import argparse
import asyncio
import logging
import os
import tomllib
from datetime import datetime

from kubernetes_asyncio import client as k8s_client
from kubernetes_asyncio import config as k8s_config
from kubernetes_asyncio.client.exceptions import ApiException
from rich import print
from rich.progress import Progress

KUADRANT_ZONE_ROOT_DOMAIN = "example.com"

# Configure logger
logging.basicConfig(
    filename="task_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger("task_logger")


class Entry:
    def __init__(
        self,
        name: str,
        namespace: str,
        data: dict,
        group: str,
        version: str,
        plural: str,
    ):
        self.name = name
        self.namespace = namespace
        self._created = None
        self._accepted = None
        self.data = data
        self.group = group
        self.version = version
        self.plural = plural

    async def create(self, client: k8s_client.CustomObjectsApi) -> None:
        try:
            resp = await client.create_namespaced_custom_object(
                group=self.group,
                version=self.version,
                namespace=self.namespace,
                plural=self.plural,
                body=self.data,
            )
            logger.info(f"Custom Resource {self.name} created: {resp}")
            self._created = datetime.utcnow()
        except ApiException as e:
            logger.error(f"failed creating {self.name}", e.reason)
            logger.debug(self)


def gateway(name: str, namespace: str, listeners: int, tls: bool) -> Entry:
    """Generate gateway object"""

    data = {
        "apiVersion": "gateway.networking.k8s.io/v1",
        "kind": "Gateway",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "spec": {
            "gatewayClassName": "istio",
            "listeners": [],
        },
    }

    for i in range(listeners):
        listener = {
            "allowedRoutes": {
                "namespaces": {
                    "from": "All",
                },
            },
            "hostname": f"api.scale-test-{name}-l{i}.{KUADRANT_ZONE_ROOT_DOMAIN}",
            "name": f"api-{i}",
            "port": 443,
            "protocol": "HTTPS",
        }
        if tls:
            listener["tls"] = {
                "mode": "Terminate",
                "certificateRefs": [
                    {
                        "name": f"cert-{name}-l{i}",
                        "kind": "Secret",
                    },
                ],
            }
        data["spec"]["listeners"].append(listener)

    return Entry(
        name=name,
        namespace=namespace,
        data=data,
        group="gateway.networking.k8s.io",
        version="v1",
        plural="gateways",
    )


def use_tls(config: dict) -> bool:
    """Returns true if tls policy creation is going to be done."""
    # TODO: Make this actual work
    return False


async def producer(queue, run: str, config: dict, progress, producer_task_id):
    """
    Producer adds items to the queue.
    """

    for i in range(config["gateways"]):
        data = None
        listeners = config.get("listeners", 1)
        for item in config["order"]:
            if item == "gateway":
                name = f"{run}-gw{i}"
                data = gateway(name, config["namespace"], listeners, use_tls(config))
                await queue.put(data)
                progress.advance(producer_task_id, 1)  # Update progress for producer
                logger.info(f"Produced: {data.name}")
                logger.debug(data)
            elif item == "route":
                for listener in range(listeners):
                    data = f"route-{listener}"
                    await queue.put(data)
                    progress.advance(
                        producer_task_id, 1
                    )  # Update progress for producer
                    logger.info(f"Produced: {data}")
            elif item == "rlp":
                data = "rlp"
                await queue.put(data)
                progress.advance(producer_task_id, 1)  # Update progress for producer
                logger.info(f"Produced: {data}")
            elif item == "route-rlp":
                for listener in range(listeners):
                    data = f"route-rlp-{listener}"
                    await queue.put(data)
                    progress.advance(
                        producer_task_id, 1
                    )  # Update progress for producer
                    logger.info(f"Produced: {data}")
            else:
                logger.error(f"unknown data type {item}")
                print("error, check log")
                exit(1)

    await queue.put(None)  # Signal completion


async def consumer(queue, progress, consumer_task_id, consumer_id, api_instance):
    """
    Consumer processes items from the queue.
    """
    while True:
        item: Entry = await queue.get()
        if item is None:  # Stop signal received
            queue.put_nowait(None)  # Pass the signal to other consumers
            break
        if type(item) != str:  # FIX: remove this check when all are of type Entry
            await item.create(api_instance)
        progress.advance(
            consumer_task_id, 1
        )  # Update progress for the shared consumer bar
        logger.info(f"Consumer-{consumer_id} processed {item}")


def number_of_tasks(config: dict) -> int:
    count = 0
    for item in config["order"]:
        if item.startswith("route"):
            count += config["listeners"]
        else:
            count += 1
    return count


def validate_config(config: dict) -> bool:
    """validates the config"""
    # TODO: create a validation function
    return True


async def main():

    parser = argparse.ArgumentParser(description="run test configuration")
    parser.add_argument("file", type=str, help="Path to configuration file")
    args = parser.parse_args()

    if not os.path.isfile(args.file):
        logger.error("file does not exist")
        print("file does not exist")
        exit(1)

    with open(args.file, "rb") as f:
        config = tomllib.load(f)
    if not validate_config(config):
        logger.error("configuration is not valid, check configuration file")
        print("configuration is not valid, check configuration file")
        exit(0)

    await k8s_config.load_kube_config()
    api_instance = k8s_client.CustomObjectsApi()

    for key in config:
        logger.info(f"starting test configuration {key}")
        print(f"[red]Running configuration {key}")
        setup = config[key]

        queue = asyncio.Queue()
        tasks_to_produce = number_of_tasks(setup)

        with Progress() as progress:
            # Add producer progress bar
            producer_task_id = progress.add_task(
                "[cyan]Producing tasks...", total=tasks_to_produce
            )

            # Add a single progress bar for all consumers
            consumer_task_id = progress.add_task(
                "[green]Consuming tasks...", total=tasks_to_produce
            )

            await producer(queue, key, setup, progress, producer_task_id)

            consumers = [
                asyncio.create_task(
                    consumer(queue, progress, consumer_task_id, i, api_instance)
                )
                for i in range(setup.get("workers", tasks_to_produce))
            ]

            # Wait for all tasks to complete
            await asyncio.gather(*consumers)
        print()


if __name__ == "__main__":
    # Run the event loop
    asyncio.run(main())
