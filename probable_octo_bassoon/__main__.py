import argparse
import asyncio
import json
import logging
import os
import tomllib
from datetime import datetime

import aiosqlite
from kubernetes_asyncio import client as k8s_client
from kubernetes_asyncio import config as k8s_config
from kubernetes_asyncio import watch as k8s_watch
from kubernetes_asyncio.client.exceptions import ApiException
from rich import print
from rich.progress import Progress
from rich.prompt import Prompt

KUADRANT_ZONE_ROOT_DOMAIN = "example.com"
DATABASE_PATH = "data.db"

# Configure logger
logging.basicConfig(
    filename="task_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger("task_logger")


async def create_tables():
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """
        CREATE TABLE IF NOT EXISTS resources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            namespace TEXT NOT NULL,
            api_group TEXT NOT NULL,
            version TEXT NOT NULL,
            plural TEXT NOT NULL,
            created_at TEXT NOT NULL,
            accepted_at TEXT NOT NULL,
            run TEXT NOT NULL,
            data BLOB NOT NULL

        )
                         """
        )
        await db.commit()
        logger.debug("database tables exist")


class Entry:
    def __init__(
        self,
        name: str,
        namespace: str,
        data: dict,
        group: str,
        version: str,
        plural: str,
        run: str,
        check,
    ):
        self.name = name
        self.namespace = namespace
        self._created: datetime = None
        self._accepted: datetime = None
        self.data = data
        self.group = group
        self.version = version
        self.plural = plural
        self.check = check
        self.run = run

    def __repr__(self):
        return f"{self.name}, {self.plural}"

    async def save(self):
        async with aiosqlite.connect(DATABASE_PATH, timeout=30) as db:
            data_blob = json.dumps(self.data).encode("utf-8")
            await db.execute(
                """
            INSERT INTO resources (name, namespace, api_group, version, plural, created_at, accepted_at, run, data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    self.name,
                    self.namespace,
                    self.group,
                    self.version,
                    self.plural,
                    self._created,
                    self._accepted,
                    self.run,
                    data_blob,
                ),
            )
            await db.commit()
            logger.info(f"{self} has being saved to database")

    async def create(self, client: k8s_client.CustomObjectsApi) -> None:
        try:
            resp = await client.create_namespaced_custom_object(
                group=self.group,
                version=self.version,
                namespace=self.namespace,
                plural=self.plural,
                body=self.data,
            )
            logger.info(f"Custom Resource {self} created: {resp}")
            self._created = datetime.utcnow()
        except ApiException as e:
            logger.error(f"failed creating {self}", e.reason)
            logger.debug(self)

    async def accepted(
        self, client: k8s_client.CustomObjectsApi, timeout: int = 300
    ) -> None:
        logger.debug("starting accpeted check")
        watch = k8s_watch.Watch()
        try:
            async with asyncio.timeout(timeout):
                async for event in watch.stream(
                    client.list_namespaced_custom_object,
                    group=self.group,
                    version=self.version,
                    namespace=self.namespace,
                    plural=self.plural,
                ):
                    logger.debug(f"{event=}")
                    if isinstance(event, str):
                        logger.warning(f"wtf {event=}")
                        break
                    if event["object"]["metadata"]["name"] == self.name:
                        status = event["object"].get("status", {})
                        if self.check(status):
                            logger.info(f"Resource {self} reached target status")
                            self._accepted = datetime.utcnow()
                            watch.stop()
                            return
        except asyncio.TimeoutError:
            logger.error(f"Timed out waiting for resource {self} to reach status")
        except ApiException as e:
            logger.error(f"Exception when waiting for status of {self}: {e.reason}", e)
        finally:
            watch.stop()

    async def delete(self, api_instance, timeout=300):
        try:
            logger.info(f"Deleting custom resource: {self}")
            await api_instance.delete_namespaced_custom_object(
                group=self.group,
                version=self.version,
                namespace=self.namespace,
                plural=self.plural,
                name=self.name,
            )
            logger.debug(f"Deletion initiated for resource {self}")

            # BUG: There is a bug here, when try to delete a large number of resources it stops processing.
            # # Wait for deletion
            # watch = k8s_watch.Watch()
            # async with asyncio.timeout(timeout):
            #     async for event in watch.stream(
            #         api_instance.list_namespaced_custom_object,
            #         group=self.group,
            #         version=self.version,
            #         namespace=self.namespace,
            #         plural=self.plural,
            #     ):
            #         if event["object"]["metadata"]["name"] == self.name:
            #             logger.debug(
            #                 f"Resource {self.name} still exists, waiting for deletion..."
            #             )
            #         else:
            #             logger.info(f"Resource {self.name} successfully deleted.")
            #             watch.stop()
            #             return
        except asyncio.TimeoutError:
            logger.error(f"Timed out waiting for resource {self.name} to be deleted.")
        except ApiException as e:
            if e.status == 404:
                logger.info(f"Resource {self.name} already deleted.")
            else:
                logger.error(
                    f"Exception when deleting resource {self.name}: {e.reason}", e
                )
        # finally:
        #     watch.stop()


def route(
    name: str, namespace: str, parent_refs: list[dict], hostnames: list[str], run: str
) -> Entry:
    """Generate rate limit policy object"""

    data = {
        "apiVersion": "gateway.networking.k8s.io/v1",
        "kind": "HTTPRoute",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "spec": {
            "parentRefs": parent_refs,
            "hostnames": hostnames,
            "rules": [
                {
                    "backendRefs": [
                        {
                            "group": "",
                            "kind": "Service",
                            "name": "httpbin",
                            "port": 8080,
                            "weight": 1,
                        }
                    ],
                    "matches": [{"path": {"type": "PathPrefix", "value": "/"}}],
                }
            ],
        },
    }

    def http_route_check(status: dict) -> bool:
        logger.debug("HTTPRoute check function")
        accepted = True
        parents: list[dict] = status.get("parents", {})
        for parent in parents:
            conditions = parent.get("conditions", {})

            found = False
            for condition in conditions:
                if condition["type"] == "Accepted":
                    found = True
                    if condition["status"] != "True":
                        accepted = False
            if not found:
                accepted = False
        return accepted

    return Entry(
        name=name,
        namespace=namespace,
        data=data,
        group="gateway.networking.k8s.io",
        version="v1",
        plural="httproutes",
        check=http_route_check,
        run=run,
    )


def rate_limit_policy(name: str, namespace: str, target_ref: dict, run: str) -> Entry:
    """Generate rate limit policy object"""

    data = {
        "apiVersion": "kuadrant.io/v1",
        "kind": "RateLimitPolicy",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "spec": {"targetRef": target_ref, "limits": {}},
    }

    if target_ref["kind"] == "Gateway":
        data["spec"]["limits"]['"global"'] = {
            "rates": [
                {"limit": 3, "window": "10s"},
            ]
        }

        def ratelimit_check(status: dict) -> bool:
            conditions: dict = status.get("conditions", [])
            logger.debug("rate limit policy gateway check function")
            logger.debug(f"{conditions=}")
            for condition in conditions:
                if condition["type"] == "Accepted" and condition["status"] == "True":
                    return True
            return False

    elif target_ref["kind"] == "HTTPRoute":
        data["spec"]["limits"]['"httproute-level"'] = {
            "rates": [
                {"limit": 3, "window": "10s"},
            ]
        }

        def ratelimit_check(status: dict) -> bool:
            conditions: dict = status.get("conditions", [])
            logger.debug("rate limit policy gateway check function")
            logger.debug(f"{conditions=}")
            for condition in conditions:
                if condition["type"] == "Enforced" and condition["status"] == "True":
                    return True
            return False

    return Entry(
        name=name,
        namespace=namespace,
        data=data,
        group="kuadrant.io",
        version="v1",
        plural="ratelimitpolicies",
        check=ratelimit_check,
        run=run,
    )


def gateway(name: str, namespace: str, listeners: int, tls: bool, run: str) -> Entry:
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

    def check(status: dict) -> bool:
        conditions: dict = status.get("conditions", [])
        logger.debug("gateway check function")
        logger.debug(f"{conditions=}")
        for condition in conditions:
            if condition["type"] == "Programmed" and condition["status"] == "True":
                return True
        return False

    return Entry(
        name=name,
        namespace=namespace,
        data=data,
        group="gateway.networking.k8s.io",
        version="v1",
        plural="gateways",
        check=check,
        run=run,
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
                data = gateway(
                    name, config["namespace"], listeners, use_tls(config), run
                )
                await queue.put(data)
                progress.advance(producer_task_id, 1)  # Update progress for producer
                logger.info(f"Produced: {data.name}")
                logger.debug(data)
            elif item == "route":
                for listener in range(listeners):
                    name = f"{run}-gw{i}-l{listener}"
                    parent_refs = [
                        {
                            "group": "gateway.networking.k8s.io",
                            "kind": "Gateway",
                            "name": f"{run}-gw{i}",
                        }
                    ]
                    hostnames = [
                        f"api.scale-test-{name}.{KUADRANT_ZONE_ROOT_DOMAIN}",
                    ]
                    data = route(name, config["namespace"], parent_refs, hostnames, run)
                    await queue.put(data)
                    progress.advance(
                        producer_task_id, 1
                    )  # Update progress for producer
                    logger.info(f"Produced: {data}")
            elif item == "rlp":
                name = f"{run}-gw{i}"
                target_ref = {
                    "group": "gateway.networking.k8s.io",
                    "kind": "Gateway",
                    "name": f"{run}-gw{i}",
                }
                data = rate_limit_policy(name, config["namespace"], target_ref, run)
                await queue.put(data)
                progress.advance(producer_task_id, 1)  # Update progress for producer
                logger.info(f"Produced: {data}")
            elif item == "route-rlp":
                for listener in range(listeners):
                    name = f"{run}-gw{i}-l{listener}"
                    target_ref = {
                        "group": "gateway.networking.k8s.io",
                        "kind": "HTTPRoute",
                        "name": f"{run}-gw{i}-l{listener}",
                    }
                    data = rate_limit_policy(name, config["namespace"], target_ref, run)
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


async def consumer(
    queue,
    clean_up_queue,
    save_queue,
    progress,
    consumer_task_id,
    consumer_id,
    api_instance,
):
    """
    Consumer processes items from the queue.
    """
    while True:
        item: Entry = await queue.get()
        if item is None:  # Stop signal received
            queue.put_nowait(None)  # Pass the signal to other consumers
            await clean_up_queue.put(None)
            await save_queue.put(None)
            break
        await clean_up_queue.put(item)
        if type(item) != str:  # FIX: remove this check when all are of type Entry
            await item.create(api_instance)
            await item.accepted(api_instance)
        await save_queue.put(item)
        progress.advance(
            consumer_task_id, 1
        )  # Update progress for the shared consumer bar
        logger.info(f"Consumer-{consumer_id} processed {item}")


async def cleaner(queue, progress, cleaner_task_id, cleaner_id, api_instance):
    """
    Consumer processes items from the queue.
    """
    while True:
        item: Entry = await queue.get()
        if item is None:  # Stop signal received
            queue.put_nowait(None)  # Pass the signal to other consumers
            break
        if type(item) != str:  # FIX: remove this check when all are of type Entry
            await item.delete(api_instance)
        progress.advance(cleaner_task_id, 1)
        logger.info(f"Cleaner-{cleaner_id} processed {item}")


async def saver(queue, progress, save_task_id, save_id):
    """
    save items to the database.
    """
    while True:
        item: Entry = await queue.get()
        if item is None:  # Stop signal received
            queue.put_nowait(None)  # Pass the signal to other consumers
            break
        if type(item) != str:  # FIX: remove this check when all are of type Entry
            await item.save()
        progress.advance(save_task_id, 1)
        logger.info(f"Cleaner-{save_id} processed {item}")


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
    parser.add_argument(
        "--pdb", help="Turn off rich UI if going to use pdb", action="store_true"
    )
    parser.add_argument(
        "--pause", help="Pause for user input before doing cleanup", action="store_true"
    )
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

    ui_disable = args.pdb

    await k8s_config.load_kube_config()
    api_instance = k8s_client.CustomObjectsApi()

    await create_tables()
    for key in config:
        logger.info(f"starting test configuration {key}")
        print(f"[red]Running configuration {key}")
        setup = config[key]

        queue = asyncio.Queue()
        clean_up_queue = asyncio.Queue()
        save_queue = asyncio.Queue()
        tasks_to_produce = number_of_tasks(setup)

        with Progress(disable=ui_disable) as progress:
            # Add producer progress bar
            producer_task_id = progress.add_task(
                "[cyan]Producing tasks...", total=tasks_to_produce
            )

            # Add a single progress bar for all consumers
            consumer_task_id = progress.add_task(
                "[green]Consuming tasks...", total=tasks_to_produce
            )
            save_task_id = progress.add_task(
                "[green]Saving tasks...", total=tasks_to_produce
            )

            await producer(queue, key, setup, progress, producer_task_id)

            api_instance = k8s_client.CustomObjectsApi()
            consumers = [
                asyncio.create_task(
                    consumer(
                        queue,
                        clean_up_queue,
                        save_queue,
                        progress,
                        consumer_task_id,
                        i,
                        api_instance,
                    )
                )
                for i in range(setup.get("workers", tasks_to_produce))
            ]

            saves = [
                asyncio.create_task(
                    saver(
                        save_queue,
                        progress,
                        save_task_id,
                        i,
                    )
                )
                for i in range(1)
            ]
            # Wait for all tasks to complete
            await asyncio.gather(*consumers)
            await asyncio.gather(*saves)

        if setup.get("cleanup", False):
            if args.pause:
                Prompt.ask("Waiting for input before doing cleanup")
            with Progress(disable=ui_disable) as progress:
                cleanup_task_id = progress.add_task(
                    "[red]Cleaning tasks...", total=tasks_to_produce
                )

                cleanup = [
                    asyncio.create_task(
                        cleaner(
                            clean_up_queue, progress, cleanup_task_id, i, api_instance
                        )
                    )
                    for i in range(setup.get("workers", tasks_to_produce))
                ]

                # Wait for all tasks to complete
                await asyncio.gather(*cleanup)
        print()


if __name__ == "__main__":
    # Run the event loop
    asyncio.run(main())
