import argparse
import asyncio
import datetime
import json
import logging
import os
import random
import tomllib
import traceback

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

# Global list to store created resources for random label updates
created_resources = []
resource_lock = asyncio.Lock()

# Configure logger
logging.basicConfig(
    filename="task_log.log",
    level=logging.DEBUG,
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
            accepted_at TEXT,
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
        self._created: datetime.datetime = None
        self._accepted: datetime.datetime = None
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
            self._created = datetime.datetime.now(datetime.UTC)
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
                            self._accepted = datetime.datetime.now(datetime.UTC)
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

    async def update(
        self, client: k8s_client.CustomObjectsApi, label_updates: dict
    ) -> None:
        """Update labels on the resource"""
        try:
            logger.debug(
                f"Getting current resource: {self.group}/{self.version} {self.namespace}/{self.plural}/{self.name}"
            )

            # Get the current resource
            current_resource = await client.get_namespaced_custom_object(
                group=self.group,
                version=self.version,
                namespace=self.namespace,
                plural=self.plural,
                name=self.name,
            )

            logger.debug(
                f"Retrieved resource {self.name},"
                f"current labels: {current_resource.get('metadata', {}).get('labels', {})}"
            )

            # Update the labels
            if "labels" not in current_resource["metadata"]:
                current_resource["metadata"]["labels"] = {}

            current_resource["metadata"]["labels"].update(label_updates)

            logger.debug(
                f"Updated labels will be: {current_resource['metadata']['labels']}"
            )

            # Apply the update
            await client.replace_namespaced_custom_object(
                group=self.group,
                version=self.version,
                namespace=self.namespace,
                plural=self.plural,
                name=self.name,
                body=current_resource,
            )
            logger.info(f"Successfully updated labels for {self}: {label_updates}")
        except ApiException as e:
            logger.error(f"Failed to update labels for {self}: {e.reason}")
            logger.error(f"API Exception details: status={e.status}, body={e.body}")
        except Exception as e:
            logger.error(f"Unexpected error updating labels for {self}: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")


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


def auth_policy(name: str, namespace: str, target_ref: dict, run: str) -> Entry:
    """Generate auth policy object"""

    data = {
        "apiVersion": "kuadrant.io/v1",
        "kind": "AuthPolicy",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "spec": {"targetRef": target_ref, "rules": {}},
    }

    if target_ref["kind"] == "Gateway":
        data["spec"]["rules"] = {
            "authorization": {"allow-all": {"opa": {"rego": "allow = false"}}},
            "response": {
                "unauthorized": {
                    "headers": {'"content-type"': {"value": "application/json"}},
                    "body": {
                        "value": '{\n  "error": "Forbidden",\n  "message": "Access denied by default by the gateway'
                        'operator. If you are the administrator of the service,"'
                        'create a specific auth policy for the route."\n}'
                    },
                }
            },
        }

        def auth_check(status: dict) -> bool:
            conditions: dict = status.get("conditions", [])
            logger.debug("auth policy gateway check function")
            logger.debug(f"{conditions=}")
            for condition in conditions:
                if condition["type"] == "Accepted" and condition["status"] == "True":
                    return True
            return False

    elif target_ref["kind"] == "HTTPRoute":
        data["spec"]["rules"] = {
            "authorization": {"allow-all": {"opa": {"rego": "allow = true"}}},
            "authentication": {
                "api-key-users": {
                    "apiKey": {
                        "allNamespaces": True,
                        "selector": {"matchLabels": {"app": "scale-test"}},
                    },
                    "credentials": {"authorizationHeader": {"prefix": "APIKEY"}},
                }
            },
            "response": {
                "success": {
                    "filters": {
                        "identity": {
                            "json": {
                                "properties": {
                                    "userid": {
                                        "selector": "auth.identity.metadata.annotations.secret\\.kuadrant\\.io/user-id"
                                    }
                                }
                            }
                        }
                    }
                }
            },
        }

        def auth_check(status: dict) -> bool:
            conditions: dict = status.get("conditions", [])
            logger.debug("auth policy gateway check function")
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
        plural="authpolicies",
        check=auth_check,
        run=run,
    )


def gateway_config(
    name: str, namespace: str, listeners: int, tls: bool, run: str
) -> Entry:
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
        listeners = config.get("listeners", 1)
        for item in config["order"]:
            if item == "gateway":
                await create_gateway(
                    listeners, run, i, config, progress, producer_task_id, queue
                )
            elif item == "route":
                await create_routes(
                    listeners, run, i, config, progress, producer_task_id, queue
                )
            elif item == "rlp":
                await gateway_rlp(run, i, config, progress, producer_task_id, queue)
            elif item == "route-rlp":
                await route_rlp(
                    listeners, run, i, config, progress, producer_task_id, queue
                )
            elif item == "auth":
                await gateway_auth(run, i, config, progress, producer_task_id, queue)
            elif item == "route-auth":
                await route_auth(
                    listeners, run, i, config, progress, producer_task_id, queue
                )
            else:
                logger.error(f"unknown data type {item}")
                print("error, check log")
                exit(1)

    await queue.put(None)  # Signal completion


async def create_gateway(listeners, run, gateway, config, progress, task_id, queue):
    name = f"{run}-gw{gateway}"
    data = gateway_config(name, config["namespace"], listeners, use_tls(config), run)
    await queue.put(data)
    progress.advance(task_id, 1)  # Update progress for producer
    logger.info(f"Produced: {data.name}")
    logger.debug(data)


async def create_routes(listeners, run, gateway, config, progress, task_id, queue):
    for listener in range(listeners):
        name = f"{run}-gw{gateway}-l{listener}"
        parent_refs = [
            {
                "group": "gateway.networking.k8s.io",
                "kind": "Gateway",
                "name": f"{run}-gw{gateway}",
            }
        ]
        hostnames = [
            f"api.scale-test-{name}.{KUADRANT_ZONE_ROOT_DOMAIN}",
        ]
        data = route(name, config["namespace"], parent_refs, hostnames, run)
        await queue.put(data)
        progress.advance(task_id, 1)  # Update progress for producer
        logger.info(f"Produced: {data}")


async def gateway_rlp(run, gateway, config, progress, task_id, queue):
    name = f"{run}-gw{gateway}"
    target_ref = {
        "group": "gateway.networking.k8s.io",
        "kind": "Gateway",
        "name": f"{run}-gw{gateway}",
    }
    data = rate_limit_policy(name, config["namespace"], target_ref, run)
    await queue.put(data)
    progress.advance(task_id, 1)  # Update progress for producer
    logger.info(f"Produced: {data}")


async def route_rlp(listeners, run, gateway, config, progress, task_id, queue):
    for listener in range(listeners):
        name = f"{run}-gw{gateway}-l{listener}"
        target_ref = {
            "group": "gateway.networking.k8s.io",
            "kind": "HTTPRoute",
            "name": f"{run}-gw{gateway}-l{listener}",
        }
        data = rate_limit_policy(name, config["namespace"], target_ref, run)
        await queue.put(data)
        progress.advance(task_id, 1)  # Update progress for producer
        logger.info(f"Produced: {data}")


async def gateway_auth(run, gateway, config, progress, task_id, queue):
    name = f"{run}-gw{gateway}"
    target_ref = {
        "group": "gateway.networking.k8s.io",
        "kind": "Gateway",
        "name": f"{run}-gw{gateway}",
    }
    data = auth_policy(name, config["namespace"], target_ref, run)
    await queue.put(data)
    progress.advance(task_id, 1)  # Update progress for producer
    logger.info(f"Produced: {data}")


async def route_auth(listeners, run, gateway, config, progress, task_id, queue):
    for listener in range(listeners):
        name = f"{run}-gw{gateway}-l{listener}"
        target_ref = {
            "group": "gateway.networking.k8s.io",
            "kind": "HTTPRoute",
            "name": f"{run}-gw{gateway}-l{listener}",
        }
        data = auth_policy(name, config["namespace"], target_ref, run)
        await queue.put(data)
        progress.advance(task_id, 1)  # Update progress for producer
        logger.info(f"Produced: {data}")


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
            break
        await clean_up_queue.put(item)
        await item.create(api_instance)
        await item.accepted(api_instance)
        await item.save()

        # Add successfully created resource to the global list for random updates
        async with resource_lock:
            created_resources.append(item)
            logger.info(
                f"Added {item} to created_resources list. Total: {len(created_resources)}"
            )

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
        await item.delete(api_instance)
        progress.advance(cleaner_task_id, 1)
        logger.info(f"Cleaner-{cleaner_id} processed {item}")


async def random_label_updater(api_instance, progress, task_id):
    """
    Randomly update labels on deployed resources every second for one minute.
    """

    logger.info("Starting random label updater for 60 seconds")

    # Track label update counter per resource
    label_counters = {}

    # Run for 60 seconds
    for second in range(60):
        pause = asyncio.sleep(1)
        async with resource_lock:
            logger.debug(
                f"Second {second + 1}/60 - Total resources available: {len(created_resources)}"
            )
            if not created_resources:
                logger.warning("No resources available for random updates")
                # Still advance progress even if no resources available
                progress.advance(task_id, 1)
                await pause
                continue

            # Randomly select a resource
            selected_resource = random.choice(created_resources)  # nosec
            logger.info(
                f"Selected resource for update: {selected_resource}"
                f"({selected_resource.group}/{selected_resource.version})"
            )

        # Get current counter for this resource
        resource_key = f"{selected_resource.name}-{selected_resource.namespace}"
        current_counter = label_counters.get(resource_key, 0) + 1
        label_counters[resource_key] = current_counter

        # Update the label with incremented counter
        label_updates = {
            "random-update-counter": str(current_counter),
            "last-updated": str(int(asyncio.get_event_loop().time())),
        }

        logger.info(
            f"Attempting to update {selected_resource} with labels: {label_updates}"
        )

        try:
            await selected_resource.update(api_instance, label_updates)
            logger.info(
                f"Successfully updated {selected_resource} with counter {current_counter}"
            )

        except Exception as e:
            logger.error(f"Failed to update {selected_resource}: {e}")
            logger.error(f"Update error traceback: {traceback.format_exc()}")

        progress.advance(task_id, 1)

        # Wait 1 second before next update
        await pause

    logger.info("Random label updater completed")


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
    parser.add_argument(
        "--random-updates",
        help="Enable random label updates on deployed resources",
        action="store_true",
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
        if key == "setup":
            continue
        logger.info(f"starting test configuration {key}")
        print(f"[red]Running configuration {key}")
        setup = config[key]

        # Clear created resources list at the start of each configuration
        async with resource_lock:
            created_resources.clear()

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

            # Wait for all tasks to complete
            await asyncio.gather(*consumers)

        # Run random label updater if enabled
        if args.random_updates:
            print("[yellow]Starting random label updates for 60 seconds...")
            with Progress(disable=ui_disable) as progress:
                updater_task_id = progress.add_task(
                    "[yellow]Random label updates...", total=60
                )
                await random_label_updater(api_instance, progress, updater_task_id)
            print("[yellow]Random label updates completed")

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
            # Clear the created resources list after cleanup
            async with resource_lock:
                created_resources.clear()
        print()


if __name__ == "__main__":
    # Run the event loop
    asyncio.run(main())
