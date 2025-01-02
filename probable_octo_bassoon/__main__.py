import argparse
import asyncio
import logging
import os
import tomllib

from rich import print
from rich.progress import Progress

# Configure logger
logging.basicConfig(
    filename="task_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger("task_logger")


class Entry:
    def __init__(self, name: str, namespace: str, _type: str, data: dict):
        self.name = name
        self.namespace = namespace
        self._type = _type
        self._created = None
        self._accepted = None
        self.data = data


async def producer(queue, run: str, config: dict, progress, producer_task_id):
    """
    Producer adds items to the queue.
    """

    for i in range(config["gateways"]):
        data = None
        for item in config["order"]:
            if item == "gateway":
                data = f"gatewy-{i}"
                await queue.put(data)
                progress.advance(producer_task_id, 1)  # Update progress for producer
                logger.info(f"Produced: {data}")
            elif item == "route":
                for listener in range(config["listeners"]):
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
                for listener in range(config["listeners"]):
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


async def consumer(queue, progress, consumer_task_id, consumer_id):
    """
    Consumer processes items from the queue.
    """
    while True:
        item = await queue.get()
        if item is None:  # Stop signal received
            queue.put_nowait(None)  # Pass the signal to other consumers
            break
        await asyncio.sleep(2)  # Simulate processing time
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
                asyncio.create_task(consumer(queue, progress, consumer_task_id, i))
                for i in range(setup.get("workers", tasks_to_produce))
            ]

            # Wait for all tasks to complete
            await asyncio.gather(*consumers)
        print()


if __name__ == "__main__":
    # Run the event loop
    asyncio.run(main())
