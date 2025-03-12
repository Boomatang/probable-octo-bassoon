import argparse
import json
import logging
import os
import sys
from collections import Counter
from datetime import datetime

import matplotlib.pyplot as plt
from pony.orm import Database, Optional, PrimaryKey, db_session

db = Database()
plt.set_loglevel(level="warning")


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
    version = Optional(int, nullable=True)
    applied_at = Optional(datetime)


def get_logger(logger_name=None):
    if logger_name is None:
        logger_name = __name__
    logger = logging.getLogger(logger_name)
    logging.basicConfig(
        level=logging.DEBUG, handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logger


log = get_logger("grapher")


def plot_timedelta_graph(data1, data2):
    # Convert timedelta to total seconds for plotting
    x_values1 = [td.total_seconds() for td, _ in data1]
    y_values1 = [value for _, value in data1]

    x_values2 = [td.total_seconds() for td, _ in data2]
    y_values2 = [value for _, value in data2]

    fig, axs = plt.subplots(2, 1, figsize=(8, 10))

    # First graph (flipped axes)
    axs[0].plot(y_values1, x_values1, marker="o", linestyle="-")
    axs[0].set_ylabel("Time (seconds)")
    axs[0].set_xlabel("Value")
    axs[0].set_title("Value vs Timedelta Graph 1")
    axs[0].grid()

    # Second graph (flipped axes)
    axs[1].plot(y_values2, x_values2, marker="s", linestyle="--", color="r")
    axs[1].set_ylabel("Time (seconds)")
    axs[1].set_xlabel("Value")
    axs[1].set_title("Value vs Timedelta Graph 2")
    axs[1].grid()

    # Format y-axis labels to show timedelta format with better spacing
    y_labels1 = [y for y in x_values1]
    axs[0].set_yticks(x_values1[::2])  # Show fewer labels to reduce overlap
    axs[0].set_yticklabels(y_labels1[::2], rotation=30, ha="right")

    y_labels2 = [y for y in x_values2]
    axs[1].set_yticks(x_values2[::2])  # Show fewer labels to reduce overlap
    axs[1].set_yticklabels(y_labels2[::2], rotation=30, ha="right")

    plt.tight_layout()
    plt.show()


@db_session()
def main():
    log.info("starting graph process")

    parser = argparse.ArgumentParser(prog="grapher")
    parser.add_argument("database", help="path to sqlite database")
    args = parser.parse_args()
    log.debug(args)
    database = os.path.join(os.getcwd(), args.database)
    log.info(f"{database=}")
    db.bind(provider="sqlite", filename=database)
    db.generate_mapping()

    # events = list(Event.select(lambda e: e.kind == "RateLimitPolicy"))
    events = Event.select()
    log.debug(len(events))
    kinds = []
    policies = []
    for e in events:
        kinds.append(e.kind)
        if e.kind == "RateLimitPolicy" and e.changed == "status":
            policies.append(e)

    counter = Counter(kinds)
    log.debug(counter)
    log.debug(f"{len(policies)}")
    policies.reverse()

    log.debug(policies[0].timestamp)
    log.debug(policies[-1].timestamp)

    data1 = []
    data2 = []
    count = 0
    state = None
    timestamp = None
    skipped = 0
    for p in policies:
        s_name = p.name.split("-")
        if len(s_name) != 3:
            skipped += 1
            continue

        if p.enfocred is None or p.accepted is None:
            # log.debug(f"skipping newly created policy: {p.name}")
            continue

        t_state = json.loads(p.enfocred.lower()) and json.loads(p.accepted.lower())
        if state is None and timestamp is None:
            state = t_state
            timestamp = p.timestamp
            count += 1
            continue

        if state == t_state and p.timestamp < timestamp:
            timestamp = p.timestamp
            count += 1
            continue

        if state != t_state:
            cycle = timestamp - p.timestamp
            if not t_state:
                data1.append((cycle, count))
            if t_state:
                data2.append((cycle, count))

            state = t_state
            timestamp = p.timestamp
            count = 1
            continue

    log.debug(f"{len(policies)=}")
    if skipped:
        log.warning(f"skipped {skipped} policies as they did not match")

    data1.sort(key=lambda a: a[1])
    data2.sort(key=lambda a: a[1])
    plot_timedelta_graph(data1, data2)


if __name__ == "__main__":
    main()
