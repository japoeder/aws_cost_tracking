import argparse
import logging
import os


def logger(cwd):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--debug",
        help="Print lots of debugging statements",
        action="store_const",
        dest="loglevel",
        const=logging.DEBUG,
        default=logging.WARNING,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Be verbose",
        action="store_const",
        dest="loglevel",
        const=logging.INFO,
    )
    args = parser.parse_args()
    out = logging.basicConfig(
        filename=f"{cwd}/output/logs/runlog.log",
        format="%(asctime)s %(message)s",
        filemode="w",
        level=logging.INFO,
    )

    return out
