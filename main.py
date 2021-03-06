import sys
import json
import logging
import asyncio

from application.app import Application

LOG_FORMAT = '%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s'


def main():
    config = json.load(open('config/main.json', encoding='utf8'))

    log_level = config['system']['log_level'].upper()
    logging.basicConfig(
        level=log_level,
        format=LOG_FORMAT,
        filename=config['system'].get('log', None)
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(log_level)
    ch.setFormatter(logging.Formatter(LOG_FORMAT))
    log = logging.getLogger()
    log.addHandler(ch)

    loop = asyncio.get_event_loop()
    app = Application(config=config, loop=loop)
    app.run()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
