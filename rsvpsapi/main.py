import tornado.ioloop
import tornado.web
from tornado.web import URLSpec as Url

import logging

from handlers.near import NearHandler
from handlers.top_cities import TopCitiesHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PORT = 8080


def make_app():
    return tornado.web.Application([
        Url(pattern=r"/near",
            handler=NearHandler,
            name="near"),
        Url(pattern=r"/topCities",
            handler=TopCitiesHandler,
            name="topCities"),
    ])


if __name__ == "__main__":
    logger.info(f"Starting the server, listening at port: {PORT}")
    app = make_app()
    app.listen(PORT)
    tornado.ioloop.IOLoop.current().start()