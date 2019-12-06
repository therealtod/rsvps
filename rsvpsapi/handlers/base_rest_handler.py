from tornado.web import RequestHandler

import json
from traceback import format_exception
from http import HTTPStatus
import logging

logger = logging.getLogger()


class BaseRestHandler(RequestHandler):
    def write_error(self, status_code, **kwargs):
        error = {}
        error["Traceback"] = "".join(format_exception(*kwargs["exc_info"]))
        logger.error("".join(format_exception(*kwargs["exc_info"])))
        self.write(error)

    def get_body_as_json(self):
        try:
            return json.loads(self.request.body)
        except ValueError:
            self.set_status(HTTPStatus.BAD_REQUEST)
            self.finish({"error": "The body of the request doesn't seem to be in JSON format"})
            return None
