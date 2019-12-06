from handlers.base_rest_handler import BaseRestHandler

from rsvps import get_top_cities


class TopCitiesHandler(BaseRestHandler):
    def get(self):
        date = self.get_query_argument("date")
        num = self.get_query_argument("num")
        response = get_top_cities(date, int(num))
        self.write({
            "top_cities": response
        })
