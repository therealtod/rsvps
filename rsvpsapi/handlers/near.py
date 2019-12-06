from handlers.base_rest_handler import BaseRestHandler

from rsvps import get_closest_n_groups

class NearHandler(BaseRestHandler):
    def get(self):
        latitude = self.get_query_argument("lat")
        longitude = self.get_query_argument("lon")
        num = int(self.get_query_argument("num"))
        response = get_closest_n_groups(longitude, latitude, num)
        self.write({
            "closest": response
        })
