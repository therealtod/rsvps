import unittest
import json
import datetime

from main import get_group_data, get_attendances_data

class TestMainModule(unittest.TestCase):
    kafka_message_content = '{"venue":{"venue_name":"The Flying Horse","lon":18.056961,"lat":59.344563,"venue_id":26756200},"visibility":"public","response":"yes","guests":0,"member":{"member_id":31293962,"photo":"https://secure.meetupstatic.com/photos/member/3/5/5/e/thumb_127033662.jpeg","member_name":"Jinal Shah"},"rsvp_id":1819352181,"mtime":1575632522626,"event":{"event_name":"Stoic pub night","event_id":"266532461","time":1575999000000,"event_url":"https://www.meetup.com/Stockholm-Stoic-Meetup/events/266532461/"},"group":{"group_topics":[{"urlkey":"stoicism","topic_name":"Stoicism"},{"urlkey":"philosophy","topic_name":"Philosophy"},{"urlkey":"critical-thinking","topic_name":"Critical Thinking"},{"urlkey":"philosophy-discussions","topic_name":"Philosophy Discussions"},{"urlkey":"humanism","topic_name":"Humanism"},{"urlkey":"ethics","topic_name":"Ethics"},{"urlkey":"skeptics","topic_name":"Skeptics"},{"urlkey":"intellectual-discussion","topic_name":"Intellectual Discussion"}],"group_city":"Stockholm","group_country":"se","group_id":32976958,"group_name":"Stockholm Stoic Meetup","group_lon":18.07,"group_urlname":"Stockholm-Stoic-Meetup","group_lat":59.33}}'

    def setUp(self):
        self.raw_data_good = json.loads(self.kafka_message_content)
        self.raw_data_good_more_than_one_rsvps = json.loads(self.kafka_message_content)
        self.raw_data_good_more_than_one_rsvps["yes_rsvp_count"] = 5
        self.raw_data_no_group_location = json.loads(self.kafka_message_content)
        del self.raw_data_no_group_location["group"]["group_lat"]
        del self.raw_data_no_group_location["group"]["group_lon"]
        self.raw_data_no_group_name = json.loads(self.kafka_message_content)
        del self.raw_data_no_group_name["group"]["group_name"]
        self.raw_data_no_group_id = json.loads(self.kafka_message_content)
        del self.raw_data_no_group_id["group"]["group_id"]
        self.raw_data_no_event_id = json.loads(self.kafka_message_content)
        del self.raw_data_no_event_id["event"]["event_id"]
        self.raw_data_no_city = json.loads(self.kafka_message_content)
        del self.raw_data_no_city["group"]["group_city"]
        self.raw_data_no_date = json.loads(self.kafka_message_content)
        del self.raw_data_no_date["mtime"]
        self.raw_data_no_rsvps_count = json.loads(self.kafka_message_content)
        del self.raw_data_no_rsvps_count["response"]
        self.raw_data_response_is_no = json.loads(self.kafka_message_content)
        self.raw_data_response_is_no["response"] = "no"

    def test_get_group_data_good(self):
        expected = {
            "group_name": "Stockholm Stoic Meetup",
            "_id": 32976958,
            "location": [18.07, 59.33]
        }
        self.assertEqual(expected, get_group_data(self.raw_data_good))
    
    def test_get_group_data_no_location(self):
        self.assertEqual(None, get_group_data(self.raw_data_no_group_location))
    
    def test_get_group_data_no_group_name(self):
        self.assertEqual(None, get_group_data(self.raw_data_no_group_name))

    def test_get_group_data_no_group_id(self):
        self.assertEqual(None, get_group_data(self.raw_data_no_group_id))
    
    def test_get_attendances_data_good(self):
        expected = {
            "event_id": "266532461",
            "rsvps": 1,
            "city": "Stockholm",
            "date": datetime.datetime(2019, 12, 6, 11, 42, 2, 626000, tzinfo=datetime.timezone.utc)
        }
        self.assertEqual(expected, get_attendances_data(self.raw_data_good))
    
    def test_get_attendances_data_good_more_than_one_rsvps(self):
        expected = {
            "event_id": "266532461",
            "rsvps": 5,
            "city": "Stockholm",
            "date": datetime.datetime(2019, 12, 6, 11, 42, 2, 626000, tzinfo=datetime.timezone.utc)
        }
        self.assertEqual(expected, get_attendances_data(self.raw_data_good_more_than_one_rsvps))

    def test_get_attendances_data_no_event_id(self):
        self.assertEqual(None, get_attendances_data(self.raw_data_no_event_id))
    
    def test_get_attendances_data_no_city_name(self):
        self.assertEqual(None, get_attendances_data(self.raw_data_no_city))
    
    def test_get_attendances_data_no_date(self):
        self.assertEqual(None, get_attendances_data(self.raw_data_no_date))

    def test_get_attendances_data_no_rsvps_count(self):
        self.assertEqual(None, get_attendances_data(self.raw_data_no_rsvps_count))
    
    def test_get_attendances_data_response_is_no(self):
        self.assertEqual(None, get_attendances_data(self.raw_data_response_is_no))

if __name__ == '__main__':
    unittest.main()