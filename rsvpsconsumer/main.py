from pymongo.errors import DuplicateKeyError

import json
from datetime import datetime, timezone

from configuration import *
from services import get_consumer, get_rsvps_database, create_location_index


def get_group_data(raw_data):
    """
    Extract the data about the group from the raw data
    """
    group_data = None
    group_name = raw_data.get("group", {}).get("group_name")
    group_id = raw_data.get("group", {}).get("group_id")
    group_lat = raw_data.get("group", {}).get("group_lat")
    group_lon = raw_data.get("group", {}).get("group_lon")
    if group_id and group_name and group_lat and group_lon:
        group_data = {
                "group_name": group_name,
                "_id": group_id,
                "location": [float(group_lon), float(group_lat)]
            }
    return group_data


def get_attendances_data(raw_data):
    """
    Extract the data about the attendances (participations to the event) 
    from the raw data
    """
    data = None
    event_id = raw_data.get("id")
    if event_id is None:
        event_id = raw_data.get("event", {}).get("event_id")
    rsvps = 0
    yes_rsvp_count = raw_data.get("yes_rsvp_count")
    if yes_rsvp_count is None:
        rsvps_response = raw_data.get("response")
        if rsvps_response == "\"yes\"" or rsvps_response == "yes":
            rsvps = 1 
    else:
        rsvps = int(yes_rsvp_count)
    timestamp_m = raw_data.get("mtime")
    city = raw_data.get("group", {}).get("group_city")
    try:
        date = datetime.fromtimestamp(timestamp_m/1000, tz=timezone.utc)
    except Exception:
        # Could't parse the date. Skip.
        return None
    if event_id and city and date and rsvps > 0:
        data = {
            "event_id": event_id,
            "city": city,
            "rsvps": rsvps,
            "date": date
        }
    return data 

def main():
    create_location_index()
    for message in get_consumer():
        raw_data = message.value
        
        group_data = get_group_data(raw_data)
        db = get_rsvps_database()
        if group_data is not None:
            try:
                db.groups.insert_one(group_data)
            except DuplicateKeyError:
                # We already have info on this group. Silently skip.
                pass
        
        attendances_data = get_attendances_data(raw_data)
        if attendances_data is not None:
            db.attendances.insert_one(attendances_data)

        
        if raw_data is not None:
            # Just print the id of the new record (for raw data)
            id = db.raw.insert_one(raw_data).inserted_id
            print(id)

if __name__ == "__main__":
    main()
