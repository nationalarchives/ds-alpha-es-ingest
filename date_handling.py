from datetime import datetime
import calendar
from collections import namedtuple
import json
import requests


def parse_eras():
    era_data = {}
    r = requests.get("https://alpha.nationalarchives.gov.uk/staticdata/eras.json")
    if r.status_code == requests.codes.ok:
        eras = r.json()
    else:
        with open("eras.json", "r") as f:
            eras = json.load(f)
    for k, v in eras.items():
        era_start = datetime.strptime(v["start_date"], "%Y-%m-%d")
        era_end = datetime.strptime(v["end_date"], "%Y-%m-%d")
        era_data[k] = {"era_start": era_start, "era_end": era_end}
    return era_data


def check_date_overlap(start_obj, end_obj, start_era, end_era):
    """
    taken, initially, from:

    https://stackoverflow.com/a/9044111

    :param start_obj:
    :param end_obj:
    :param start_era:
    :param end_era:
    :return:
    """
    Range = namedtuple('Range', ['start', 'end'])
    # Just make a date that spans the entire history of tNA if no bound provided
    if not start_obj:
        s = datetime(974, 1, 1)  # Start of medieval period
    else:
        s = datetime(start_obj["year"], start_obj["month"], start_obj["day"])
    if not end_obj:
        e = datetime.today()  # Today
    else:
        e = datetime(end_obj["year"], end_obj["month"], end_obj["day"])
    r1 = Range(start=s,
               end=e)
    r2 = Range(start=start_era,
               end=end_era)
    latest_start = max(r1.start, r2.start)
    earliest_end = min(r1.end, r2.end)
    delta = (earliest_end - latest_start).days + 1
    overlap = max(0, delta)
    if overlap > 0:
        return True
    else:
        return False


def identify_eras(era_dict, item_start, item_end):
    if era_dict and item_start and item_end:
        eras = []
        for k, v in era_dict.items():
            match = check_date_overlap(start_obj=item_start, end_obj=item_end,
                                       start_era=v["era_start"], end_era=v["era_end"])
            if match:
                eras.append(k)
        return eras
    else:
        return []


def fallback_date_parser(datestring):
    """
    This date might have something wonky happening, e.g.

    29th of Feb, when it's not a leap year

    :param datestring:
    :return:
    """
    try:
        month = int(datestring[4:6])
        year = int(datestring[0:4])
        day = int(datestring[-2:])
        last_day_of_month = calendar.monthrange(year, month)[1]
        if day > last_day_of_month:
            day = last_day_of_month
        fixed_date = datetime.strptime("".join([str(year), str(month), str(day)]), "%Y%m%d")
        d_dict = {"year": fixed_date.year, "month": fixed_date.month, "day": fixed_date.day}
        d_string = fixed_date.strftime("%Y-%m-%d")
        return d_string, d_dict
    except ValueError:  # Changed this to avoid confusion where it returns a dict, rather than None
        return None, None  # {"year": None, "month": None, "day": None}


def gen_date(datestring, identifier, catalogue_ref):
    """

    :param datestring:
    :param identifier:
    :param catalogue_ref:
    :return:
    """
    try:
        d = datetime.strptime(datestring, "%Y%m%d")
        d_dict = {"year": d.year, "month": d.month, "day": d.day}
        if d.year:
            d_dict["century"] = int(str(d.year)[:-2])
        d_string = d.strftime("%Y-%m-%d")
        return d_string, d_dict
    except ValueError:
        with open("date_errors.txt", "a") as df:
            df.writelines(f"ID: {identifier} - Cat Ref: {catalogue_ref} - Date with "
                          f"error: {datestring}\n")
            return fallback_date_parser(datestring)


if __name__ == "__main__":
    a_, a = gen_date("09740101", "foo", "bar")
    b_, b = gen_date("14851231", "foo", "bar")

    e = parse_eras()
    print(e)
    eras_ = identify_eras(era_dict=e, item_start=a, item_end=b)
    print(eras_)

