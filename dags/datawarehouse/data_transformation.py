from datetime import timedelta, datetime


def parse_duration(duration_str: str) -> timedelta:
    duration_str = duration_str.replace("P", "").replace("T", "")

    components = ["D", "H", "M", "S"]
    values = {"D": 0, "H": 0, "M": 0, "S": 0}

    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component, 1)
            values[component] = int(value)

    return timedelta(
        days=values["D"], hours=values["H"], minutes=values["M"], seconds=values["S"]
    )


def transform_data(row):
    duration_raw = row.get("Duration")

    if duration_raw:
        duration_td = parse_duration(duration_raw)
        row["Duration"] = (datetime.min + duration_td).time()
        secs = duration_td.total_seconds()
    else:
        # if Duration is missing/empty
        row["Duration"] = None
        secs = 0

    row["Video_Type"] = "Shorts" if secs <= 60 else "Normal"
    return row
