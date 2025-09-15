from datetime import date, datetime, timezone

def utc_now():
    return datetime.now(timezone.utc)

def get_current_nfl_season_year():
    today = date.today()
    current_year = today.year

    if today.month < 9:
        return current_year - 1
    return current_year