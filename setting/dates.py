from datetime import datetime, timedelta

def dates():
    today = datetime.now()
    day_of_week = today.weekday()

    if day_of_week in [4, 5, 6]:  # Friday, Saturday, or Sunday
        # Calculate the number of days until the next Monday
        days_until_monday = 7 - day_of_week

        if day_of_week == 4:
            next_monday = today 
            next_tuesday = next_monday + + timedelta(days=days_until_monday)
        else:
            next_monday = today + timedelta(days=days_until_monday)
            next_tuesday = next_monday + timedelta(days=1)
        return next_monday.strftime("%Y-%m-%d"), next_tuesday.strftime("%Y-%m-%d")
    else:
        tomorrow = today + timedelta(days=1)
        return today.strftime("%Y-%m-%d"), tomorrow.strftime("%Y-%m-%d")
    