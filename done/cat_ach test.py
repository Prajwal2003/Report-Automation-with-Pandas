from datetime import datetime, timedelta

def get_mondays(date=None):
    if date is None:
        date = datetime.now().date()
    
    weekday = date.weekday()
    
    days_to_last_monday = (weekday - 0) % 7
    
    last_monday = date - timedelta(days=days_to_last_monday)
    
    previous_monday = last_monday - timedelta(days=7)
    
    return last_monday, previous_monday

latest_monday, previous_monday = get_mondays()

start_date = previous_monday
end_date = latest_monday

month = start_date.month
year = start_date.year

start_date_week = start_date
start_date_month = datetime(2024, month, 1)
start_date_year = datetime(year, 4, 1)

start_date_week_str = start_date_week.strftime("%Y-%m-%dT00:00:00Z")
start_date_month_str = start_date_month.strftime("%Y-%m-%dT00:00:00Z")
start_date_year_str = start_date_year.strftime("%Y-%m-%dT00:00:00Z")

end_date = end_date.strftime("%Y-%m-%dT23:59:59Z")

start_dates = [start_date_week_str, start_date_month_str, start_date_year_str]

print(start_dates[2])
print(end_date)
from datetime import datetime

day1 = str(datetime.strptime(start_dates[1], "%Y-%m-%dT%H:%M:%SZ").day)
day2 = str(int(day1) + 6)
x = day1 + "-" + day2
header = []
header.append(x + ' Target')
header.append(x + ' Achieved')
header.append(x + ' Margin')
print(header)