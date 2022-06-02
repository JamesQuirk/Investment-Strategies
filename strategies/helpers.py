
from datetime import datetime

def parse_date(date):
	if isinstance(date, str):
		return datetime.strptime(date, "%d/%m/%Y")
	elif isinstance(date, int):
		return datetime.fromtimestamp(date)
	else:
		return date
