
import requests
from datetime import datetime, timedelta
import time
import pandas as pd
from pathlib import Path

intervals = {
	"day": "1d",
	"week": "1wk",
	"month": "1mo"
}

def _format_data(data):
	data.rename(
		mapper={
			"Date": "date",
			"Open": "open",
			"High": "high",
			"Low": "low",
			"Close*": "close",
			"Adj Close**": "adj-close",
			"Volume": "volume"
		},
		axis=1,
		inplace=True
	)
	data["date"] = pd.to_datetime(data["date"],errors="coerce")
	data["open"] = pd.to_numeric(data["open"],errors="coerce",downcast="float")
	data["high"] = pd.to_numeric(data["high"],errors="coerce",downcast="float")
	data["low"] = pd.to_numeric(data["low"],errors="coerce",downcast="float")
	data["close"] = pd.to_numeric(data["close"],errors="coerce",downcast="float")
	data["adj-close"] = pd.to_numeric(data["adj-close"],errors="coerce",downcast="float")
	data["volume"] = pd.to_numeric(data["volume"],errors="coerce",downcast="integer")
	data.dropna(inplace=True)
	return data

def _date_format_or_default(date,granularity,boundary):
	if date:
		date = datetime.strptime(date, "%d/%m/%Y")
	else:
		if boundary == "start":
			# 1000 records up to now (i.e. today - 1000*<granularity>)
			multiplier = 1 if granularity == "day" else 7 if granularity == "week" else 30
			date = datetime.now() - timedelta(days=1000*multiplier)
		elif boundary == "end":
			date = datetime.now()
	return round(time.mktime(date.timetuple()))

def _fetch_data(ticker, date_from, date_to, granularity):
	"""
	Fetch data from finance.yahoo.com
	"""
	interval = intervals[granularity]
	url = f"https://finance.yahoo.com/quote/{ticker}/history?period1={date_from}&period2={date_to}&interval={interval}&filter=history&frequency={interval}&includeAdjustedClose=true"
	
	resp = requests.get(
		url,
		headers={
			"Connection": "keep-alive",
			"Expires": "-1",
			"Upgrade-Insecure-Requests": "1",
			"User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) \
			AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36"
		}
	)
	data = pd.read_html(
		resp.content,
		attrs={"data-test":"historical-prices"}
	)[0]
	return _format_data(data)

def _meticulously_fetch_data(ticker,date_from,date_to,granularity):
	"""
	Since finance.yahoo.com loads the history table as you scroll, it is necessary
	to apply some logic here that keeps recollecting missing data until the target 
	range is statisfied.
	"""
	history = None
	# history_date_min = datetime.fromtimestamp(date_to)
	# distance_from_target_date = history_date_min - datetime.fromtimestamp(date_from)
	prev_distance = None
	loop_counter = 0
	while (history is None) or (history["date"].min() > datetime.fromtimestamp(date_from)) \
		and (distance_from_target_date > timedelta(days=1)):
		date_to = round(time.mktime(history["date"].min().timetuple())) if history is not None else date_to
		history = pd.concat(
			[
				history,
				_fetch_data(ticker,date_from,date_to,granularity)
			],
			ignore_index=True
		)
		distance_from_target_date = history["date"].min() - datetime.fromtimestamp(date_from)
		if distance_from_target_date == prev_distance:
			loop_counter += 1
			if loop_counter == 3:
				print("Warning: Unable to reach target 'date_from'; data not available.")
				break
		else:
			prev_distance = distance_from_target_date
	history.drop_duplicates(inplace=True)
	return history

def get_data(ticker, date_from=None, date_to=None, granularity="day", save=True):
	"""
	Gets data for ticker between chosen date period (date_from defaults to 1000 
	records before date_to; date_to defaults to now).
	Saving collected data improves efficiency for future executions: data will be
	taken from saved records and expanded by fetching any missing data.
	"""
	assert granularity in ("day","week","month")
	
	if not isinstance(date_from, int):
		date_from = _date_format_or_default(date_from, granularity, "start")
	if not isinstance(date_to, int):
		date_to = _date_format_or_default(date_to, granularity, "end")

	data_path = Path("data/")
	csv_path = data_path / f"{ticker.lower()}_{date_from}_{date_to}.csv"
	if csv_path.exists():
		history = pd.read_csv(csv_path)
		return _format_data(history)
	elif sum([ticker.lower() in fname.name for fname in data_path.iterdir()]):
		for fname in data_path.iterdir():
			fname = fname.name
			if ticker.lower() in fname:
				break
		partial_history = _format_data(pd.read_csv(data_path / fname))
		before_to = int(fname.split("_")[1])
		after_from = int(fname.split("_")[2][:-4])

		concat_list = []
		if before_to > date_from:
			concat_list.append(
				_meticulously_fetch_data(ticker, date_from, before_to, granularity),
			)
		concat_list.append(partial_history)
		if after_from < date_to:
			concat_list.append(
				_meticulously_fetch_data(ticker, after_from, date_to, granularity)
			)

		if len(concat_list) > 1:
			## If time period needs extending
			history = pd.concat(
				concat_list,
				ignore_index=True
			)
			(data_path / fname).unlink()
		else:
			## If existing data contains target time period
			print("Filtering existing data...")
			history = partial_history[
				(partial_history["date"] >= datetime.fromtimestamp(date_from)) & \
					(partial_history["date"] <= datetime.fromtimestamp(date_to))
			].reset_index()
			save = False
	else:
		history = _meticulously_fetch_data(ticker, date_from, date_to, granularity)
	
	if save:
		data_path.mkdir(exist_ok=True)
		history.to_csv(csv_path)
	return history

if __name__ == "__main__":
	data = get_data("AAPL",date_from="05/07/2011",date_to="05/07/2021")
	print(data["date"].min(),data["date"].max())
