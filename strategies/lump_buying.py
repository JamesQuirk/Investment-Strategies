from .helpers import parse_date

def lump_buy(data,start,end,quantity=None,value=None,fractional_stocks=False):
	"""
	Lump-buying strategy:
		Buy a stock for 'value' or 'quantity' ASAP after 'start' and return the net final value at 'end'.
	"""
	assert quantity != value
	data = data.copy()
	data.sort_values("date",ascending=False,inplace=True,ignore_index=True)

	start = parse_date(start)
	end = parse_date(end)

	start_state = data[data["date"] > start].iloc[-1]
	cost = (start_state["open"] + start_state["close"]) / 2
	if value:
		quantity = value / cost if fractional_stocks else int(value / cost)
	total_cost = cost * quantity

	end_state = data[data["date"] < end].iloc[0]
	value = (end_state["open"] + end_state["close"]) / 2
	total_value = value * quantity

	portfolio = data[(data["date"] >= start) & (data["date"] <= end)][["date","open","close"]].reset_index(drop=True)
	portfolio["cost"] = (portfolio["open"] + portfolio["close"]) / 2
	portfolio["current_total_value"] = portfolio["cost"] * quantity
	portfolio["cum_cost"] = total_cost
	portfolio["cum_quantity"] = quantity
	portfolio.drop(["cost","open","close"],axis=1,inplace=True)
	portfolio.sort_values(by="date",ascending=True,inplace=True,ignore_index=True)
	return portfolio

