
import pandas as pd
from datetime import timedelta
if __name__ == "__main__":
	from helpers import parse_date
else:
	from .helpers import parse_date

def cost_average(data,start,end,period_days,quantity=None,value=None, fractional_stocks=False):
	"""
	Cost-averaging strategy:
		Buy 'quantity' or 'value' of stock at intermittent periods, between 'start' and 'end'.
	"""
	assert quantity != value
	data = data.copy()
	data.sort_values("date",ascending=False,inplace=True,ignore_index=True)

	start = parse_date(start)
	end = parse_date(end)

	# data = data[data["date"] >= start & data["date"] <= end]

	portfolio = []
	surpluss = 0
	period_start = data[data["date"] >= start].iloc[-1]["date"]
	while period_start <= end - timedelta(days=period_days):
		period_state = data[data["date"] == period_start].iloc[0]
		cost = (period_state["open"] + period_state["close"]) / 2
		if value:
			quantity = (value + surpluss) / cost if fractional_stocks else int((value + surpluss) / cost)
		buy_cost = quantity * cost
		surpluss += value - buy_cost
		cum_quantity = quantity if len(portfolio) == 0 else portfolio[-1]["cum_quantity"] + quantity
		portfolio.append(
			{
				"date": period_start,
				"cost": cost,
				"quantity": quantity,
				"buy_cost": buy_cost,
				"cum_quantity": cum_quantity,
				"cum_cost": buy_cost if len(portfolio) == 0 else portfolio[-1]["cum_cost"] + buy_cost,
				"current_total_value": cum_quantity * cost
			}
		)
		try:
			period_start = data[
				data["date"] >= period_start + timedelta(days=period_days)
			].iloc[-1]["date"]
		except IndexError:
			break
	end_state = data[data["date"] <= end].iloc[0]
	end_cost = (end_state["open"] + end_state["close"])/2
	end_quantity = portfolio[-1]["cum_quantity"]
	portfolio.append(
		{
			"date": end_state["date"],
			"cost": end_cost,
			"quantity": pd.NA,
			"buy_cost": pd.NA,
			"cum_quantity": end_quantity,
			"cum_cost": portfolio[-1]["cum_cost"],
			"current_total_value": end_quantity * end_cost
		}
	)
	portfolio_df = pd.DataFrame(portfolio)
	
	full_portfolio_df = pd.merge(
		data[(data["date"] >= start) & (data["date"] <= end)],
		portfolio_df,
		how="left",
		on="date"
	)\
		.sort_values(by="date",ascending=True)\
			.reset_index(drop=True)
	
	## Fill missing vals
	full_portfolio_df["cost"] = ( full_portfolio_df["open"] + full_portfolio_df["close"] ) / 2
	# full_portfolio_df[["quantity","buy_cost"]] = full_portfolio_df[["quantity","buy_cost"]].fillna(value=0)
	full_portfolio_df[["cum_quantity","cum_cost"]] = full_portfolio_df[["cum_quantity","cum_cost"]].fillna(method="ffill")
	full_portfolio_df["current_total_value"] = full_portfolio_df["cum_quantity"] * full_portfolio_df["cost"]

	return full_portfolio_df[portfolio_df.columns].drop(["quantity","buy_cost"],axis=1)

if __name__ == "__main__":
	import os
	data = pd.read_csv("data/" + os.listdir("data")[1],parse_dates=["date"],index_col=0)
	cost_average(data,data["date"].min(),data["date"].max(),30,value=500,fractional_stocks=True)
