import datetime
import time
import json
from mongoengine import *
from pymongo import *
import timeit

DB_URI = "MONGODBLINK"

### CONSTANTS #################
DIVERSITY_MAX_PERCENT = 0.1   # the maximum percent a stock can be of the portfolio
SELL_LIMIT = 0.98             # the threshhold for selling
###############################


#
class PositionField(EmbeddedDocument):
    num_shares = IntField(min_value=0, max_value=2000)
    buy_price = DecimalField()
    target_price = DecimalField()
    stop_loss = DecimalField()


#
class PaperPosition(EmbeddedDocument):
    symbol = StringField(required=True)
    position = EmbeddedDocumentField(PositionField)


class BalanceArray(EmbeddedDocument):
    balance = DecimalField(required=True)
    timestamp = DateTimeField(required=True, default=datetime.datetime.now())


#
class PaperAccount(Document):
    current_balance = DecimalField(required=True)
    buying_power = DecimalField(required=True)
    equity = DecimalField(required=True)
    starting_balance = DecimalField(min_value=0.0)
    balance_array = EmbeddedDocumentListField(BalanceArray)
    positions = EmbeddedDocumentListField(PaperPosition)
    bot_status = StringField()


## get the ML data from the model for active symbols, return it as a DICTIONARY
def get_ml_data():
    return []


# get the current portfolio info so we know how much we have to work with and whether or not we already own stock
def get_current_portfolio():
    return []


# function for buying a position
def buy_position(symbol, num_shares_to_buy):
    return []


# function for selling a position
def sell_position(symbol, num_shares_to_sell):
    return []


# decide based off of current active symbols whether or not to buy/sell/hold/ignore/etc
def trading_algorithm(ml_data, current_portfolio):
    # CONSTANTS
    temp_share_volume_limit = 1000

    # for each stock that is active and has been put through the model
    for stock in ml_data:
        # if its current position is less that DIVERSITY_MAX_PERCENT of the portfolio
        if current_portfolio.stock.symbol.balance < (current_portfolio.current_balance * DIVERSITY_MAX_PERCENT):
            # if the stock is likely going up and is less than $10
            if stock.future > (stock.current * 1.05) and stock.current < 10.00:
                # buy position
                buy_position(stock.symbol, temp_share_volume_limit)
        # if the stop loss is triggered OR the future price is expected down then sell
        if current_portfolio.positions.stock.position.stop_loss == stock.currnet_price or \
                stock.future < (stock.current * 0.98):
            # sell position
            sell_position(stock.symbol, temp_share_volume_limit)


    return []


# update database with latest portfolio data
def update_dee_dee_portfolio(portfolio_data):
    # set the current balance to buying power ++ equity
    curr_bal = buying_power + equity

    # push the current balance to the historical data
    balance_array.push__(curr_bal, timestamp)

    # update the positions in the database
    positions.update(positions)


# main loop to handle program flow
def paper_main():
    while True:
        # get the ML data from the model for active symbols, return it as a DICTIONARY
        ml_data = get_ml_data()

        # get the current portfolio info so we know how much we have to work with and whether or not we already own stock
        current_portfolio = get_current_portfolio()

        # decide based off of current active symbols whether or not to buy/sell/hold/ignore/etc
        portfolio_data = trading_algorithm(ml_data, current_portfolio)

        # update database with latest portfolio data
        update_dee_dee_portfolio(portfolio_data)

        # sleep for 5 minutes
        time.sleep(60 * 5)


# function to give dee dee an input amount of credits
def give_dee_dee_credits(credit_amt):
    # set dee dee credits to credit_amt
    amt = credit_amt
    print("Dee Dee now has: $" + str(amt))



