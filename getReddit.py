# IMPORT python modules
import json
import praw
import re
import collections
import datetime
import timeit
import time

# IMPORT external files
import getSymbols
import databaseConn
import cleanDB
import finnhub_controller

# consts
run_interval = 5  # the number of minutes between each poll/post
post_limit = 32
subreddits = ["RobinhoodPennyStocks", "PennyStocks"]

# GET the full list of symbols from NASDAQ
stockSymbols = getSymbols.getsymbols()

# Connect to reddit api
reddit = praw.Reddit(client_id='a', client_secret='a', user_agent='a')

# GLOBAL VARIABLES init

# raw data from Reddit
reddit_hot_post_raw_data = []      #
reddit_new_post_raw_data = []      #
reddit_comment_raw_data = []       #
reddit_all_mentions_raw_data = []  # this is the list of unique mentions

# parsed data from Reddit
# raw data after parse
reddit_hot_num_mentions_extracted_data = []        #
reddit_new_num_mentions_extracted_data = []        #
reddit_comment_num_mentions_extracted_data = []    #
# count data after parse
reddit_hot_num_mentions_count_data = {}      #
reddit_new_num_mentions_count_data = {}      #
reddit_comment_num_mentions_count_data = {}  #

# raw data from finnhub
finnhub_raw_data = {}

# this is the 'master' data set that gets set to the database.
# this represents the final, formatted data
master_data = {}


def clear_all_data():
    # clear reddit data
    reddit_hot_post_raw_data.clear()
    reddit_new_post_raw_data.clear()
    reddit_comment_raw_data.clear()
    reddit_all_mentions_raw_data.clear()
    reddit_hot_num_mentions_extracted_data.clear()
    reddit_new_num_mentions_extracted_data.clear()
    reddit_comment_num_mentions_extracted_data.clear()
    reddit_hot_num_mentions_count_data.clear()
    reddit_new_num_mentions_count_data.clear()
    reddit_comment_num_mentions_count_data.clear()

    # clear finnhub data
    finnhub_raw_data.clear()

    # clear the master data
    master_data.clear()
    reddit_all_mentions_raw_data.clear()


# fetch data from the reddit api
def fetch_reddit_data():
    for sub in subreddits:
        # get 32 HOT posts from the subreddit
        try:
            hot_posts = reddit.subreddit(sub).hot(limit=post_limit)
            for post in hot_posts:
                if post.stickied is True:
                    for comment in post.comments:
                        try:
                            reddit_comment_raw_data.append(comment.body)
                        except:
                            print("API for COMMENTS FAILED")
                            pass
                else:
                    reddit_hot_post_raw_data.append(post.title)
        except:
            print("API for HOT posts FAILED")
            pass
        # get 10 NEW posts from the subreddit
        try:
            new_posts = reddit.subreddit(sub).new(limit=post_limit)
            for post in new_posts:
                reddit_new_post_raw_data.append(post.title)
        except:
            print("API for NEW posts FAILED")
            pass


# parse the data from reddit posts by extracting only the stock symbols
def parse_reddit_data():
    # Parse HOT title words
    global reddit_hot_num_mentions_count_data
    global reddit_new_num_mentions_count_data
    global reddit_comment_num_mentions_count_data

    for post_title in reddit_hot_post_raw_data:                # for each post title, in the data from reddit
        words = re.split(' |_|,|\\$|~|-|%|#|@', post_title)    # split it into its individual words
        for word in words:                                     # then for each word found in the title
            if word in stockSymbols:                           # if the word is a stock symbol
                if word not in reddit_all_mentions_raw_data:   # then if it's not already in the total mentions list
                    reddit_all_mentions_raw_data.append(word)  # add it to the total mentions
                reddit_hot_num_mentions_extracted_data.append(word)  # then add it to the list of hot mentions
    reddit_hot_num_mentions_count_data = dict(collections.Counter(reddit_hot_num_mentions_extracted_data))

    # Parse NEW title words
    for post_title in reddit_new_post_raw_data:
        words = re.split(' |_|,|\\$|~|-|%|#|@', post_title)
        for word in words:
            if word in stockSymbols:
                if word not in reddit_all_mentions_raw_data:
                    reddit_all_mentions_raw_data.append(word)
                reddit_new_num_mentions_extracted_data.append(word)
    reddit_new_num_mentions_count_data = dict(collections.Counter(reddit_new_num_mentions_extracted_data))

    # Parse COMMENT data
    for comment_body in reddit_comment_raw_data:
        words = re.split(' |_|,|\\$|~|-|%|#|@', comment_body)
        for word in words:
            if word in stockSymbols:
                if word not in reddit_all_mentions_raw_data:
                    reddit_all_mentions_raw_data.append(word)
                reddit_comment_num_mentions_extracted_data.append(word)
    reddit_comment_num_mentions_count_data = dict(collections.Counter(reddit_comment_num_mentions_extracted_data))


# export the data to a text file for log checking
def export_data_to_file():
    with open('output_Logs/output.txt', 'w') as file:
        file.write(json.dumps(master_data))


def combine_data_for_posting():
    for stock_symbol in reddit_all_mentions_raw_data:
        # Create the master data for the symbol
        try:
            master_data[stock_symbol] = {}
            master_data[stock_symbol]['symbol'] = stock_symbol
            # Create the point
            master_data[stock_symbol]['point'] = {}
            # Add price data to the point
            master_data[stock_symbol]['point']['price'] = finnhub_raw_data[stock_symbol]['price']
            master_data[stock_symbol]['point']['price_high'] = finnhub_raw_data[stock_symbol]['price_high']
            master_data[stock_symbol]['point']['price_low'] = finnhub_raw_data[stock_symbol]['price_low']
            master_data[stock_symbol]['point']['price_open'] = finnhub_raw_data[stock_symbol]['price_open']
            master_data[stock_symbol]['point']['price_pvcl'] = finnhub_raw_data[stock_symbol]['price_pvcl']
            # Add candle data to the point
            master_data[stock_symbol]['point']['volume_1'] = finnhub_raw_data[stock_symbol]['volume_1']
            master_data[stock_symbol]['point']['volume_2'] = finnhub_raw_data[stock_symbol]['volume_2']
            master_data[stock_symbol]['point']['volume_3'] = finnhub_raw_data[stock_symbol]['volume_3']
            master_data[stock_symbol]['point']['volume_4'] = finnhub_raw_data[stock_symbol]['volume_4']
            master_data[stock_symbol]['point']['high_1'] = finnhub_raw_data[stock_symbol]['high_1']
            master_data[stock_symbol]['point']['low_1'] = finnhub_raw_data[stock_symbol]['low_1']
            master_data[stock_symbol]['point']['high_2'] = finnhub_raw_data[stock_symbol]['high_2']
            master_data[stock_symbol]['point']['low_2'] = finnhub_raw_data[stock_symbol]['low_2']
            # Add reddit data to the point
            if stock_symbol in reddit_hot_num_mentions_extracted_data:
                master_data[stock_symbol]['point']['hot_mentions'] = reddit_hot_num_mentions_count_data[stock_symbol]
            else:
                master_data[stock_symbol]['point']['hot_mentions'] = 0
            if stock_symbol in reddit_new_num_mentions_extracted_data:
                master_data[stock_symbol]['point']['new_mentions'] = reddit_new_num_mentions_count_data[stock_symbol]
            else:
                master_data[stock_symbol]['point']['new_mentions'] = 0
            if stock_symbol in reddit_comment_num_mentions_extracted_data:
                master_data[stock_symbol]['point']['comment_mentions'] = reddit_comment_num_mentions_count_data[stock_symbol]
            else:
                master_data[stock_symbol]['point']['comment_mentions'] = 0
        except:
            del master_data[stock_symbol]
            pass


# sends the data to databaseConn for posting
def post_data():
    try:
        databaseConn.db_post(master_data)
        print("SUCCESS: Data posted!")
    except NameError as err:
        print("ERROR: Unable to post data.")
        print(err)


# handles the function sequence and operates the program
def run_program():
    global finnhub_raw_data
    # STEP 1: Get all of the reddit data and parse it to find the symbols being discussed
    fetch_reddit_data()
    parse_reddit_data()

    # STEP 2: Get all of the finnhub data for each reddit symbol and add it to its Dictionary
    finnhub_raw_data = finnhub_controller.fetch_finnhub_data(reddit_all_mentions_raw_data)

    # STEP 3: Put it all together
    combine_data_for_posting()

    # STEP 4: Send all of that data to the database, and print debug to the console if needed
    post_data()
    export_data_to_file()


def main():
    while True:
        run = input("\n\n\n\n[S to start watching] / [R to run query] / [C to clean DB] / [E to close] : ")
        if run.lower() == "s":
            run = True
            while run is True:
                current_time = datetime.datetime.now()
                while current_time.minute % run_interval != 0:
                    print("Next Pull in " + str(run_interval - (current_time.minute % run_interval)) + " minutes.")
                    time.sleep(60)
                    current_time = datetime.datetime.now()
                start = timeit.default_timer()
                print("Fetching data... at: " + str(current_time.hour) + " : " + str(current_time.minute))
                run_program()
                clear_all_data()
                stop = timeit.default_timer()
                print('\nExecution Time: ' + str(stop - start) + " seconds (" + str((stop - start) / 60) + ") mins")
                if current_time.minute % 20 == 0:
                    cleanDB.clean_data()
                time.sleep(30)
        elif run.lower() == "r":
            start = timeit.default_timer()
            subs = ""
            for sub in subreddits:
                subs += sub + " "
            print("Fetching data from: " + subs)
            run_program()
            stop = timeit.default_timer()
            print('\nExecution Time: ' + str(stop - start) + " seconds")
        elif run.lower() == "c":
            cleanDB.clean_data()
        elif run.lower() == "e":
            break
        else:
            print(">> ! Invalid Instruction")


main()
