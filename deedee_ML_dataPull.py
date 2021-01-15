from mongoengine import *
import databaseConn
import datetime
import csv
import os

# The directory where the training CSV's are located
TRAINING_DIRECTORY = "training_data/symbol_csvs/"
LIVE_DIRECTORY = "live_data/symbol_csvs/"


# Deletes all of the CSV files in the training directory
def clean_data_directory(directory):
    try:
        # add all of the files in the directory to the 'filelist'
        filelist = [f for f in os.listdir(directory) if f.endswith(".csv")]
        # delete them
        for f in filelist:
            os.remove(os.path.join(directory, f))
    except FileNotFoundError:
        print("Didn't find the deletion directory. Probably cause it doesn't exist.")
        pass
    except:
        pass


# this function gets the percent change between the current price and the remaining high for the day
# it also returns bear/bull information based on these two future values
def get_percent_change(points_array, current_point_price):
    percent_change_up = 0.0
    percent_change_dn = 0.0

    bull_ahead = 0.0
    bear_ahead = 0.0

    remaining_day_high = 0.0
    remaining_day_low = 99999.0
    prev_time = datetime.datetime.min
    for pt in points_array:
        check_time = pt['poll_time']
        if prev_time.hour > check_time.hour:
            percent_change_up = (remaining_day_high - current_point_price) / current_point_price
            percent_change_dn = (remaining_day_low - current_point_price) / current_point_price
            break
        if remaining_day_high < float(pt['price']) and float(pt['price']) > 0.01:
            remaining_day_high = float(pt['price'])
        if remaining_day_low > float(pt['price']) > 0.01:
            remaining_day_low = float(pt['price'])
        prev_time = check_time

    # bull or bear if the next price is +/- 10% -- these are the breakouts we're looking for
    if remaining_day_high > (current_point_price * 1.10):
        bull_ahead = 1.0
    if remaining_day_low < (current_point_price * 0.90):
        bear_ahead = 1.0

    return percent_change_up, percent_change_dn, bull_ahead, bear_ahead


# this function gets the number of points to the next open
# since the data is in points and not in 'time'
def get_distance_to_next_day_open_point(points_array, current_point_price):
    # get the current points poll_time info
    current_point_time = points_array[0]['poll_time']

    # find the desired next trading day (default to monday, or the next weekday)
    desired_weekday = 0  # 0 = monday, if current day is (fri,sat,sun)
    if current_point_time.weekday() < 4:  # if it is 0, 1, 2, 3 (mon,tue,wed,thu)
        desired_weekday = current_point_time.weekday() + 1  # desired day is next weekday

    # proceed forward through the points, looking for the point that's at the next day's open
    distance_in_points = 0
    for pt in points_array:
        next_time = pt['poll_time']
        if (next_time.weekday() == desired_weekday) and (next_time.hour == 9) and (next_time.minute > 30):
            return distance_in_points
        distance_in_points += 1
    return 0


# Pulls data from the MongoDB and creates CSV's
# this is the raw training data that is parsed to train the model
def pull_data_and_create_csv():
    # Connect to the Database
    databaseConn.db_connect()
    print("######## GETTING DATA FOR LEARNING ########")

    # Clean out the training data folder
    clean_data_directory(TRAINING_DIRECTORY)
    print("Directory cleared...")

    # Pull all data from the DB
    # This pull needs to be optimized as the dataset is getting quite large
    print("Fetching data...")
    database_data = databaseConn.DeeDeeData.objects()

    num_symbols_returned = 0  # tracks the number of symbols we have
    for tracked_symbol in database_data:
        # the number of hours that are required in order to run the calculation (**these are NOT continuous**)
        pull_hour_span = 6
        # (60min / 5 pulls per minute) * (span of hours) = hour-span of data
        get_num_points = (60 / 5) * pull_hour_span

        # if the stock symbol has less than "get_num_points" points
        # (equivalent to "pull_hour_span" tracking hours, with polls every 5 min)
        # then we skip it since we don't have enough data to train on
        if (len(tracked_symbol['points']) < get_num_points) or ("vol_1" not in tracked_symbol['points'][-1]):
            print("! -- symbol ** " + tracked_symbol['symbol'] + " ** skipped due to insufficient data.")
            continue

        # counts the number of valid symbols, just for reference
        num_symbols_returned += 1

        # sets the directory of the CSV file to be saved, naming it like: SYMBOL.csv
        symbol_file_path = TRAINING_DIRECTORY + tracked_symbol['symbol'] + ".csv"
        print(symbol_file_path)

        # counts the number of data points, also just for reference
        num_data_points = 0

        # open the .csv file with the current symbol in 'write' mode
        with open(symbol_file_path, 'w', newline='') as csvfile:
            spamwriter = csv.writer(csvfile)
            # initializing the column headers
            spamwriter.writerow([
                "red_hot",
                "red_new",
                "red_comment",
                "price_hi",
                "price_lo",
                "price_op",
                "price_pc",
                "vol_1",
                "vol_2",
                "vol_3",
                "vol_4",
                "hi_1",
                "lo_1",
                "hi_2",
                "lo_2",
                "hour",
                "min",
                "dayWk",
                "price",
                # "delta_1",
                # "up_in_1",
                # "dn_in_1",
                # "delta_3",
                # "up_in_3",
                # "dn_in_3",
                # "delta_12",
                # "up_in_12",
                # "dn_in_12",
                "delta_up",
                "delta_dn",
                "bull_ahead",
                "bear_ahead",
                "delta_cl",
                "close_up",
                "close_dn",
                "delta_o",
                "open_up",
                "open_dn"
            ])
            # loop through all of the points, add the row to the csv file, excluding the last 4 hours
            for i in range(len(tracked_symbol['points']) - 48):
                try:
                    # get the poll_time of the data-point and make it a date-time object
                    timestamp = tracked_symbol['points'][i]['poll_time']
                    time_check = datetime.datetime.now()

                    # if the data point doesn't have the new document type with 17 inputs, skip that data point
                    # alternative is accepting legacy data, and setting vol_1, etc to 0 .. this may harm the model
                    if "vol_1" not in tracked_symbol['points'][i] or \
                            float(tracked_symbol['points'][i]['price']) == 0 or \
                            (timestamp.day == time_check.day and timestamp.month == time_check.month) or \
                            (timestamp.hour < 7) or \
                            (timestamp.hour > 16) or \
                            (timestamp.weekday() > 4):
                        continue
                    # otherwise, we're all set to add our row to the .csv file

                    price_raw = float(tracked_symbol['points'][i]['price'])
                    red_hot = float(tracked_symbol['points'][i]['hot_mentions'])
                    red_new = float(tracked_symbol['points'][i]['new_mentions'])
                    red_comment = float(tracked_symbol['points'][i]['comment_mentions'])
                    price_hi = float(tracked_symbol['points'][i]['price_h'])
                    price_lo = float(tracked_symbol['points'][i]['price_l'])
                    price_op = float(tracked_symbol['points'][i]['price_o'])
                    price_pc = float(tracked_symbol['points'][i]['price_pc'])
                    vol_1 = float(tracked_symbol['points'][i]['vol_1'])
                    vol_2 = float(tracked_symbol['points'][i]['vol_2'])
                    vol_3 = float(tracked_symbol['points'][i]['vol_3'])
                    vol_4 = float(tracked_symbol['points'][i]['vol_4'])
                    hi_1 = float(tracked_symbol['points'][i]['hi_1'])
                    lo_1 = float(tracked_symbol['points'][i]['lo_1'])
                    hi_2 = float(tracked_symbol['points'][i]['hi_2'])
                    lo_2 = float(tracked_symbol['points'][i]['lo_2'])

                    delta_up, delta_dn, bull_ahead, bear_ahead = get_percent_change(tracked_symbol['points'][i:], price_raw)

                    # get the number of points to the next day's open
                    dist = get_distance_to_next_day_open_point(tracked_symbol['points'][i:], price_raw)
                    # ------------------ price at close
                    delta_cl = (float(tracked_symbol['points'][i + dist]['price']) - price_raw) / price_raw
                    close_up = 1.0 if float(tracked_symbol['points'][i + dist]['price_pc']) > (price_raw * 1.025) else 0.0
                    close_dn = 1.0 if float(tracked_symbol['points'][i + dist]['price_pc']) < (price_raw * 0.975) else 0.0
                    # ------------------ price at next open
                    delta_o = (float(tracked_symbol['points'][i + dist]['price']) - price_raw) / price_raw
                    open_up = 1.0 if float(tracked_symbol['points'][i + dist]['price_o']) > (price_raw * 1.025) else 0.0
                    open_dn = 1.0 if float(tracked_symbol['points'][i + dist]['price_o']) < (price_raw * 0.975) else 0.0

                    # write it all to the row
                    spamwriter.writerow([
                        red_hot,
                        red_new,
                        red_comment,
                        price_hi,
                        price_lo,
                        price_op,
                        price_pc,
                        vol_1,
                        vol_2,
                        vol_3,
                        vol_4,
                        hi_1,
                        lo_1,
                        hi_2,
                        lo_2,
                        timestamp.hour / 23.0,
                        timestamp.minute / 59.0,
                        timestamp.weekday() / 4.0,
                        price_raw,
                        delta_up,
                        delta_dn,
                        bull_ahead,
                        bear_ahead,
                        delta_cl,
                        close_up,
                        close_dn,
                        delta_o,
                        open_up,
                        open_dn
                    ])
                    # increment the row counter
                    num_data_points += 1
                except:
                    pass
        print(">>> Added {" + str(num_data_points) + "} data-points to " + tracked_symbol['symbol'])

    print(str(num_symbols_returned) + " symbols pulled from the database and saved as CSV's.")


# pulls only the data for desired symbols and stores it for model usage
# this is used in the finished ML Model on the most recent data
# results are used directly for paper trading
def pull_data_and_create_csv_livedata():
    # Connect to the Database
    databaseConn.db_connect()
    print("######## GETTING DATA FOR PREDICTION ########")

    # Clean out the training data folder
    clean_data_directory(LIVE_DIRECTORY)
    print("Directory cleared: " + LIVE_DIRECTORY)

    # Pull all data from the DB
    # This pull needs to be optimized as the dataset is getting quite large
    print("Fetching data...")
    database_data = databaseConn.DeeDeeData.objects(is_active=True).order_by('-r_index').limit(10)

    num_symbols_returned = 0  # tracks the number of symbols we have
    for tracked_symbol in database_data:
        culled_list = []
        # loop through all of the points, add the row to the csv file, excluding the last 4 hours
        for i in range(len(tracked_symbol['points'])):
            # get the poll_time of the data-point and make it a date-time object
            timestamp = tracked_symbol['points'][i]['poll_time']

            # if the data point doesn't have the new document type with 17 inputs, skip that data point
            # alternative is accepting legacy data, and setting vol_1, etc to 0 .. this may harm the model
            if "vol_1" not in tracked_symbol['points'][i] or \
                    (timestamp.hour < 6) or \
                    (timestamp.hour > 17) or \
                    (timestamp.weekday() > 4):
                continue
            else:
                culled_list.append(tracked_symbol['points'][i])

        # minimum number of points to be considered for training
        get_num_points = 48

        # if the stock symbol has less than "get_num_points" points
        # (equivalent to "pull_hour_span" tracking hours, with polls every 5 min)
        # then we skip it since we don't have enough data to train on
        if len(culled_list) < get_num_points:
            print("! -- symbol ** " + tracked_symbol['symbol'] + " ** skipped due to insufficient data.")
            continue

        # counts the number of valid symbols, just for reference
        num_symbols_returned += 1

        # sets the directory of the CSV file to be saved, naming it like: SYMBOL.csv
        symbol_file_path = LIVE_DIRECTORY + tracked_symbol['symbol'] + ".csv"

        # counts the number of data points, also just for reference
        num_data_points = 0

        # open the .csv file with the current symbol in 'write' mode
        with open(symbol_file_path, 'w', newline='') as csvfile:
            spamwriter = csv.writer(csvfile)
            # initializing the column headers
            spamwriter.writerow([
                "red_hot",
                "red_new",
                "red_comment",
                "price_hi",
                "price_lo",
                "price_op",
                "price_pc",
                "vol_1",
                "vol_2",
                "vol_3",
                "vol_4",
                "hi_1",
                "lo_1",
                "hi_2",
                "lo_2",
                "hour",
                "min",
                "dayWk",
                "price"
            ])

            for i in range(-48, 0):
                try:
                    # get the poll_time of the data-point and make it a date-time object
                    timestamp = culled_list[i]['poll_time']

                    price_raw = float(culled_list[i]['price'])
                    red_hot = float(culled_list[i]['hot_mentions'])
                    red_new = float(culled_list[i]['new_mentions'])
                    red_comment = float(culled_list[i]['comment_mentions'])
                    price_hi = float(culled_list[i]['price_h'])
                    price_lo = float(culled_list[i]['price_l'])
                    price_op = float(culled_list[i]['price_o'])
                    price_pc = float(culled_list[i]['price_pc'])
                    vol_1 = float(culled_list[i]['vol_1'])
                    vol_2 = float(culled_list[i]['vol_2'])
                    vol_3 = float(culled_list[i]['vol_3'])
                    vol_4 = float(culled_list[i]['vol_4'])
                    hi_1 = float(culled_list[i]['hi_1'])
                    lo_1 = float(culled_list[i]['lo_1'])
                    hi_2 = float(culled_list[i]['hi_2'])
                    lo_2 = float(culled_list[i]['lo_2'])

                    # write it all to the row
                    spamwriter.writerow([
                        red_hot,
                        red_new,
                        red_comment,
                        price_hi,
                        price_lo,
                        price_op,
                        price_pc,
                        vol_1,
                        vol_2,
                        vol_3,
                        vol_4,
                        hi_1,
                        lo_1,
                        hi_2,
                        lo_2,
                        timestamp.hour / 23.0,
                        timestamp.minute / 59.0,
                        timestamp.weekday() / 4.0,
                        price_raw
                    ])
                    # increment the row counter
                    num_data_points += 1
                except Exception as err:
                    print(err)
                    pass
        print(">>> Added {" + str(num_data_points) + "} data-points to " + tracked_symbol['symbol'])

    print(str(num_symbols_returned) + " symbols pulled from the database and saved as CSV's.")


# run the data pull
# keep commented out unless you're testing the pull data in excel
# pull_data_and_create_csv()
