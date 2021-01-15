import datetime
import databaseConn


def clean_data():
    databaseConn.db_connect()
    print("RUNNING DAILY CLEAN ##################################")
    database_data = databaseConn.DeeDeeData.objects(is_active=True)
    num_symbols_total = 0
    num_symbols_clean = 0
    for tracked_symbol in database_data:
        num_symbols_total += 1
        hot = tracked_symbol['points'][-1]['hot_mentions']
        new = tracked_symbol['points'][-1]['new_mentions']
        com = tracked_symbol['points'][-1]['comment_mentions']
        last_update = tracked_symbol['points'][-1]['poll_time']
        curr_time = datetime.datetime.now()
        time_diff = curr_time - last_update
        time_diff_as_sec = time_diff.total_seconds() / 3600
        if (hot + new + com) <= 1 or time_diff_as_sec > 6:
            databaseConn.DeeDeeData.objects(symbol=tracked_symbol['symbol']).update_one(is_active=False)
            print(tracked_symbol['symbol'] + " is no longer listed as active.")
            num_symbols_clean += 1
    print(str(num_symbols_clean) + " symbols were set inactive.")
    print(str(num_symbols_total - num_symbols_clean) + " symbols are currently active.")


# clean_data()
