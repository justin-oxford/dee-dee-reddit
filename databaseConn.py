import datetime
from mongoengine import *
import timeit

DB_URI = "MONGO"


# DEPRECIATED high/lows over different time-frames
class SpreadField(EmbeddedDocument):
    high = FloatField(min_value=0.0, default=0.0)
    low = FloatField(min_value=0.0, default=9999.0)


# These are the individual points containing time-related data
class PointField(EmbeddedDocument):
    price = DecimalField(min_value=0.0)
    price_h = DecimalField(min_value=0.0)
    price_l = DecimalField(min_value=0.0)
    price_o = DecimalField(min_value=0.0)
    price_pc = DecimalField(min_value=0.0)
    vol_1 = DecimalField(min_value=0.0)
    vol_2 = DecimalField(min_value=0.0)
    vol_3 = DecimalField(min_value=0.0)
    vol_4 = DecimalField(min_value=0.0)
    hi_1 = DecimalField(min_value=0.0)
    lo_1 = DecimalField(min_value=0.0)
    hi_2 = DecimalField(min_value=0.0)
    lo_2 = DecimalField(min_value=0.0)
    hot_mentions = IntField(min_value=0)
    new_mentions = IntField(min_value=0)
    comment_mentions = IntField(min_value=0)
    poll_time = DateTimeField(default=datetime.datetime.now)


# dee dee ML stats
class DeeDeeMLStats(EmbeddedDocument):
    delta_up = DecimalField(default=0.0)
    delta_dn = DecimalField(default=0.0)
    bull_ahead = DecimalField(default=0.0)
    bear_ahead = DecimalField(default=0.0)
    delta_cl = DecimalField(default=0.0)
    close_up = DecimalField(default=0.0)
    close_dn = DecimalField(default=0.0)
    delta_o = DecimalField(default=0.0)
    open_up = DecimalField(default=0.0)
    open_dn = DecimalField(default=0.0)


# Data-type Definitions
class DeeDeeData(Document):
    symbol = StringField(required=True)
    date_added = DateTimeField(default=datetime.datetime.now)
    daily_spread = EmbeddedDocumentField(SpreadField)    # DEPRECIATED
    weekly_spread = EmbeddedDocumentField(SpreadField)   # DEPRECIATED
    monthly_spread = EmbeddedDocumentField(SpreadField)  # DEPRECIATED
    points = EmbeddedDocumentListField(PointField)
    is_active = BooleanField(required=True)
    active_track = IntField(required=True)
    r_index = IntField(required=True)
    ml_stats = EmbeddedDocumentField(DeeDeeMLStats)


def db_connect():
    connect(host=DB_URI)


def db_post(data):
    db_connect()
    for symbol in data:
        # add if not in database
        print(symbol)
        start = timeit.default_timer()
        R_INDEX = data[symbol]['point']['hot_mentions'] + data[symbol]['point']['new_mentions'] + data[symbol]['point'][
            'comment_mentions']
        if not DeeDeeData.objects(symbol=data[symbol]["symbol"]):
            print("     Adding " + data[symbol]['symbol'] + " to the database.")
            dep_spread = SpreadField(  # initialize this to the price for the first pull
                high=0.0,
                low=0.0
            )
            point = PointField(
                price=data[symbol]['point']['price'],
                price_h=data[symbol]['point']['price_high'],
                price_l=data[symbol]['point']['price_low'],
                price_o=data[symbol]['point']['price_open'],
                price_pc=data[symbol]['point']['price_pvcl'],
                vol_1=data[symbol]['point']['volume_1'],
                vol_2=data[symbol]['point']['volume_2'],
                vol_3=data[symbol]['point']['volume_3'],
                vol_4=data[symbol]['point']['volume_4'],
                hi_1=data[symbol]['point']['high_1'],
                lo_1=data[symbol]['point']['low_1'],
                hi_2=data[symbol]['point']['high_2'],
                lo_2=data[symbol]['point']['low_2'],
                hot_mentions=data[symbol]['point']['hot_mentions'],
                new_mentions=data[symbol]['point']['new_mentions'],
                comment_mentions=data[symbol]['point']['comment_mentions'],
                poll_time=datetime.datetime.now()
            )
            post_ml = DeeDeeMLStats(
                delta_up=0.0,
                delta_dn=0.0,
                bull_ahead=0.0,
                bear_ahead=0.0,
                delta_cl=0.0,
                close_up=0.0,
                close_dn=0.0,
                delta_o=0.0,
                open_up=0.0,
                open_dn=0.0
            )
            post = DeeDeeData(
                symbol=data[symbol]['symbol'],
                date_added=datetime.datetime.now(),
                daily_spread=dep_spread,
                weekly_spread=dep_spread,
                monthly_spread=dep_spread,
                points=[point],
                is_active=True,
                active_track=1,
                r_index=R_INDEX,
                ml_stats=post_ml
            )
            post.save()
            stop = timeit.default_timer()
            print("     ...Done  (" + str(stop - start) + ")")
        # update if is in database
        else:
            print("     Updating " + data[symbol]['symbol'] + "...")
            point = PointField(
                price=data[symbol]['point']['price'],
                price_h=data[symbol]['point']['price_high'],
                price_l=data[symbol]['point']['price_low'],
                price_o=data[symbol]['point']['price_open'],
                price_pc=data[symbol]['point']['price_pvcl'],
                vol_1=data[symbol]['point']['volume_1'],
                vol_2=data[symbol]['point']['volume_2'],
                vol_3=data[symbol]['point']['volume_3'],
                vol_4=data[symbol]['point']['volume_4'],
                hi_1=data[symbol]['point']['high_1'],
                lo_1=data[symbol]['point']['low_1'],
                hi_2=data[symbol]['point']['high_2'],
                lo_2=data[symbol]['point']['low_2'],
                hot_mentions=data[symbol]['point']['hot_mentions'],
                new_mentions=data[symbol]['point']['new_mentions'],
                comment_mentions=data[symbol]['point']['comment_mentions'],
                poll_time=datetime.datetime.now()
            )
            DeeDeeData.objects(symbol=data[symbol]['symbol']).update_one(push__points=point)
            DeeDeeData.objects(symbol=data[symbol]['symbol']).update_one(is_active=True)
            DeeDeeData.objects(symbol=data[symbol]['symbol']).update_one(active_track=1)
            DeeDeeData.objects(symbol=data[symbol]['symbol']).update_one(r_index=R_INDEX)
            stop = timeit.default_timer()
            print("     ...Done  (" + str(stop - start) + ")")


def db_post_ml_data(
        symbol,
        ml_data_delta_up,
        ml_data_delta_dn,
        ml_data_bull_ahead,
        ml_data_bear_ahead,
        ml_data_delta_cl,
        ml_data_close_up,
        ml_data_close_dn,
        ml_data_delta_o,
        ml_data_open_up,
        ml_data_open_dn
):
    post_ml = DeeDeeMLStats(
        delta_up=ml_data_delta_up,
        delta_dn=ml_data_delta_dn,
        bull_ahead=ml_data_bull_ahead,
        bear_ahead=ml_data_bear_ahead,
        delta_cl=ml_data_delta_cl,
        close_up=ml_data_close_up,
        close_dn=ml_data_close_dn,
        delta_o=ml_data_delta_o,
        open_up=ml_data_open_up,
        open_dn=ml_data_open_dn
    )
    DeeDeeData.objects(symbol=symbol).update_one(ml_stats=post_ml, upsert=True)
