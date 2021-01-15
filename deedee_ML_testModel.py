import os
import random
import deedee_ML_dataPull
import databaseConn
from numpy import asarray
from pandas import read_csv
import tensorflow as tf

n_steps = 48

# split sequence into samples
def split_sequence(sequence_in, n_steps):
    # define X (input)
    X = list()
    # find the end of this pattern
    end_ix = n_steps
    # print(end_ix)
    seq_x = sequence_in[:end_ix]
    # print(seq_x)
    X.append(seq_x)
    return asarray(X)


def get_prediction(symbol_to_train):
    # load the dataset
    train_symbol = symbol_to_train
    train_file_path = 'live_data/symbol_csvs/%s' % train_symbol

    print("Prediction For: " + symbol_to_train)

    # load the dataset from csv using pandas
    df_in = read_csv(
        train_file_path,
        delimiter=',',
        header=0,
        index_col=None,
        usecols=[
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
        ]
    )

    # retrieve the values as floating point numbers
    values_in = df_in.values.astype('float32')
    # print(values_in)

    X = split_sequence(values_in, n_steps)
    # print(X.shape)

    # reshape into [samples, timesteps, features]
    X = X.reshape((X.shape[0], X.shape[1], 19))
    # print(X)

    X_predict = X

    test_model = tf.keras.models.load_model('save_model/model')

    row = X_predict
    # print("Prediction on: " + train_symbol)
    # print("Based on this sequence (most recent 64 points):")
    guess = test_model.predict(row)
    # print("---\nDelta Up: %.3f" % guess[0][0])
    # print("Delta Dn: %.3f" % guess[0][1])
    # print("---\nBull Ahead?: %.3f" % guess[0][2])
    # print("Bear Ahead?: %.3f" % guess[0][3])
    # print("---\nDelta CL: %.3f" % guess[0][4])
    # print("Close UP: %.3f" % guess[0][5])
    # print("Close DN: %.3f" % guess[0][6])
    # print("---\nDelta OP: %.3f" % guess[0][7])
    # print("Open UP: %.3f" % guess[0][8])
    # print("Open DN: %.3f" % guess[0][9])
    # print(">>> Uploading predictions to DB")
    try:
        databaseConn.db_post_ml_data(
            train_symbol[:-4],
            guess[0][0],
            guess[0][1],
            guess[0][2],
            guess[0][3],
            guess[0][4],
            guess[0][5],
            guess[0][6],
            guess[0][7],
            guess[0][8],
            guess[0][9],
        )
    except Exception as err:
        print("<<< Upload of ML Data Failed for "+ train_symbol[:-4])
        print(err)
        pass
    else:
        print("<<< ML Data Posted for " + train_symbol[:-4])


def deedee_predict_main():
    # pull the data from the database
    deedee_ML_dataPull.pull_data_and_create_csv_livedata()

    # for each file in the directory, train on that file
    file_names = []
    for filename in os.listdir('live_data/symbol_csvs'):
        file_names.append(filename)
    random.shuffle(file_names)

    databaseConn.db_connect()

    for file in file_names:
        get_prediction(file)

    print("Model prediction completed.")

