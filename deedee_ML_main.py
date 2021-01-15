# lstm for time series forecasting
import os
import random
import timeit
import statistics
from numpy import asarray
from pandas import read_csv
import tensorflow as tf
import deedee_ML_dataPull
import deedee_ML_createModel


# show TF version
print(tf.__version__)
print("Num GPUs Available: ", len(tf.config.experimental.list_physical_devices('GPU')))

# specify the window size
n_steps = 48


# split sequence into samples
def split_sequence(sequence_in, sequence_out, n_steps):
    # define X (input) and y (output) as lists
    X, y = list(), list()

    # for each row/data-point
    for i in range(0, len(sequence_in)):
        # find the end of this pattern
        end_ix = i + n_steps
        # check if we are beyond the sequence
        if end_ix > len(sequence_in) - 1:
            break
        # gather input and output parts of the pattern
        # seq_x is the first 'n' rows, and seq_y is the last row which will be the output
        seq_x, seq_y = sequence_in[i:end_ix], sequence_out[end_ix]
        X.append(seq_x)
        y.append(seq_y)
    return asarray(X), asarray(y)


# runs the training process
def run_training(symbol_to_train):
    # load the dataset
    train_symbol = symbol_to_train
    train_file_path = 'training_data/symbol_csvs/%s' % train_symbol
    print("Opening: " + train_symbol)

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

    df_out = read_csv(
        train_file_path,
        delimiter=',',
        header=0,
        index_col=None,
        usecols=[
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
        ]
    )

    # retrieve the values as floating point numbers
    values_in = df_in.values.astype('float32')
    values_out = df_out.values.astype('float32')

    # set training variables
    EPOCHS = 32
    BATCH_SIZE = 16
    n_test = 128

    if len(values_in) > (n_test + BATCH_SIZE):
        print("Training...")
        try:
            # split into samples
            X, y = split_sequence(values_in, values_out, n_steps)
            # print(X)
            # print(X.shape, y.shape)

            # reshape into [samples, timesteps, features]
            X = X.reshape((X.shape[0], X.shape[1], 19))
            y = y.reshape((y.shape[0], 10))

            # split into train/test
            X_train, X_test, y_train, y_test = X[:-n_test], X[-n_test:], y[:-n_test], y[-n_test:]
            # print(X_train.shape, X_test.shape, y_train.shape, y_test.shape)

            # define model
            model = tf.keras.models.load_model('save_model/model')

            # fit the model
            model.fit(X_train, y_train, epochs=EPOCHS, batch_size=BATCH_SIZE, verbose=2, validation_data=(X_test, y_test))

            # save the model
            model.save("save_model/model")
        except:
            print("There was an error. Exiting...")
            pass
    else:
        print("Skipping - not enough data")


def main():
    # pull the data from the database
    deedee_ML_dataPull.pull_data_and_create_csv()

    # define the model and save it
    deedee_ML_createModel.define_model(n_steps)

    # for each file in the directory, train on that file
    file_names = []
    for filename in os.listdir('training_data/symbol_csvs'):
        file_names.append(filename)
    #  random.shuffle(file_names)

    trained = 1
    training_limit = 0  # 0 for no limit
    training_times = []
    for file in file_names:
        start = timeit.default_timer()
        print("(" + str(trained) + " of " + str(len(file_names)) + ")")
        run_training(file)
        stop = timeit.default_timer()
        training_times.append(stop-start)
        print("-----\nTime To Train:  %.1f" % (stop-start))
        avg_time = statistics.mean(training_times)
        print("Average time:   %.1f" % avg_time)
        est_time = avg_time * (len(file_names) - trained)
        print("Estimated time: %.1f minutes" % (est_time / 60))
        if (training_limit != 0) and (trained >= training_limit):
            break
        trained += 1

    print("Model training completed.")
