from tensorflow.keras import Sequential
from tensorflow.keras.layers import Dense, LSTM, Dropout, BatchNormalization


def define_model(steps):
    # define model
    model = Sequential()
    model.add(LSTM(256, kernel_initializer='he_normal', input_shape=(steps, 19), return_sequences=True))
    model.add(Dropout(0.2))
    model.add(BatchNormalization())
    model.add(LSTM(256, kernel_initializer='he_normal'))
    model.add(Dropout(0.2))
    model.add(BatchNormalization())
    model.add(Dense(128, activation='relu', kernel_initializer='he_normal'))
    model.add(Dropout(0.2))
    model.add(Dense(64, activation='relu', kernel_initializer='he_normal'))
    model.add(Dropout(0.2))
    model.add(Dense(10))

    model.summary()

    # compile the model
    model.compile(optimizer='adam', loss='mse', metrics=['mae', 'accuracy'])

    # save the model
    model.save("save_model/model")
