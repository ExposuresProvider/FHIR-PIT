import matplotlib.pyplot as plt
import numpy as np
import h5py
import datetime
import glob
from time import time
from keras.callbacks import TensorBoard
from keras.models import Model
from keras.layers import LSTM, Input, Dense, RepeatVector
import vec_to_array

epoch = datetime.datetime(1970,1,1)
sequence_length = 32
batch_size = 256  # Batch size for training.
epochs = 100  # Number of epochs to train for.

MODEL_PATH = "/mnt/d/autoencoder.model"

header_map = vec_to_array.header_map("/mnt/d/json/header")

samples = []
for filename in glob.glob("/mnt/d/json/vector*"):
    print("processing", filename)
    timestamps, time_series, sex_cd0, race_cd0, birth_date0 = vec_to_array.vec_to_array(header_map, filename)
    sex_cd = ["M", "F"].index(sex_cd0)
    race_cd = int(race_cd0)
    birth_date = (datetime.datetime.strptime(birth_date0, "%Y-%m-%d") - epoch).days
    abs_timestamps = list(map(lambda x : x + birth_date, timestamps))
    rel_timestamps = list(map(lambda x : x - timestamps[0], timestamps))

    num_rows, num_cols = time_series.shape
    for i in range(0, num_rows - sequence_length + 1):
        samples.append(time_series[i:i+sequence_length])
    if len(samples) > 10000:
        break

X = np.array(samples)

print("number of rows", len(samples))
print("number of columns", num_cols)

# Define an input sequence and process it.
inputs = Input(shape=(sequence_length, num_cols))
encoded = LSTM(4)(inputs)
decoded = RepeatVector(sequence_length)(encoded)
outputs = LSTM(num_cols, return_sequences=True)(decoded)
sequence_autoencoder = Model(inputs, outputs)
model = Model(inputs, outputs)

infer_model = Model(inputs, encoded)

# Run training
tensorboard = TensorBoard(log_dir="/mnt/d/autoencoder.logs/{}".format(time()))

model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=["mse"])
model.fit(X, X,
          batch_size=batch_size,
          epochs=epochs,
          validation_split=0.2,
          callbacks=[tensorboard])

Y = infer_model.predict(X)
for i in range(0,max(len(X),100)):
    print(X[i], "->", Y[i])

# Save model
infer_model.save(MODEL_PATH)