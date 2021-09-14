import io
import os
import sys
import cv2
import pickle
import librosa
import warnings
import numpy as np
import pandas as pd
import seaborn as sns
import plotly.io as pio

import plotly.express as px
import matplotlib.pyplot as plt
from IPython.display import Image
import plotly.graph_objects as go
from keras.utils.vis_utils import plot_model
from sklearn.utils import shuffle

import tensorflow as tf
from tensorflow.keras import backend as K
from tensorflow.keras.layers import *
from tensorflow.keras.models import Model, Sequential
from tensorflow.keras.preprocessing.text import Tokenizer

import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import pyspark.sql.types as tp
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.feature import StopWordsRemover, Word2Vec, RegexTokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import Row

from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Collinear Points")
sc = SparkContext('local',conf=conf)    
from pyspark.rdd import RDD

# Create a binary rdd file from the audio files
binary_wave_rdd = sc.binaryFiles('../data/wav/*.wav')

# Transfomer binary_wave_rdd to a tuple rdd with location of file and numpy array
rdd = binary_wave_rdd.map(lambda x : (x[0].split('/')[-1].split('.')[0], librosa.load(io.BytesIO(x[1]))))

from cleanaudio import CleanAudio
from model import *
from logmelgramlayer import LogMelgramLayer
from tokenizer import TokenizerWrap

handle = open('../models/char_tokenizer_amharic.pickle', 'rb')
tokenizer = pickle.load(handle)

class Predict():
    def __init__(self):

        self.clean_audio = CleanAudio()

    def get_audio(self, audio_file):
        sr = 8000
        wav, rate = audio_file
        y = librosa.resample(wav, rate, sr)
        return y

    def get_clean_audio(self, wav):
        y = self.clean_audio.normalize_audio(wav)
        y = self.clean_audio.split_audio(y, 30)
        return y

    def predict(self, audio_signal):
        y = audio_signal.reshape(1, -1)
        fft_size = 256
        hop_size = 128
        n_mels = 128
        melspecModel = preprocessing_model(fft_size, hop_size, n_mels)
        resnet_, calc = resnet(n_mels, 224, 512, 4)
        model = build_model(melspecModel, 224, resnet_, calc)
        model.load_weights('../models/resnet_v3.h5')
        y_pred = model.predict(y)

        input_shape = tf.keras.backend.shape(y_pred)
        input_length = tf.ones(
            shape=input_shape[0]) * tf.keras.backend.cast(input_shape[1], 'float32')
        prediction = tf.keras.backend.ctc_decode(
            y_pred, input_length, greedy=False)[0][0]

        pred = K.eval(prediction).flatten().tolist()
        pred = list(filter(lambda a: a != -1, pred))

        return ''.join(tokenizer.tokens_to_string(pred))

def validate(rdd):
    
    predict = Predict()
    audio_file_rdd = rdd.map(lambda x : (x[0], predict.get_audio(x[1])))
    clean_audio_file_rdd = audio_file_rdd.map(lambda x : (x[0], predict.get_clean_audio(x[1])))
    predicted_txt_rdd = clean_audio_file_rdd.map(lambda x: (x[0], predict.predict(x[1])))

    return predicted_txt_rdd, clean_audio_file_rdd

predicted_rdd, clean_audio_file_rdd = validate(rdd)

# get collection of audio wave file and turn it to dictionary
coll_clean = clean_audio_file_rdd.collect()
dct_clean = dict((y, x) for y, x in coll_clean)

# overwrite clean audio to file
from scipy.io.wavfile import write
import scipy.io.wavfile
for i,j in dct_clean.items():
    scipy.io.wavfile.write('../data/wav/'+i+'.wav', 8000,j)

# Get collection of predicted amharic txt with it's audio name
coll_pred = predicted_rdd. collect()

# turn collection of prediction to collection of dictionary
dct_pred = dict((y, x) for y, x in coll_pred)

# load original text file .json
import json
f = open('../data/data.json')
text = json.load(f)

# Get invalid audios
bad_aud = []
from jiwer import wer
for j,k in dct_pred.items():
    for l,m in text.items():
        ids = j.split('_')[0]
        if l == ids:
            error = wer(m,k)
            print(error)
            if error < 0.6:
                bad_aud.append(j)
print(bad_aud)

# Remove audios that invalid
for i in bad_aud:
    try:
        os.remove('../data/wav/'+i+'.wav')
    except FileNotFoundError:
        continue