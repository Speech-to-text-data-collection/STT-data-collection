import os
import sys
import pickle

import warnings

import tensorflow as tf
from tensorflow.keras import backend as K
from tensorflow.keras.layers import *
from tensorflow.keras.models import Model, Sequential
from cleanaudio import CleanAudio
from logmelgramlayer import LogMelgramLayer

sr = 8000
fft_size = 256
hop_size = 128
n_mels = 128

def preprocessing_model(fft_size, hop_size, n_mels):
    
    input_data = Input(name='input', shape=(None,), dtype="float32")
    spec = LogMelgramLayer(
        num_fft=fft_size,
        hop_length=hop_size,
        num_mels=n_mels,
        sample_rate=sr,
        f_min=0.0,
        f_max=sr // 2,
        eps=1e-6)(input_data)
    x = BatchNormalization(axis=2)(spec)
    # x = Permute((2, 1, 3), name='permute', dtype="float32")(x)
    model = Model(inputs=input_data, outputs=x, name="preprocessin_model")
    
    return model

def build_model(melspecModel, output_dim, custom_model, calc=None):

    input_audios = Input(name='the_input', shape=(None,))
    pre = melspecModel(input_audios)
    pre.trainable = False  # Freeze the layer
    pre = tf.squeeze(pre, [3])

    y_pred = custom_model(pre)
    model = Model(inputs=input_audios, outputs=y_pred, name="model_builder")
    model.output_length = calc

    return model


def cnn_output_length(input_length, kernel_list, pool_sizes, cnn_stride, mx_stride, padding='same'):

    if padding == 'same':
        output_length = input_length
        for i, j in zip(cnn_stride, pool_sizes):
            output_length = (output_length)/i
            if j != 0:
                output_length = (output_length - j)/mx_stride + 1

        return tf.math.ceil(output_length)

    elif padding == 'valid':

        output_length = input_length
        for i, j in zip(kernel_list, pool_sizes):
            output_length = (output_length - i)/cnn_stride + 1
            if j != 0:
                output_length = (output_length - j)/mx_stride + 1

        return tf.math.floor(output_length)


def block(filters, inp):
    x = BatchNormalization()(inp)
    x = LeakyReLU(.1)(x)
    x = Dropout(.4)(x)
    x = Conv2D(filters, (3, 3), padding='same')(x)
    x = BatchNormalization()(x)
    x = LeakyReLU(.1)(x)
    x = Dropout(.4)(x)
    x = Conv2D(filters, (3, 3), padding='same')(x)
    return(x)


def resnet(input_dim, output_dim=224, units=256,  num_birnn=2):

    filters = [32, 32, 32]
    kernels = [3, 3, 3]
    pool_sizes = [0, 0, 2]
    cnn_stride = [1, 1, 1]
    mx_stride = 2

    input_data = Input(name='the_input', shape=(None, input_dim))
    x = Reshape((-1, input_dim, 1), dtype="float32")(input_data)

    x = Conv2D(filters[0], (3, 3), padding='same')(x)
    x = MaxPooling2D((1, 2), strides=(1, 2), padding='same')(x)

    x = Add()([block(filters[0], x), x])
    x = Add()([block(filters[0], x), x])
    x = Add()([block(filters[0], x), x])

    x = Conv2D(filters[1], (3, 3), padding='same')(x)
    x = MaxPooling2D((1, 2), strides=(1, 2), padding='same')(x)

    x = Add()([block(filters[1], x), x])
    x = Add()([block(filters[1], x), x])
    x = Add()([block(filters[1], x), x])

    x = Conv2D(filters[2], (3, 3), padding='same')(x)
    x = MaxPooling2D((1, 2), strides=(1, 2), padding='same')(x)

    x = Add()([block(filters[2], x), x])
    x = Add()([block(filters[2], x), x])
    x = Add()([block(filters[2], x), x])

    # x = MaxPooling2D((2,2), strides=2, padding = 'same')(x)
    x = AveragePooling2D((2, 2), strides=2, padding='same')(x)
    x = Reshape((-1, x.shape[-1] * x.shape[-2]))(x)

    # GRULayer
    for i in range(num_birnn):
        x = Bidirectional(GRU(units=units, return_sequences=True,
                          implementation=2, name='rnn_{}'.format(i)))(x)
        x = Dropout(.4)(x)
        x = LeakyReLU(.1)(x)
        x = BatchNormalization(name='bn_rnn_{}'.format(i))(x)

    x = TimeDistributed(Dense(output_dim))(x)
    y_pred = Activation('softmax', name='softmax')(x)

    model = Model(inputs=input_data, outputs=y_pred, name="custom_model")

    def output_length_calculater(x): return cnn_output_length(
        x, kernels, pool_sizes, cnn_stride, mx_stride)

    return model, output_length_calculater
