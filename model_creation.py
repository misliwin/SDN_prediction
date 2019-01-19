try:
    print('Trying to import pyspark...')
    import pyspark
except ImportError:
    print('Pyspark import failed...')
    print('Installing pyspark in conda environment...')
    import sys

from pyspark import SparkConf, SparkContext
try:
    sc
except NameError:
    conf = SparkConf().setAppName('SDN')
    sc = SparkContext(conf=conf)
else:
    if sc != None:
        sc.stop()
    conf = SparkConf().setAppName('SDN')
    sc = SparkContext(conf=conf)

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

from os import system
import pandas as pd
import numpy as np
import statistics
from datetime import datetime, timedelta
import time
import scipy.stats as sts

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import matplotlib.dates as mdates
import json
import urllib
import logging, sys

try:
    print('Trying to import paramiko...')
    import paramiko
    print('Paramiko imported.')
except ImportError:
    print('Paramiko import failed...')
    print('Installing paramiko in conda environment...')
    import sys

try:
    print('Trying to import scp...')
    import scp
    print('Scp imported.')
except ImportError:
    print('Scp import failed...')
    print('Installing scp in conda environment...')
    import sys

try:
    print('Trying to import keras...')
    import keras
    print('Keras imported.')
except ImportError:
    print('Keras import failed...')
    print('Installing keras in conda environment...')
    import sys

# keras
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import Dense
def loginanddownload(hostname,uname,pwd,sfile,tfile):
    """
    Can copy files and directories from PNDa to remote system.
    Usage example:
        loginanddownload(red_pnda_ip, username, password, remote_folder, local_destination)
        loginanddownload('192.168.57.4', 'pnda', 'pnda', '/data', '/home/amadeusz/')
    
    I am using it only for download full copy of /data folder from pnda VM. There is a dependency of openssh-server
    installation on red_pnda VM.
    
    """
    try:
        print("Establishing ssh connection")
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=hostname, username=uname, password=pwd)
    except paramiko.AuthenticationException:
        print("Authentication failed, please verify your credentials: %s")
    except paramiko.SSHException as sshException:
        print("Unable to establish SSH connection: %s" % sshException)
    except paramiko.BadHostKeyException as badHostKeyException:
        print("Unable to verify server's host key: %s" % badHostKeyException)
    except Exception as e:
        print(e.args)
    try:
        print("Getting SCP Client")
        scpclient = scp.SCPClient(ssh_client.get_transport())
        print("Hostname: %s", hostname)
        print("source file: %s", sfile)
        print("target file: %s", tfile)
        scpclient.get(sfile,tfile, recursive = True)
    except scp.SCPException as e:
        print("Operation error: %s", e) 

class Prediction:
    """
    Prediction class - class for preprocessing data from red_pnda.
    Not full variables are in use (this is a changed copy of Lecturer shared file)
    
    Prediction class takes exacly one argument - bytes (network traffic data)
    It is further processed and returned in other format.
    
    """
    def __init__(self, bytes):
        self.bytes = bytes
        self.omega = 2.0 * np.pi / len(bytes)
        self.export = pd.DataFrame()
        self.x = None
        self.data = None
        self.y = None
        self.train = None
        self.index = None
        self.train_mean = None
        self.train_std = None

    def prepare_data_for_prediction(self):
        """
        From RAW data compute time dependency (x) and bandwidth (y).
        """
        self.train = self.bytes
        self.train_mean = np.mean(self.train)
        self.train_std = np.std(self.train)
        self.train = (self.train - self.train_mean) / self.train_std
        self.index = np.asarray(range(len(self.train)), dtype=np.float64)
        self.x = np.asarray(range(len(self.train)), dtype=np.float64)
        self.y = self.train
        self.data = pd.DataFrame(np.column_stack([self.x, self.y]), columns=['x', 'y'])

    def proceed_prediction(self, percent):
        """
        Start data preprocessing.
        """
        self.prepare_data_for_prediction()
class WatchDog:
    """
    WatchDog class - extracts bandwidth information from collected flow and port data.
    (Not all variables are in use)
    
    self.dpid - particular switch identification number
    self.port_no - particular port number on the switch
    """
    def __init__(self, data_path):    
        self.interval = 300
        self.unusual = 0
        self.normal_work = True
        self.train_data = None
        self.pred = None
        self.pi = None
        self.current_stats = None
        self.sched = BackgroundScheduler()
        self.sched.start()
        self.dpid = 2
        self.port_no = 2
        self.resampled = None
        self.data_path = data_path
        
    def get_last_hour_stats(self):
        """
        Currently not in use.
        """
        last_hour_time = datetime.now() - timedelta(hours = 1)
        year = int(last_hour_time.strftime("%Y"))
        month = int(last_hour_time.strftime("%m"))
        day = int(last_hour_time.strftime("%d"))
        hour = int(last_hour_time.strftime("%H"))
        self.train_data = sqlContext.read.json(self.data_path+"data/year="+str(year)+"/month="+str(month)+
                                             "/day="+str(day)+"/hour="+str(hour)+"/dump.json")
        
    def get_previous_day_stats(self):
        """
        Gather statistics from particular hour/day.
        You can change dates based on your gathered data.
        """
        year="2018"
        month="12"
        day="21"
        hour=0
        data = sqlContext.read.json(self.data_path+"data/year="+year+"/month="+month+"/day="+day+"/hour="+str(hour)+"/dump.json")

        d = []
        for h in range(hour+1,15):
            d.append(sqlContext.read.json(self.data_path+"data/year="+year+"/month="+month+"/day="+day+"/hour="+str(h)+"/dump.json"))
        for i in range(0,len(d)):
            data = data.unionAll(d[i])            
        self.train_data = data

    
    def get_current_stats(self):
        """
        Currently not in use.
        """
        year = int(time.strftime("%Y"))
        month = int(time.strftime("%m"))
        day = int(time.strftime("%d"))
        hour = int(time.strftime("%H"))
        self.current_stats = sqlContext.read.json(self.data_path+"data/year="+str(year)+"/month="+str(month)+
                                             "/day="+str(day)+"/hour="+str(hour)+"/dump.json")
    
    def get_port_stats(self,data,dpid,port_no):
        """
        Gathers data from specific port on the switch.
        """
        port = data.filter((data['origin']=='port_stats') & 
                           (data['switch_id']==dpid) & 
                           (data['port_no']==port_no)).orderBy('timestamp')
        port = port.toPandas()
        ts = pd.Series(port['timestamp'].astype(int))
        ts = pd.to_datetime(ts, unit='s')
        index = pd.DatetimeIndex(ts)
        raw_data = pd.Series(port['tx_bytes'].values, index=index)
        return raw_data, port
    
    def get_last_tput(self,dpid,port_no):
        """
        Currently not in use.
        """
        last_two_rows = self.current_stats.filter((self.current_stats['origin']=='port_stats') & 
                           (self.current_stats['switch_id']==dpid) & 
                           (self.current_stats['port_no']==port_no)).orderBy('timestamp', ascending=False).limit(2)
        last_two_rows = last_two_rows.toPandas()
        bytes = last_two_rows['tx_bytes'].astype(int)
        time = last_two_rows['timestamp'].astype(int)
        tput = (bytes[0]-bytes[1])/(time[0]-time[1])
        return tput
        
    def resample_port_stats (self,raw_data, port):
        """
        Resamples data into correct date format and frequency.
        """
        raw_data = raw_data[~raw_data.index.duplicated(keep='first')]
        resampled_data = raw_data.resample('s').interpolate()
        resampled_data = [(y - x) for x,y in zip(resampled_data.values,resampled_data.values[1:])]
        ts_resampled = pd.Series(range(len(resampled_data)))
        ts_resampled= pd.to_datetime(ts_resampled, unit='s')
        return resampled_data, ts_resampled
               
    def proceed_prediction(self,enable_plot=False):
        """
        Start preprocessing traffic data.
        
        If you want to download data to your localhost just uncomment first line.
        This is not necessary when you have all the data stored locally already.
        """
        self.get_previous_day_stats()
        data, port_stats = self.get_port_stats(self.train_data,self.dpid,self.port_no)
        resampled_data, ts_resampled = self.resample_port_stats(data, port_stats)
        self.resampled = resampled_data
        prediction = Prediction(resampled_data)
        prediction.proceed_prediction(95)
        self.ts_resampled = ts_resampled
        if enable_plot:
            fig = plt.figure(figsize=(15,8))
            ax = fig.add_subplot(111)
            ax.plot(ts_resampled,[x*8/1e6 for x in resampled_data])
            ax.set_ylabel("Tput [Mbps]")
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))


# please specify path to data folder
wd = WatchDog("/home/misliwin/Desktop/sdn/SDN_data_preprocessing_simple_NN_with_data/")
wd.proceed_prediction()
wd.get_previous_day_stats()
series = [x*8/1e6 for x in wd.resampled]

# frame a sequence as a supervised learning problem
def timeseries_to_supervised(data, lag=1):
    df = pd.DataFrame(data)
    columns = [df.shift(i) for i in range(1, lag+1)]
    columns.append(df)
    df = pd.concat(columns, axis=1)
    df.fillna(0, inplace=True)
    return df
 
# create a differenced series
def difference(dataset, interval=1):
    diff = list()
    for i in range(interval, len(dataset)):
        value = dataset[i] - dataset[i - interval]
        diff.append(value)
    return pd.Series(diff)
 
# invert differenced value
def inverse_difference(history, yhat, interval=1):
    return yhat + history[-interval]
 
# scale train and test data to [-1, 1]
def scale(train, test):
    # fit scaler
    scaler = MinMaxScaler(feature_range=(-1, 1))
    scaler = scaler.fit(train)
    # transform train
    train = train.reshape(train.shape[0], train.shape[1])
    train_scaled = scaler.transform(train)
    # transform test
    test = test.reshape(test.shape[0], test.shape[1])
    test_scaled = scaler.transform(test)
    return scaler, train_scaled, test_scaled
 
# inverse scaling for a forecasted value
def invert_scale(scaler, X, value):
    new_row = [x for x in X] + [value]
    array = np.array(new_row)
    array = array.reshape(1, len(array))
    inverted = scaler.inverse_transform(array)
    return inverted[0, -1]
 
print("load dataset")
# load dataset
series = [x*8/1e6 for x in wd.resampled]

# transform data to be stationary
diff_values = difference(series, 1)
 
# transform data to be supervised learning
supervised = timeseries_to_supervised(diff_values, 1)
supervised_values = supervised.values
 
# split data into train and test-sets
train, test = supervised_values[0:-2000], supervised_values[-2000:]

# transform the scale of the data
scaler, train_scaled, test_scaled = scale(train, test)

range_size = 450

X_train = train_scaled[:,0]
Y_train = train_scaled[:,-1]
X_test = test_scaled[:,0]
Y_test = test_scaled[:,-1]

print("prepare data for model")

for i in range(range_size):
    if i == 0:
        X_train_complete  =  np.array(X_train[range_size:-1-range_size])
        X_test_complete  =  np.array(X_test[range_size:-1-range_size])
        
        Y_train_complete =  np.array(Y_train[2*range_size+1:])
        Y_test_complete  =  np.array(Y_test[2*range_size+1:])
        
    elif i ==1:
        X_train_complete = np.array([X_train_complete[:], X_train[range_size-i:-i-1-range_size]])
        X_test_complete = np.array([X_test_complete[:], X_test[range_size-i:-i-1-range_size]])
        
        Y_train_complete = np.array([Y_train_complete[:], Y_train[2*range_size-i:-i-1]])
        Y_test_complete = np.array([Y_test_complete[:], Y_test[2*range_size-i:-i-1]])
    else:
        X_train_complete = np.append(X_train_complete, [X_train[range_size-i:-i-1-range_size]], axis=0)
        X_test_complete = np.append(X_test_complete, [X_test[range_size-i:-i-1-range_size]], axis=0)
        
        Y_train_complete = np.append(Y_train_complete, [Y_train[2*range_size-i:-i-1]], axis=0)
        Y_test_complete = np.append(Y_test_complete, [Y_test[2*range_size-i:-i-1]], axis=0)
        
X_train_complete = np.swapaxes(X_train_complete, 1, 0)
X_test_complete = np.swapaxes(X_test_complete, 1, 0)
Y_train_complete = np.swapaxes(Y_train_complete, 1, 0)
Y_test_complete = np.swapaxes(Y_test_complete, 1, 0)

print("data ready - creating the model")

from keras.layers import Dropout
from keras.constraints import maxnorm
def wider_model():
    # create model
    model = Sequential()
    model.add(Dense(16, input_dim=range_size, kernel_initializer='normal', activation='relu', kernel_constraint=maxnorm(3)))
    model.add(Dropout(0.4))
    model.add(Dense(range_size, kernel_constraint=maxnorm(3)))
    model.add(Dropout(0.4))
    model.add(Dense(range_size, kernel_constraint=maxnorm(3)))
    model.add(Dropout(0.4))
    model.add(Dense(range_size, kernel_initializer='normal'))
    # Compile model
    model.compile(loss='mean_squared_error', optimizer='adam')
    return model
model = wider_model()
print("train model")
history = model.fit(X_train_complete, Y_train_complete, epochs=3, batch_size=1000, verbose=1)
print("save model")
model.save('SDN_model_30min.h5')


