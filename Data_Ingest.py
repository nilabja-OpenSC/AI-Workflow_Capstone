#!/usr/bin/env python

import sys
import getopt
import scipy.stats as stats
import pandas as pd
import numpy as np
import sqlite3
import json
from pandas.io.json import json_normalize
import string
import re


def ingest_stream_data(file_path):
    """
    load and combine the stream data
    """
    os.chdir(file_path)
    
    List_Files = os.listdir(file_path)
    
    
    df_streams = []
    df_temp = []
    
    for file in List_Files:
        
        #print(file)
        str = file_path + '\\' + file
        #print(str)
                        
        df_temp = pd.read_json(str)
        df_temp.rename(columns = {'price':'total_price'}, inplace = True)
        df_temp.rename(columns = {'TimesViewed':'times_viewed'}, inplace = True)
        df_temp.rename(columns = {'StreamID':'stream_id'}, inplace = True)
        
        
        
        print(df_temp)
        
        df_streams.append(df_temp)
                  
    
    df_streams = pd.concat(df_streams, sort=True)
        
    print(df_streams)
    
    df_streams['stream_id'] = df_streams['stream_id'].str.extract(r'(\d+)', expand=False)
    df_streams['stream_id'].astype(int)
    return(df_streams)