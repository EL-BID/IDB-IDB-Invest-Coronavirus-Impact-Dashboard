
# LIBRARIES
import pandas as pd
import numpy as np
from datetime import datetime

import random
import itertools

from loguru import logger
from src import utils
from src.runners.partitioned_athena_query import _get_hours

def sample_dates(start_date, end_date):
    """
    Sample 50 days from date range
    - Exclude weekends and end of year. 
    - Balanced sample per day of week
    Sampling seeds are set to 1234
    Parameters
    ----------
    start_date: chr
        Start date from sequence
    end_date: chr
        End date from sequence
    """
    df = pd.DataFrame(pd.date_range(start=start_date, end=end_date), 
                           columns=['date'])

    df['day']   = df['date'].apply(lambda x: int(x.strftime('%d')))
    df['dow']   = df['date'].apply(lambda x: x.strftime('%A'))
    df['month'] = df['date'].apply(lambda x: x.strftime('%m'))
    df['year']  = df['date'].apply(lambda x: x.strftime('%Y'))
    df['week']  = (df['dow'] == 'Monday').cumsum()
    df['fwnd']  =  df['dow'].isin(['Saturday', 'Sunday']) == False
    df['fdec']  = ((df['month'] == '01') & (df['day'] <= 15) | 
                   (df['month'] == '12') & (df['day'] >= 15)) == False
    
    # Filter weekends and end of year
    df = df[df.fwnd]
    df = df[df.fdec]
    
    # Filter complete weeks
    df = df. \
        merge(df[['week','dow']].groupby('week'). \
              agg('count').reset_index(). \
              rename(columns = {'dow':'nd'}))
    df = df[df.nd == 5]
    
    # Sample 50 weeks
    np.random.seed(1234)
    df = df.merge(df[['week']].drop_duplicates().sample(n=50), how='inner')

    # For each week sample a weekday
    random.seed(1234)
    regular_list = [random.sample(df.dow.unique().tolist(), k = 5) for x in range(10)]
    dow_list = list(itertools.chain(*regular_list))
    week_list  = df.week.unique()
    
    _dates = pd.DataFrame(columns=df.columns.tolist())
    for x in range(50):
        _dates = _dates.append( df[((df.week == week_list[x]) & (df.dow == dow_list[x]))])

    logger.debug(_dates[['date', 'dow']].groupby('dow').agg('count'))
    logger.debug(_dates[['year', 'dow']].groupby('year').agg('count'))

    
    return(_get_hours(_dates))


def check_existence(config):

    return True

def start(config):

    
    # Date run ----    
    globals()[config["name"]](config)
    
    