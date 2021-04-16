
from pathlib import Path
import numpy as np
import pandas as pd
from functools import reduce
from loguru import logger
from datetime import datetime, timedelta
from siuba import *

# plots
import matplotlib.pyplot as plt 
import plotnine as p9
p9.theme_set(p9.theme_linedraw()) # default theme
from mizani.breaks import date_breaks
from mizani.formatters import date_format


from adtk.data import validate_series
from adtk.visualization import plot
from adtk.detector import LevelShiftAD, PersistAD, GeneralizedESDTestAD, SeasonalAD, AutoregressionAD
from tsmoothie.smoother import DecomposeSmoother

from src import utils
conn = utils.connect_athena(path='../configs/athena.yaml')


# -------------- #
### INDEX:
### 1. Initialize functions
### 2. Outlier detection functions
### 3. Imputation functions
### 4. Level shift correction functions
### 5. Plot functions 
### 6. Process functions
### 7. Run functions
### 8. Start process
# -------------- #


### 1. Initialize functions
def _validate_series(df, column_name):
    """
    A function to validate series
    
    Parameters
        ----------
        df : data frame 
            Data frame with column to validare. The data frame requires
            columns region_slug and date to be sorted. Date is defined as
            the index.
        column_name : str
            Name of the column to validate
    """

    logger.debug(f"... validating {column_name}...\n")
    y_df = df.sort_values(['date'])[['date', column_name]].set_index('date')
    s = validate_series(y_df)
    
    return s


### 2. Outlier detection functions
def _outlier_persist_ad(s, target_column_name, c_param = 3.0, window_param = 7):
    
    try :
        persist_ad = PersistAD(c=c_param, side='both', window = window_param)
        anomalies = persist_ad.fit_detect(s)
    except : 
        logger.debug('!! No Persist !!')
        anomalies = s
        anomalies[target_column_name] = 0
    finally : 
        anomalies = anomalies \
            .rename(columns={target_column_name:'anomaly_persist'}) \
            .reset_index()   
    
    return anomalies

def _outlier_gesdt_ad(s, target_column_name, alpha_param = 0.3, window_param = 7):
    
    try :
        esd_ad = GeneralizedESDTestAD(alpha=alpha_param)
        anomalies = persist_ad.fit_detect(s)
    except : 
        logger.debug('!! No gesdt !!')
        anomalies = s 
        anomalies[target_column_name] = 0
    finally : 
        anomalies = anomalies \
            .rename(columns={target_column_name:'anomaly_gesdt'}) \
            .reset_index()   
    
    return anomalies

def _outlier_seasonal_ad(s, target_column_name, c_param = 3.0):
    
    try : 
        seasonal_ad = SeasonalAD(c=c_param, side="both")
        anomalies = seasonal_ad.fit_detect(s)
    except : 
        logger.debug('!! No Seasonal !!')
        anomalies = s
        anomalies[target_column_name] = False
    finally : 
        anomalies = anomalies \
            .rename(columns={target_column_name:'anomaly_seasonal'}) \
            .reset_index()   
        
    return anomalies     

def _outlier_autregr_ad(s, target_column_name, c_param = 3.0, n_steps_param = 1, step_size_param=7):
    
    try : 
        autoregression_ad = AutoregressionAD(n_steps=n_steps_param, step_size=step_size_param, c=c_param)
        anomalies = autoregression_ad.fit_detect(s)
    except : 
        logger.debug('!! No Seasonal !!')
        anomalies = s    
        anomalies[target_column_name] = 0
    finally : 
        anomalies = anomalies \
            .rename(columns={target_column_name:'anomaly_autor'}) \
            .reset_index()   
        
    return anomalies     

def _anomalies_detector(s, target_column_name):
    """
    The function runs three algorithms to detect outliers.
    
    Parameters
        ----------
        s : data frame 
            Description
        target_column_name : str
            Target column name to detect outliers
    """
    
    anomalies = _outlier_persist_ad(s, target_column_name) \
        .merge(_outlier_seasonal_ad(s, target_column_name)) \
        .merge(_outlier_autregr_ad(s, target_column_name)) \
        .fillna(0)
    
    anomalies['anomaly_sum'] = \
        (anomalies['anomaly_persist']) + \
        (anomalies['anomaly_seasonal'] == True) +  \
        (anomalies['anomaly_autor'])
    
    anomalies.anomaly_sum[anomalies.date <= '2020-03-31'] = 0
    anomalies.anomaly_sum[((anomalies.date >= '2020-12-15') & (anomalies.date <= '2021-01-15'))] = 0
    
    anomalies.head(2)
      
    return(anomalies[anomalies.anomaly_sum > 0])


# 2. Find anomalies
def _find_anomalies(df, anomaly_vote_minimun, target_column_name, print_report=True):

    logger.debug("\n... finding outliers ...")
    
    # validate series
    s = _validate_series(df, target_column_name)
    #plot(s)
    
    # join anomialies detector
    df_anomaly = df.merge(_anomalies_detector(s, target_column_name), how = 'left')
    anomalies_cnt = sum(df_anomaly.anomaly_sum >= anomaly_vote_minimun)
    anomalies_date = df_anomaly[df_anomaly.anomaly_sum >= anomaly_vote_minimun].date.to_list()
    
    logger.debug('Number of anomalies found: ' + str(anomalies_cnt))  
    logger.debug(anomalies_date)
    
    if print_report:
        logger.debug('... printing anomalies report ...\n')
        print(_plot_anomalies(df_anomaly, 
                              observed_column = target_column_name,
                              anomalies_cnt=anomalies_cnt))
    
    return(df_anomaly, anomalies_date)



### 3. Imputation functions
def _decompose_lowess(variable_smooth, missing_values, smooth_fraction):
    # operate smoothing
    smoother = DecomposeSmoother(smooth_type='lowess', 
                                 periods=7,
                                 smooth_fraction=smooth_fraction)
    smoother.smooth(variable_smooth)

    result = variable_smooth
    smooth_result = smoother.smooth_data[0]
    result[missing_values] = smooth_result[missing_values]
    
    # removing negatives
    result[result <0] = 0
    
    return result

# 3. Impute anomalies
def _impute_anomalies(observed_column, 
                      date_column,
                      anomaly_sum_column, 
                      anomaly_vote_minimun, 
                      smooth_fraction = 0.4):
    """
    The function runs several algorithms to detect level shifts.
    
    Parameters
        ----------
        observed_column : data frame 
            Description
        date_column : str
            Description
        nomaly_sum_column : str
            Description
        anomaly_vote_minimun : str
            Description
        smooth_fraction : str
            Description
    """
    
    logger.debug("\n... imputing outliers ...")
    
    # building data frame to impute
    df_impute = pd.DataFrame({
        'date': date_column,
        'observed_column': observed_column,
        'anomaly_sum': anomaly_sum_column,
        'observed_missing': observed_column
    }) 
    # create missing values
    df_impute.loc[df_impute.anomaly_sum >= anomaly_vote_minimun, 'observed_missing'] = None
    df_impute.loc[df_impute.observed_column < 0, 'observed_missing'] = None

    df_impute = df_impute.set_index('date')
    
    # algorithms to impute
    df_impute =  df_impute \
        .assign(RollingMean=df_impute.observed_missing \
                    .fillna(df_impute.observed_missing \
                            .rolling(30, min_periods=1,) \
                            .mean()) ) \
        .assign(RollingMedian=df_impute.observed_missing \
                    .fillna(df_impute.observed_missing \
                            .rolling(30, min_periods=1,) \
                            .median()) ) \
        .assign(Polinomial=df_impute.observed_missing \
                    .interpolate(method='polynomial', order = 5)) \
        .assign(Loess=_decompose_lowess(df_impute.observed_column, 
                                        df_impute.observed_missing.isna(),
                                        smooth_fraction = smooth_fraction)) 


    return df_impute



### 4. Level shift correction functions
def _c_param(region_slug, 
             athena_path = '~/shared/spd-sdv-omitnik-waze/corona'):
    
    c_region = pd.read_csv(athena_path + '/cleaning/data/staging/cities_c_iqr.csv') 
    
    if sum(c_region.region_slug == region_slug) > 0:
        c_param = c_region[c_region.region_slug == region_slug].c_min.to_list()[0]
    else :
        c_param = 3
        
    logger.debug('c_param: ' + str(c_param))
    return c_param


def _level_shift_detection(s, 
                           c_param = 6.0, 
                           window_param = 14, 
                           print_plot = False):
    """
    Level shift or change point detection. This function uses the function 
    LevelShiftAD from ADTK library.
    
    Parameters
    ----------
        s : validated serie object 
            Description
        c_param : dbl, default 6.0
            Description
        window_param : int, default 14
            Description
        print_plot: bool, default True
            Description
    """
    #logger.debug(f"... detecting shift c{c_param}-w{window_param}...")
    level_shift_ad = LevelShiftAD(c=c_param, side='both', window=window_param)
    shifts = level_shift_ad.fit_detect(s)
    
    if print_plot:
        plot(s, anomaly=shifts, anomaly_color='red')
    
    return shifts


def _run_shift_grid(s, observed_variable, c_param, low_grid = .20, upp_grid = .60):
    """
    Run shift detector grid for several values. 
    
    Parameters
    ----------
        s : validated serie object 
            Description
        observed_variable : validated serie object 
            Description    
        c_param : dbl, default 6.0
            Description
        low_grid : int, default 14
            Description
        upp_grid: bool, default True
            Description
    """
    logger.debug(f"\n... shift level running grid  ...")
    
    shift_l = list()
    for cp in [round(c_param-c_param*(upp_grid), 4), 
               round(c_param-c_param*(low_grid), 4), 
               c_param, 
               round(c_param+c_param*(low_grid), 4), 
               round(c_param+c_param*(upp_grid), 4) ]:
        for wdw in [14, 15, 16, 17, 18]:    
            shift = _level_shift_detection(s, c_param = cp,
                                           window_param=wdw, 
                                           print_plot = False) \
                    .rename(columns={observed_variable:f'shift_c{cp}_w{wdw}'})
            shift_l.append(shift)
    len(shift_l)    
    df_grid = reduce(lambda df1, df2: df1.merge(df2, on='date'), shift_l)
    df_grid.shape
    
    logger.debug(f"Total combinations: {len(shift_l)}")
    
    return df_grid

def _shifted_adtk_ts(s, column_name, agg="std", window=(3,3), diff="l2", print_plot=True):
    # shift ts level
    
    s_transformed = DoubleRollingAggregate(
        agg=agg,
        window=window,
        diff=diff).transform(s).rename(columns={column_name:'adtk_shift'})
    
    if print_plot:
        plot(pd.concat([s, s_transformed], axis=1))
        
    return s_transformed


def _shift_sum(df_shift):
    
    df_shift_sum = (df_shift.reset_index()
     >> filter(_.date > '2020-03-31',
              ((_.date < '2020-12-15') ))
     >> gather('variable', 'value', -_.date)
     >> filter(_.variable.str.startswith('shift'))
     >> group_by('date')
     >> summarize(shift_sum = _.value.sum())
     >> filter(_.shift_sum > 0)
     >> arrange('date')
    )

    return df_shift_sum


def _rolling_manual_sum(tab, days_before= 0, days_after = 7):
    
    rolling_sum = list()
    
    for dat in tab.date:
        
        date_init = dat + timedelta(days=days_before)
        date_end  = dat + timedelta(days=days_after)
        
        #logger.debug( str(dat ) + ' to ' + str(dat + timedelta(days=7)))
        
        sum_sum = tab[(tab.date >= date_init) & (tab.date < date_end)].suma.sum()
        rolling_sum.append(sum_sum)

    return rolling_sum


def _shift_window_sum(df_shift, 
                      days_before= 0, 
                      days_after = 7):
    
    tab = (df_shift.reset_index()
     >> filter(_.date > '2020-03-31', 
              ((_.date < '2020-12-15') ))
     >> gather('variable', 'value', -_.date)
     >> filter(_.variable.str.startswith('shift'))
     >> arrange(_.date)
     >> group_by('date')
     >> summarize(suma = _.value.sum())
     >> ungroup()
    )
    tab['shift_sum'] = _rolling_manual_sum(tab, 
                                             days_before= days_before, 
                                             days_after = days_after)
    
    return tab[['date', 'shift_sum']]



def _initial_shift_date(df_shift_sum):

    shift_init = df_shift_sum[df_shift_sum.shift_sum == df_shift_sum.shift_sum.max()].date.min()
    
    logger.debug(f'Shift found at {shift_init}')
    
    return shift_init



def _shift_ts(shift_init, 
              date_column, 
              to_shift_column):
    
    # TODO: step_shift_before, step_shift_after
    # a two weeks both sided window
    shift_wdw_init = shift_init - timedelta(days=7)
    shift_wdw_end  = shift_init + timedelta(days=7)
    
    
    # level centered
    center_point = ((to_shift_column[(date_column >  shift_wdw_init) & 
                        (date_column <= shift_init)].mean()) -
                    (to_shift_column[(date_column >  shift_init) & 
                        (date_column <= shift_wdw_end)].mean())
                   )
    # change print to logger
    logger.debug('\n')  
    logger.debug('Center point: ' + str(center_point))
    
    shifted_column = to_shift_column
    shifted_column[ (date_column > shift_init) ] = \
        ( (to_shift_column[(date_column > shift_init)]) + center_point )
    
    # remove negatives 
    shifted_column[shifted_column < 0]=0    
        
    return shifted_column   


def _shift_level_report(df_shift, 
                        df_shift_sum, 
                        observed_column, 
                        region_slug):
    
    df_shift=df_shift.reset_index()    
    df_shift['observed_column'] = observed_column
    df_gather = (df_shift
         >> gather('variable', 'value', -_['date', 'observed_column']) 
       )   
    tab = (df_gather
         >> mutate(value_rec = _.value.replace( 0, np.nan))
        )
    
    tab[['shift', 'cparam', 'window']] = tab['variable'].str.split('_',expand=True)
    gg_1 = (p9.ggplot(tab, p9.aes(x ='date', y = 'observed_column'))
     + p9.geom_line(size = 1) 
     + p9.geom_point(p9.aes(size = 'value_rec', color = 'value_rec')) 
     + p9.facet_grid('window ~ cparam')
     + p9.scale_size_continuous(range=(1.5, 1.5)) 
     + p9.theme(axis_text_x=p9.element_text(angle=90),
                figure_size=(8, 6) )
        )
    print(gg_1)
    
    gg_2 = (p9.ggplot(df_shift_sum, p9.aes(x ='date', y = 'shift_sum'))
     + p9.geom_col()
     + p9.theme(axis_text_x=p9.element_text(angle=90),
                figure_size=(8, 2) )
        )
    print(gg_2)
    
    #logger.debug(df_shift_sum[df_shift_sum.shift_sum == df_shift_sum.shift_sum.max()])
    

def _shift_level(df, 
                 column_name, 
                 c_param  = 3.0, 
                 low_grid = .20, 
                 upp_grid = .60, 
                 grid_days_before= 0, 
                 grid_days_after = 7,
                 print_report = False):

    logger.debug('\n')  
    logger.debug(f"... shifting levels for {column_name} ...")
    
    s = _validate_series(df.reset_index(), column_name=column_name)
    #plot(s)
    
    # running grid
    df_grid = _run_shift_grid(s, 
                            observed_variable = column_name,
                            c_param  = c_param, 
                            low_grid = low_grid, 
                            upp_grid = upp_grid)
    # grid summary
    df_grid_sum = _shift_sum(df_grid)
    if False:
        df_grid_sum = _shift_window_sum(df_grid, 
                                        days_before= grid_days_before, 
                                        days_after = grid_days_after)
    
    # first date
    shift_init  = _initial_shift_date(df_grid_sum)
    
    # center shift ts
    shifted_column = _shift_ts(shift_init  = shift_init,
                               date_column = df.date,
                               to_shift_column = df[column_name])
    
    
    if print_report:
        _shift_level_report(df_grid, 
                            df_grid_sum,
                            observed_column=s.reset_index()[column_name])
    
    return shifted_column, shift_init



### 5. Plot functions
def _plot_levelshift(df_level, observed_column, shifted_column):
    print(observed_column)
    gg = (p9.ggplot(data=df_level,
               mapping=p9.aes(x='date', y=observed_column)) 
        + p9.geom_line(size = 1) 
        + p9.geom_line(p9.aes(y = shifted_column), size = 1, color = "red") 
        + p9.labs(title='Level Shift')
         )
    return gg
    
def _plot_anomalies(df_anomaly, observed_column, anomalies_cnt):
    gg = (p9.ggplot(data=df_anomaly,
               mapping=p9.aes(x='date', y=observed_column)) 
        + p9.geom_line(size = 1) 
        + p9.geom_point(p9.aes(size = 'anomaly_sum', color ='anomaly_sum') )
        + p9.labs(title='Anomalies identification',
                 subtitle = f'Anomalies found: {anomalies_cnt}') 
        + p9.theme(figure_size=(6, 3))
         )
    return gg
    
def _plot_imputation(df_imputate):
    gg = (p9.ggplot(data=df_imputate.reset_index(),
           mapping=p9.aes(x='date', y='observed_column')) 
        + p9.geom_line(color = 'gray') 
        + p9.geom_line(p9.aes(y = 'RollingMean'), color = "red", alpha = .4) 
        + p9.geom_line(p9.aes(y = 'Polinomial'), color = "green", alpha = .4) 
        + p9.geom_line(p9.aes(y = 'RollingMedian'), color = "blue", alpha = .4)
        + p9.geom_line(p9.aes(y = 'Loess'), color = "black") 
        + p9.labs(title='Imputation of anomalies')
        + p9.theme(figure_size=(6, 3))
         )    
    return gg

def _plot_end(df_run_1, df_run_2, df_end, region_slug):
        
        title_lab = f'{region_slug} \n ' +\
            f'Imputated dates: {df_run_1.outliers.sum() + df_run_2.outliers.sum()} \n' +\
            f'Level shifts dates: {df_run_1.level_shifts.sum() + df_run_2.level_shifts.sum()}'
        
        gg = (df_end 
            >> gather('variable', 'value', -_['date', 'region_slug', 'observed', 'expected_2020', 'tcp'])
            >> filter(_.variable.isin(['S2_shift', 'observed']))
            >> p9.ggplot(p9.aes(x = 'date', y = 'observed'))
             + p9.geom_line()
             + p9.geom_line(p9.aes(x = 'date', y = 'value', color = 'variable'), size = 1, alpha = .7)
             + p9.facet_wrap('variable', ncol = 1)
             + p9.labs(title = title_lab)
             + p9.theme(figure_size = (6, 3),
                       axis_text_x=p9.element_text(angle=90))
            )
        
        return gg
    
    
    

### 6. Process functions    
def _reading_data(region_slug):
      
    qry = f"""
        select 
            *,
            date_parse(concat(cast(year as varchar), ' ', cast(month as varchar), ' ', cast(day as varchar)), '%Y %m %e') date
        from spd_sdv_waze_corona.prod_daily_daily_index
        where region_slug in ('{region_slug}')
        """
    logger.debug(qry)
    df_cty = pd.read_sql_query(qry, conn)
    
    return df_cty

def _write_missing(df_run_1, df_run_2, region_slug, athena_path):
    
    logger.debug('... writing anomalies file ...')
    df_miss = df_run_1[['date', 'outliers', 'level_shifts']] \
        .append(df_run_2[['date', 'outliers', 'level_shifts']]) \
        .melt(id_vars=["date"]) \
        .siu_filter(_.value) 
    
    write_path = athena_path + '/cleaning/anomalies/'
    Path(write_path).mkdir(parents=True, exist_ok=True)

    df_miss.to_csv(write_path + f'/anomalies_{region_slug}.csv')
    

### 7. Run functions
def _run_step(df_run, 
              target_column_name, 
              output_column_name, 
              c_param, 
              anomaly_vote_minimun = 1,  
              print_report = True, 
              print_plot = False):

    logger.debug('... step start ...')  
    logger.info(f'Target variable: {target_column_name} \n')
    
    # 1. Detect and clean outliers
    df_anomaly, anomalies_date = _find_anomalies(df_run, 
                                 anomaly_vote_minimun = anomaly_vote_minimun, 
                                 target_column_name = target_column_name,
                                 print_report = print_report)

    df_final = df_anomaly[['date', target_column_name]]
    df_output  = _impute_anomalies(observed_column = df_anomaly[target_column_name], 
                                  date_column = df_anomaly.date, 
                                  anomaly_sum_column = df_anomaly.anomaly_sum, 
                                  anomaly_vote_minimun = anomaly_vote_minimun).reset_index()

    df_final = df_final.merge(df_output[['date', 'Loess']])
    
    # 2. Detect and move level shift
    logger.info(f'Output variable {output_column_name}')
    df_output[output_column_name], shift_date = _shift_level(df_output, 
                                                column_name='Loess', 
                                                c_param = c_param, 
                                                low_grid = .20, 
                                                upp_grid = .60,
                                                grid_days_before= 0, 
                                                grid_days_after = 7,
                                                print_report = print_report)
    
    df_final = df_final.merge(df_output[['date', output_column_name]]) \
        .assign(outliers = df_final.apply(lambda row: row['date'] in anomalies_date , axis = 1),
                level_shifts = df_final.apply(lambda row: row['date'] in [shift_date] , axis = 1))
    

    gg = (df_final
         >> gather('variable', 'value', -_['date', 'outliers', 'level_shifts'])
         >> p9.ggplot(p9.aes(x = 'date', y = 'value', color = 'variable'))
         + p9.geom_line()
         + p9.theme(figure_size = (6, 3)) 
         + p9.labs(title = f"Step {output_column_name}")
        )
    if print_plot:
        print(gg)
        
    df_output.columns
    df_final.columns
    
    logger.debug('... step done ...') 
    
    return df_final, gg




def _run_single(region_slug, 
                anomaly_vote_minimun_s1, 
                anomaly_vote_minimun_s2, 
                print_report = False, 
                print_plot = False, 
                athena_path = "/home/soniame/shared/spd-sdv-omitnik-waze/corona"):
    
    logger.info(f'... here we go {region_slug}...\n')  
    region_slug = region_slug #df_run.region_slug.unique()
    
    # 0. download data
    df_run = _reading_data(region_slug)
    
    
    # 00. parameter
    c_p = _c_param(region_slug)
    
    
    # 1. running first step
    df_run_1, _gg_1 = _run_step(df_run = df_run.sort_values(by=['region_slug', 'date']),
                         anomaly_vote_minimun = 1,
                         c_param= c_p,
                         target_column_name = 'observed',
                         output_column_name = 'S1_shift',
                         print_report = print_report, 
                         print_plot = print_plot)

    
    # 2. running second step
    df_run_2,  _gg_2 = _run_step(df_run = df_run_1[['date', 'S1_shift']].sort_values(by=['date']),
                         anomaly_vote_minimun = 1, 
                         c_param= c_p,
                         target_column_name = 'S1_shift',
                         output_column_name = 'S2_shift',
                         print_report = print_report, 
                         print_plot = print_plot)

    
    # 3. join daily results
    df_daily = df_run[['date', 'region_slug', 'observed', 'expected_2020', 'tcp']] \
        .merge(df_run_1[['date', 'Loess', 'S1_shift']] \
               .rename(columns = {'Loess':'S1_Loess'})) \
        .merge(df_run_2[['date', 'Loess', 'S2_shift']] \
               .rename(columns = {'Loess':'S2_Loess'})) 
    df_daily['tcp_clean'] = df_daily \
        .apply(lambda row: 100*(row['S2_shift'] - row['expected_2020'])/row['expected_2020'], axis = 1)
    df_daily.to_csv(athena_path + f'/cleaning/daily/daily_{region_slug}.csv', index= False)
    
    
    # 4. join weekly results
    df_weekly = df_daily \
        .assign(week = df_daily.date.dt.week,
                year = df_daily.date.dt.year) \
        .siu_group_by('week', 'year', 'region_slug') \
        .siu_summarize(date_min = _.date.min(),
                       date_max = _.date.max(), 
                       observed = _.observed.sum(),
                       cleaned  = _['S2_shift'].sum(), 
                       expected_2020 = _.expected_2020.sum()
                    ) \
        .siu_ungroup() 
    df_weekly['tcp'] = df_weekly \
        .apply(lambda row: 100*(row['observed'] - row['expected_2020'])/row['expected_2020'], axis = 1)
    df_weekly['tcp_clean'] = df_weekly \
        .apply(lambda row: 100*(row['cleaned'] - row['expected_2020'])/row['expected_2020'], axis = 1)
    df_weekly.to_csv(athena_path + f'/cleaning/weekly/weekly_{region_slug}.csv', index= False)
    

    # 5. write anomalies found
    _write_missing(df_run_1, df_run_2, region_slug, athena_path)
    
    
    # 6. plot results
    _gg = _plot_end(df_run_1, df_run_2, df_daily, region_slug)
    if print_plot:
        print(_gg)
    p9.save_as_pdf_pages([_gg_1, _gg_2, _gg], 
                         filename = f'{athena_path}/cleaning/figures/plot_{region_slug}.pdf')
    
   
    logger.info(f'... {region_slug} done ...\n')  
    
    return df_daily, df_weekly


def _run_batch(athena_path = "/home/soniame/shared/spd-sdv-omitnik-waze/corona"):

    qry = """
    select 
        distinct region_slug
    from spd_sdv_waze_corona.prod_daily_daily_index
    """
    regions_list = pd.read_sql_query(qry, conn).sort_values('region_slug').region_slug.unique()
    logger.info('Total regions process' + str(len(regions_list)))
    daily_l = list()
    weekly_l = list()
    
    for region in regions_list:
        print(region)
        df_daily, df_weekly = _run_single(region_slug=region, 
                            anomaly_vote_minimun_s1=1, 
                            anomaly_vote_minimun_s2=1, 
                            print_report = False, 
                            print_plot = False)
        daily_l.append(df_daily)
        weekly_l.append(df_weekly)

    daily= pd.concat(daily_l)
    daily.to_csv(athena_path + f'/cleaning/daily/daily_daily_index.csv', index= False)
    
    weekly= pd.concat(weekly_l)
    weekly.to_csv(athena_path + f'/cleaning/weekly/weekly_weekly_index.csv', index= False)
    


### 8. Start process    
# _run_batch(athena_path = "/home/soniame/shared/spd-sdv-omitnik-waze/corona")    