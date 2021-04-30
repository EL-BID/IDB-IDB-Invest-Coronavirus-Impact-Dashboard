# Running the Cleaning Process



## Process 

The cleaning process consists in two steps: 

First step: 

1. First identification of anomalies
2. Imputation of anomalies
3. First identification of level shifts in 2020

Second step: 

4. Second identification of anomalies
5. Imputation of anomalies
6. Second identification of level shifts in 2020
5. Imputation of negative values


## Detectors 

To identify anomalies we use the Anomaly Detection Toolkit (ADTK). ADTK is a Python package for unsupervised / rule-based time series anomaly detection.


### Anomalies detectors

The methodology implemented considers three types of anomaly detectors.

#### **Persist Anomaly Detector**

This detector compares each time series value with its previous values. It compares time series values with the values of their preceding time windows, and identifies a time point as anomalous if the change of value from its preceding median is anomalously large.

Function in pipeline: `_outlier_persist_ad(s, target_column_name, c_param, window_param)`

Parameters:

  - `window` (int or str, 7) – Size of the preceding time window.
  - `c` (float, 1.5 to 3) – Factor used to determine the bound of normal range based on historical interquartile range.
  - `side` (str, both) - If *both*, detect anomalous positive and negative changes;
  - `min_periods` (int, None) - Minimum number of observations in each window required to have a value for that window.
  - `agg` (str, median) - Aggregation operation of the time window, either “mean” or “median”



#### **Seasonal Anomaly Detector**

This detector identifies anomalous values away from seasonal pattern. It uses a seasonal decomposition transformer to remove seasonal pattern (as well as trend), and identifies a time point as anomalous when the residual of seasonal decomposition is anomalously large.

Function in pipeline: `_outlier_seasonal_ad(s, target_column_name, c_param)`

Parameters: 

  - `freq` (int, None) - Length of a seasonal cycle as the number of time points in a cycle.
  - `c` (float, 1.5 to 3.0) - Factor used to determine the bound of normal range based on historical interquartile range
  - `side` (str, both) - If both, to detect anomalous positive and negative residuals;
  - `trend` (bool, False) -  Whether to extract trend during decomposition.


#### **Autoregression Anomaly Detector**

This detector indentifies anomalous autoregression property in time series. The algorthm applies a regressor to learn autoregressive property of the time series, and identifies a time point as anomalous when the residual of autoregression is anomalously large.

Function in pipeline: `_outlier_autregr_ad(s, target_column_name, c_param, n_steps_param, step_size_param)`

Parameters: 
    
  - `n_steps` (int, 1) - Number of steps (previous values) to include in the model.
  - `step_size` (int, 7) - Length of a step. For example, if n_steps=2, step_size=3, X_[t-3] and X_[t-6] will be used to predict X_[t].
  - `regressor` (object, linear) - Regressor to be used. Same as a scikit-learn regressor, it should minimally have fit and predict methods. 
  - `c` (float, 1.5 to 3.0) - Factor used to determine the bound of normal range based on historical interquartile range. 
  - `side` (str, both) - “both”, to detect anomalous positive and negative residuals;


**Note:** The identificaction of outliers do not apply for data observed before 2020-03-31 and between 2020-12-15 and 2021-01-15.



### Level shifts detectors

This detector identifies level shift of time series values. It compares values of two time windows next to each others, and identifies the time point in between as an level-shift point if the difference of the medians in the two time windows is anomalously large.


Function in pipeline: `_outlier_autregr_ad(s, target_column_name, c_param = 3.0, n_steps_param = 1, step_size_param=7)`

Parameters: 
   
  - `window` (int, 14) - Size of the time windows.
  - `c` (float, 0.2 to 5.2) - Factor used to determine the bound of normal range based on historical interquartile range. 
  - `side` (str, both) - If “both”, to detect anomalous positive and negative changes;
  - `min_periods` (int, None) - Minimum number of observations in each window required to have a value for that window. If 2-tuple, it defines the left and right window respectively.


For more information about [ADTK Detectors](https://arundo-adtk.readthedocs-hosted.com/en/stable/api/detectors.html) .



## Imputation

After detecting an anomaly a threshold is set to recognize each observation as an outlier.In the pipeline is for at least **one** detector to identify the observation as an anomaly. Although four methods were tested (Rolling Mean, Rolling Median, Polinomial and Loess Regression), each anomaly is estimated by the decomposition of the serie and smooth it using local regression or lowess curve. 

The serie is log transformed before running the smoothing with the function `numpy.log1p()` and then transformed to the original scale by the funtion `numpy.expm1()`. 

To smooth the serie we use the tsmoothie Python library for time series smoothing and outlier detection in a vectorized way.

Function in pipeline: `_decompose_lowess(variable_smooth, missing_values, smooth_fraction)`

Parameters: 

  - `smooth_type` (, lowess) 
  - `periods` (int, 7)
  - `smooth_fraction` (dbl, .2)
    

    
For more information about [Loess Smoothing](https://pypi.org/project/tsmoothie/).    


## Report and results




## Function structures



## Run the pipeline

Single 

Batch

