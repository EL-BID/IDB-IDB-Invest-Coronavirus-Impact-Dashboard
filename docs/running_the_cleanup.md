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


### Anomalies 


1. Anomaly detection

The methodology implemented considers three types of anomaly detectors.

- Persist Anomaly Detector

This detector compares each time series value with its previous values. It compares time series values with the values of their preceding time windows, and identifies a time point as anomalous if the change of value from its preceding average or median is anomalously large.

Function in pipeline: `_outlier_persist_ad(s, target_column_name, c_param, window_param)`

Parameters:

    - `window` (int or str, 7) – Size of the preceding time window.
    - `c` (float, 1.5 to 3) – Factor used to determine the bound of normal range based on historical interquartile range.
    - `side` (str, both) – to detect anomalous positive and negative changes;
    - `min_periods` (int, None) – Minimum number of observations in each window required to have a value for that window.
    - `agg` (str, median) – Aggregation operation of the time window, either “mean” or “median”


- Seasonal Anomaly Detector

This detector identifies anomalous values away from seasonal pattern. It uses a seasonal decomposition transformer to remove seasonal pattern (as well as trend), and identifies a time point as anomalous when the residual of seasonal decomposition is anomalously large.

Function in pipeline: `_outlier_seasonal_ad(s, target_column_name, c_param = 3.0)`

Parameters: 

    - `freq` (int, optional) - Length of a seasonal cycle as the number of time points in a cycle.
    - `c` (float, optional) - Factor used to determine the bound of normal range based on historical interquartile range
    - `side` (str, optional) - If both, : to detect anomalous positive and negative residuals;
    - `trend` (bool, optional) -  Whether to extract trend during decomposition.

- Autoregression Anomaly Detector

This detector indentifies anomalous autoregression property in time series. The algorthm applies a regressor to learn autoregressive property of the time series, and identifies a time point as anomalous when the residual of autoregression is anomalously large.

Function in pipeline: `_outlier_autregr_ad(s, target_column_name, c_param = 3.0, n_steps_param = 1, step_size_param=7)`

Parameters: 
    
    - `n_steps` (int, 1) - Number of steps (previous values) to include in the model.
    - `step_size` (int, 7) - Length of a step. For example, if n_steps=2, step_size=3, X_[t-3] and X_[t-6] will be used to predict X_[t].
    - `regressor` (object, linear) - Regressor to be used. Same as a scikit-learn regressor, it should minimally have fit and predict methods. 
    - `c` (float, 1.5 to 3.0) - Factor used to determine the bound of normal range based on historical interquartile range. 
    - `side` (str, both) - “both”, to detect anomalous positive and negative residuals;

The identificaction of outliers do not apply for data observed before 2020-03-31 and between 2020-12-15 and 2021-01-15. 


2. Anomaly imputation

After detecting an anomaly a threshold is set to recognize each observation as an outlier.In the pipeline is for at least **one** detector to identify the observation as an anomaly. Then, the imputation of each anomaly is estimated by smoothing the serie using local regression. Although four methods were tested (Rolling Mean, Rolling Median, Polinomial and Loess Regression). 



### Level Shifts



----

Results

Correct level shifts

Methodology

Parameters

Selection 

Results



## Run the process

Single 

Batch


## Report and results



## Function structures


## In the pipeline



