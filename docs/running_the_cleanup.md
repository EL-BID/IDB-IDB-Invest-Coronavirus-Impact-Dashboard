# Running the Cleaning Process



## Remove outliers




1. Outlier detection

The methodology implemented considers four types of identification. 

- `_outlier_persist_ad(s, target_column_name, c_param = 3.0, window_param = 7)`
- `_outlier_gesdt_ad(s, target_column_name, alpha_param = 0.3, window_param = 7)`
- `_outlier_seasonal_ad(s, target_column_name, c_param = 3.0)`
- `_outlier_autregr_ad(s, target_column_name, c_param = 3.0, n_steps_param = 1, step_size_param=7)`

The identificaction of outliers do not apply for data observed before 2020-03-31 and between 2020-12-15 and 2021-01-15. 


2. Imputation of outliers

After detecting teh aoutlier a threshold is set to recognize each observation as an outlier. Then, the imputation of each anomaly is estimated by smoothing the serie using local regression. Although four methods were tested. 

- RollingMean
- RollingMedian
- Polinomial
- Loess



Results


## Correct level shifts

Methodology

Parameters

Selection 

Results



## Iterative process


Step 1 and step 2


Final correction


## Run the process

Single 

Batch


## Report and results



## Function structures


## In the pipeline



