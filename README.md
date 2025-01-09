# Project
Outputting Swiss public transport options based on user-specified criteria

[Video Presentation Link](https://drive.google.com/file/d/1kEiY0OPtfdXo1725JZ5B97gK9cgS-qNv/view?usp=sharing)

## Description
Group F Final Project Implementation.

We use the **Connection Scan Algorithm** to find journeys and a **Random Forest** model to predict connection delays.



## How To

### Step 1
Run the `01_data_preparation.ipynb` notebook.

Modify **object_id** at the top of the notebook for your intended region!

After this notebook is run, you should have the following files in /data/:
- footpaths.csv
- timetable.csv
- stops.csv
- isdaten_stops_full.csv
- stops_matching.csv

### Step 2
Run the `02_delay_model_training.ipynb` notebook.

After this notebook is run, you should have the following files:

- `/user/<username>/features_with_edge_stats.parquet`
- the trained delay inference model in /user/`username`/models

where <username> is the username used to access PySpark. 

You can access the username from `spark.conf.get("spark.executorEnv.USERNAME", "default_value")`.

**Note:**
We initially filtered the istdaten data on `AN_PROGNOSE_STATUS=='REAL'`. This improved our model's performance. We got a R Squared = 0.6818, and Mean Absolute Error = 18.748 (seconds). After we processed the stop ids (matching the istdaten stop_id with the sbb_orc_stop_times stop_ids by lat and lon), we got even better prediction result (R Squared > 0.7). However, we noticed that `AN_PROGNOSE_STATUS=='REAL'` will cause us to lose a lot of data, and we couldn't find the matching stop_ids when calculating the route. So we decided not to filter on this, we lost some accuracy (now Mean Absolute Error (MAE): 67.187, R-squared (R²): 0.569), but we have more stops matched for the routing algorithm now.

### Step 3
Run the `03_user_interface.ipynb` notebook.

The user interface will appear at the bottom when all cells are run. Modify parameters desired and 'Find Journeys'. 

The journeys will be output visually and in the text below.

> Note that we added the Day of Week option after our video presentation! 

## Authors
- Lorenzo Drudi
- Kaede Johnson
- Li Ke
- Hans Kristian B. Kvaerum
- Viacheslav Surkov
- Xingyue Zhang

## References 
- [Connection Scan Algorithm](https://arxiv.org/abs/1703.05997)