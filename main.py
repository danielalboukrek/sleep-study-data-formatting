# Importing all required packages
import os
import glob
import pandas as pd
import numpy as np
from pandas.errors import EmptyDataError
from functools import reduce
import warnings

# Disabling warning messages that will not affect output for our purposes
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)
pd.options.mode.chained_assignment = None

# Setting the working directory
os.chdir('N:/Sleep/Data/Data Formatting')

# Importing raw data files (replace XXXXXX with the actual filenames after downloading from REDCap)
partinfo = pd.read_csv('XXXXXX.csv')
baseline = pd.read_csv('XXXXXX.csv')
onemonth = pd.read_csv('XXXXXX.csv')
threemonth = pd.read_csv('XXXXXX.csv')
sixmonth = pd.read_csv('XXXXXX.csv')
mema_am = pd.read_excel('XXXXXX.xlsx', sheet_name='AM')
mema_event = pd.read_excel('XXXXXX.xlsx', sheet_name='Event')
mema_gng = pd.read_excel('XXXXXX.xlsx', sheet_name='Go')
mema_pm = pd.read_excel('XXXXXX.xlsx', sheet_name='PM')
mema_random = pd.read_excel('XXXXXX.xlsx', sheet_name='Random')

'''''''''''''''
INTERVIEW DATA
'''''''''''''''
# Renaming variables that were incorrectly named in REDCap
baseline = baseline.rename(columns={"mu_13_t1": "mu_other_t1", "mu_6f_notes_t1": "mu_6c_notes_t1",
                                    "mu_7f_notes_t1": "mu_7c_notes_t1", "mu_8f_notes_t1": "mu_8c_notes_t1",
                                    "mu_8_other_t1": "mu_8b_other_t1", "mu_9f_notes_t1": "mu_9c_notes_t1",
                                    "mu_10f_notes_t1": "mu_10c_notes_t1", "mu_11f_notes_t1": "mu_11c_notes_t1",
                                    "mu_12f_notes_t1": "mu_12c_notes_t1", "mu_13f_notes_t1": "mu_13c_notes_t1",
                                    "mu_14f_notes_t1": "mu_14c_notes_t1", "mu_15f_notes_t1": "mu_15c_notes_t1",
                                    "mu_16f_notes_t1": "mu_16c_notes_t1"})
onemonth = onemonth.rename(columns={"mu_4c_notes_t2": "mu_4b_notes_t2", "mu_5c_notes_t2": "mu_5b_notes_t2",
                                    "mu_9c_notes_t2": "mu_8c_notes_t2", "mu_9c_notes": "mu_9c_notes_t2",
                                    "mu_10c_notes": "mu_10c_notes_t2", "mu_11g_t2": "mu_11c_notes_t2",
                                    "mu_12b_anx_t3": "mu_12b_anx_t2", "thi_hos_t2": "thi_hos_timepoint2",
                                    "thi_php_t2": "thi_php_timepoint2", "thi_res_t2": "thi_res_timepoint2",
                                    "chart_hos_t2": "chart_hos_timepoint2", "chart_php_t2": "chart_php_timepoint2",
                                    "chart_res_t2": "chart_res_timepoint2"})
threemonth = threemonth.rename(columns={"thi_hos_t3": "thi_hos_timepoint3", "thi_php_t3": "thi_php_timepoint3",
                                        "thi_res_t3": "thi_res_timepoint3", "chart_hos_t3": "chart_hos_timepoint3",
                                        "chart_php_t3": "chart_php_timepoint3", "chart_res_t3": "chart_res_timepoint3"})
sixmonth = sixmonth.rename(columns={"thi_hos_t4": "thi_hos_timepoint4", "thi_php_t4": "thi_php_timepoint4",
                                    "thi_res_t4": "thi_res_timepoint4", "chart_hos_t4": "chart_hos_timepoint4",
                                    "chart_php_t4": "chart_php_timepoint4", "chart_res_t4": "chart_res_timepoint4"})

# Creating a list for each data file with the column names
baseline_labels = baseline.columns.to_list()
onemonth_labels = onemonth.columns.to_list()
threemonth_labels = threemonth.columns.to_list()
sixmonth_labels = sixmonth.columns.to_list()

# For the C-SSRS variable names, replacing 'sla' with 'lt' so that all variables are consistently named throughout
for i in onemonth_labels:
    if "sla" in i and i[0] == 'c':
        onemonth_labels[onemonth_labels.index(i)] = i.replace("sla", "lt")
for i in onemonth_labels:
    if "fu_t2" in i:
        onemonth_labels[onemonth_labels.index(i)] = i.replace("fu_t2", "t2_t2")
for i in threemonth_labels:
    if "sla" in i and i[0] == 'c':
        threemonth_labels[threemonth_labels.index(i)] = i.replace("sla", "lt")
for i in threemonth_labels:
    if "fu_t3" in i:
        threemonth_labels[threemonth_labels.index(i)] = i.replace("fu_t3", "t3_t3")
for i in sixmonth_labels:
    if "sla" in i and i[0] == 'c':
        sixmonth_labels[sixmonth_labels.index(i)] = i.replace("sla", "lt")
for i in sixmonth_labels:
    if "fu_t3_t4" in i:
        sixmonth_labels[sixmonth_labels.index(i)] = i.replace("fu_t3_t4", "t4_t4")

onemonth.columns = onemonth_labels
threemonth.columns = threemonth_labels
sixmonth.columns = sixmonth_labels

# Creating a dataframe with the combined data from all time points and creating a list with the column names
df = pd.concat([baseline, onemonth, threemonth, sixmonth, partinfo], axis=1)
df_labels = df.columns.to_list()

# Creating a list containing the roots of all variables that need to be combined across multiple time points
# Also creating a list of the variable names to be included in the final dataset
varroots = pd.read_csv('variable roots.csv')
varroots_labels = varroots.columns.to_list()
finalvars = pd.read_csv('final variables.csv')
finalvars_labels = finalvars.columns.to_list()

# For each variable that appears across multiple timepoints, combining into one variable
col = {}
for i in varroots:
    temp = ([a for a in baseline_labels if i in a] + [b for b in onemonth_labels if i in b] +
            [c for c in threemonth_labels if i in c] + [d for d in sixmonth_labels if i in d])
    col[i] = temp

# Deleting duplicate variables
df = df.loc[:, ~df.columns.duplicated()]
print(col)
for key, value in col.items():
    temp = list(df[variable] for variable in value)
    df[str(key)] = reduce(lambda x, y: x.combine_first(y), temp)

# Creating final dataset containing the variables selected above, and removing duplicate variables
df_final = df[finalvars_labels]

# Selecting variables containing dates and copying them to a new datafile
date_labels = ["datec", "cssrs_intdate", "sds_date", "date_mood_t1", "date_psychotic_t1", "date_trauma_t1",
               "date_acasi_bl", "date_igt"]

df_dates = df_final.loc[:, (col for col in df_final if col in date_labels)]
df_dates.insert(0, 'record_id', df_final['record_id'])

# Converting date variables to datetime format
df_final['datec'] = pd.to_datetime(df_final['datec'])
df_final['cssrs_intdate'] = pd.to_datetime(df_final['cssrs_intdate'])
df_final['sds_date'] = pd.to_datetime(df_final['sds_date'])
df_final['date_acasi_bl'] = pd.to_datetime(df_final['date_acasi_bl'])
df_final['date_mood_t1'] = pd.to_datetime(df_final['date_mood_t1'])
df_final['date_psychotic_t1'] = pd.to_datetime(df_final['date_psychotic_t1'])
df_final['date_trauma_t1'] = pd.to_datetime(df_final['date_trauma_t1'])
df_final['date_igt'] = pd.to_datetime(df_final['date_igt'])

# Converting each date variable to an integer reflecting number of days from consent date
consentdates = {}
grouped = df_final.groupby('record_id')
for name, group in grouped:
    datec_grouped = group.datec.tolist()
    startdate = datec_grouped[0]
    consentdates[name] = startdate
    for index, row in group.iterrows():
        if row['cssrs_intdate'] is not pd.NaT:
            df_final.loc[index, 'cssrs_intdate_new'] = (row['cssrs_intdate'] - startdate).days
        if row['sds_date'] is not pd.NaT:
            df_final.loc[index, 'sds_date_new'] = (row['sds_date'] - startdate).days
        if row['date_acasi_bl'] is not pd.NaT:
            df_final.loc[index, 'date_acasi_bl_new'] = (row['date_acasi_bl'] - startdate).days
        if row['date_mood_t1'] is not pd.NaT:
            df_final.loc[index, 'date_mood_t1_new'] = (row['date_mood_t1'] - startdate).days
        if row['date_psychotic_t1'] is not pd.NaT:
            df_final.loc[index, 'date_psychotic_t1_new'] = (row['date_psychotic_t1'] - startdate).days
        if row['date_trauma_t1'] is not pd.NaT:
            df_final.loc[index, 'date_trauma_t1_new'] = (row['date_trauma_t1'] - startdate).days
        if row['date_igt'] is not pd.NaT:
            df_final.loc[index, 'date_igt_new'] = (row['date_igt'] - startdate).days

# Defining a function to swap two columns in the same dataset
def swap_columns(df, col1, col2):
    col_list = df.columns.to_list()
    x, y = col_list.index(col1), col_list.index(col2)
    col_list[y], col_list[x] = col_list[x], col_list[y]
    df = df[col_list]
    return df

# Swapping the old date variables with the new integer variables so that they appear where the orginal date variables
# appeared in the datafile
df_final = swap_columns(df_final, 'cssrs_intdate', 'cssrs_intdate_new')
df_final = swap_columns(df_final, 'sds_date', 'sds_date_new')
df_final = swap_columns(df_final, 'date_acasi_bl', 'date_acasi_bl_new')
df_final = swap_columns(df_final, 'date_mood_t1', 'date_mood_t1_new')
df_final = swap_columns(df_final, 'date_psychotic_t1', 'date_psychotic_t1_new')
df_final = swap_columns(df_final, 'date_trauma_t1', 'date_trauma_t1_new')
df_final = swap_columns(df_final, 'date_igt', 'date_igt_new')

# Dropping the original date variables from the main datafile, now that they were already moved to a separate file
df_final = df_final.drop(columns=[col for col in df_final if col in date_labels])

'''''''''''''''
   mEMA DATA
'''''''''''''''
# For each mEMA survey, converting all dates to a set of integers reflecting time since the mEMA survey started,
# dropping the original date variables, and exporting to CSV file
grouped = mema_am.groupby('record_id')
mema_am['consentdate'] = mema_am['record_id'].map(consentdates)
mema_am['monthofyear'] = pd.to_datetime(mema_am['actual_start']).dt.month
mema_am['dayofweek'] = pd.to_datetime(mema_am['actual_start']).dt.dayofweek
mema_am['hourofday'] = pd.to_datetime(mema_am['actual_start']).dt.hour
mema_am['minuteofday'] = pd.to_datetime(mema_am['actual_start']).dt.minute
for name, group in grouped:
    for index, row in group.iterrows():
        if row['actual_start'] is not pd.NaT:
            mema_am.loc[index, 'dayssinceconsent'] = (row['actual_start'] - row['consentdate']).days
df_dates.insert(len(df_dates.columns), 'record_id_AM', mema_am['record_id'])
df_dates.insert(len(df_dates.columns), 'mEMA_AM_timestamp', mema_am['actual_start'])
mema_am = mema_am.drop(columns=['actual_start'])
mema_am = mema_am.drop(columns=['consentdate'])
mema_am.to_csv("output/mEMA_am.csv", index=False)

grouped = mema_event.groupby('record_id')
mema_event['consentdate'] = mema_event['record_id'].map(consentdates)
mema_event['monthofyear'] = pd.to_datetime(mema_event['actual_start']).dt.month
mema_event['dayofweek'] = pd.to_datetime(mema_event['actual_start']).dt.dayofweek
mema_event['hourofday'] = pd.to_datetime(mema_event['actual_start']).dt.hour
mema_event['minuteofday'] = pd.to_datetime(mema_event['actual_start']).dt.minute
for name, group in grouped:
    for index, row in group.iterrows():
        if row['actual_start'] is not pd.NaT:
            mema_event.loc[index, 'dayssinceconsent'] = (row['actual_start'] - row['consentdate']).days
df_dates.insert(len(df_dates.columns), 'record_id_Event', mema_event['record_id'])
df_dates.insert(len(df_dates.columns), 'mEMA_event_timestamp', mema_event['actual_start'])
mema_event = mema_event.drop(columns=['actual_start'])
mema_event = mema_event.drop(columns=['consentdate'])
mema_event.to_csv("output/mema_event.csv", index=False)

grouped = mema_gng.groupby('record_id')
mema_gng['consentdate'] = mema_gng['record_id'].map(consentdates)
mema_gng['monthofyear'] = pd.to_datetime(mema_gng['actual_start']).dt.month
mema_gng['dayofweek'] = pd.to_datetime(mema_gng['actual_start']).dt.dayofweek
mema_gng['hourofday'] = pd.to_datetime(mema_gng['actual_start']).dt.hour
mema_gng['minuteofday'] = pd.to_datetime(mema_gng['actual_start']).dt.minute
for name, group in grouped:
    for index, row in group.iterrows():
        if row['actual_start'] is not pd.NaT:
            mema_gng.loc[index, 'dayssinceconsent'] = (row['actual_start'] - row['consentdate']).days
df_dates.insert(len(df_dates.columns), 'record_id_GNG', mema_gng['record_id'])
df_dates.insert(len(df_dates.columns), 'mEMA_GNG_timestamp', mema_gng['actual_start'])
mema_gng = mema_gng.drop(columns=['actual_start'])
mema_gng = mema_gng.drop(columns=['consentdate'])
mema_gng.to_csv("output/mema_gng.csv", index=False)

grouped = mema_pm.groupby('record_id')
mema_pm['consentdate'] = mema_pm['record_id'].map(consentdates)
mema_pm['monthofyear'] = pd.to_datetime(mema_pm['actual_start']).dt.month
mema_pm['dayofweek'] = pd.to_datetime(mema_pm['actual_start']).dt.dayofweek
mema_pm['hourofday'] = pd.to_datetime(mema_pm['actual_start']).dt.hour
mema_pm['minuteofday'] = pd.to_datetime(mema_pm['actual_start']).dt.minute
for name, group in grouped:
    for index, row in group.iterrows():
        if row['actual_start'] is not pd.NaT:
            mema_pm.loc[index, 'dayssinceconsent'] = (row['actual_start'] - row['consentdate']).days
df_dates.insert(len(df_dates.columns), 'record_id_PM', mema_pm['record_id'])
df_dates.insert(len(df_dates.columns), 'mEMA_PM_timestamp', mema_pm['actual_start'])
mema_pm = mema_pm.drop(columns=['actual_start'])
mema_pm = mema_pm.drop(columns=['consentdate'])
mema_pm.to_csv("output/mema_pm.csv", index=False)

grouped = mema_random.groupby('record_id')
mema_random['consentdate'] = mema_random['record_id'].map(consentdates)
mema_random['monthofyear'] = pd.to_datetime(mema_random['actual_start']).dt.month
mema_random['dayofweek'] = pd.to_datetime(mema_random['actual_start']).dt.dayofweek
mema_random['hourofday'] = pd.to_datetime(mema_random['actual_start']).dt.hour
mema_random['minuteofday'] = pd.to_datetime(mema_random['actual_start']).dt.minute
for name, group in grouped:
    for index, row in group.iterrows():
        if row['actual_start'] is not pd.NaT:
            mema_random.loc[index, 'dayssinceconsent'] = (row['actual_start'] - row['consentdate']).days
df_dates.insert(len(df_dates.columns), 'record_id_Random', mema_random['record_id'])
df_dates.insert(len(df_dates.columns), 'mEMA_Random_timestamp', mema_random['actual_start'])
mema_random = mema_random.drop(columns=['actual_start'])
mema_random = mema_random.drop(columns=['consentdate'])
mema_random.to_csv("output/mema_random.csv", index=False)

# Outputing df_final and df_dates to CSV files
df_final.to_csv("output/master.csv", index=False)
df_dates.to_csv("N:/Sleep/Data/Data Formatting/output/dates.csv", index=False)
