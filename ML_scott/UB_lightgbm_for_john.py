

import pandas as pd
import numpy as np
import json

get_ipython().run_line_magic('matplotlib', 'inline')
import matplotlib.pyplot as plt
import seaborn as sns
plt.style.use('Cole.mplstyle')

from collections import defaultdict
from sklearn import svm
from sklearn import metrics
import lightgbm as lgb

############################################################
# Code for going from space-separated files to digraph table
############################################################

all_users = np.arange(1,76)
all_sess = [0, 1, 2]
all_tasks = [0, 1]

N_char_trial = 100

dg_df_list = []
for user_i in all_users:
    if user_i % 5 == 0:
        print(user_i)

    for sess_i in all_sess:
        for task_i in all_tasks:
            # Determine paths to data
            file_path = 'data_raw/ub/s{:d}/baseline/{:03d}{:d}0{:d}.txt'.format(
                sess_i, user_i, sess_i, task_i)

            df = pd.read_csv(file_path, delimiter=' ',
                     header=None, names=['key', 'event', 'time'])
            df['time'] = df['time'] - df['time'][0]

            # Split task into trials
            df_down = df[df['event']=='KeyDown']
            trial_idx_bounds = list(df_down[::100].index)
            N_trials = len(trial_idx_bounds) - 1
            for i_trial in range(N_trials):
                df_trial = df.loc[trial_idx_bounds[i_trial]:trial_idx_bounds[i_trial+1]-1]

                # Compute digraphs
                trial_name = '{:03d}_{:d}_{:d}_{:03d}'.format(user_i, sess_i, task_i, i_trial)
                dfd = df_trial[df_trial['event']=='KeyDown'].reset_index(drop=True)
                dfd['dg_time'] = dfd['time'].diff()
                dfd['key_prev'] = np.insert(dfd['key'].values[:-1], 0, np.nan)
                dfd['key_pair'] = dfd['key_prev'] + '_' + dfd['key']
                df_dg = dfd[['key_pair', 'dg_time']].dropna().reset_index(drop=True)
                df_dg['user_id'] = user_i
                df_dg['sess'] = sess_i
                df_dg['task'] = task_i
                df_dg['trial'] = i_trial

                dg_df_list.append(df_dg)
df_all_dg = pd.concat(dg_df_list)
df_all_dg.to_csv('all_digraphs.csv')



############################################################
# LightGBM model
############################################################

# Parameters for training
param = {'num_leaves':31, 'num_trees':200, 'objective':'binary',
         'metric': 'auc'}
# number of boosting iterations
num_round = 100

# Create feature matrix
df_feat = df_all_dg.pivot_table(
    index=['user_id', 'sess', 'task', 'trial'],
    columns='key_pair', values='dg_time', aggfunc='median').reset_index()
df_feat_users = df_feat.drop(['user_id', 'sess', 'task', 'trial'], axis=1)

lgbm_eval_dict = defaultdict(list)
for user_i in all_users:
    # Make training data
    df_temp = df_feat[df_feat['sess'].isin([0,1])]
    
    col_names = list(df_feat_users.columns)
    train_label = np.array((df_temp['user_id']==user_i).values, dtype=int)
    train_data_mat = df_temp.drop(['user_id', 'sess','task','trial'], axis=1).as_matrix()
    train_data = lgb.Dataset(train_data_mat, label=train_label, feature_name=col_names)

    # Make test data
    df_temp = df_feat[df_feat['sess']==2]
    test_label = np.array((df_temp['user_id']==user_i).values, dtype=int)
    test_data_mat = df_temp.drop(['user_id', 'sess','task','trial'], axis=1).as_matrix()
    test_data = lgb.Dataset(test_data_mat, label=test_label, feature_name=col_names)

    # Train lgbm
#     bst = lgb.train(param, train_data, num_round, valid_sets=[test_data])
    bst = lgb.train(param, train_data, num_round)
    bst.save_model('lgbm_models/{:d}.txt'.format(user_i))

    # Make predictions
    ypred = bst.predict(test_data_mat)

    # Compute AUC and EER
    auc = metrics.roc_auc_score(test_label, ypred)
    fpr, tpr, thresholds = metrics.roc_curve(test_label, ypred)
    fnr = 1 - tpr
    idx_eer = np.argmin(np.abs(fpr - fnr))
    print('User {:d}, AUC = {:.3f}, EER = {:.3f}'.format(user_i, auc, fnr[idx_eer]))
    
    lgbm_eval_dict['user_id'].append(user_i)
    lgbm_eval_dict['auc'].append(auc)
    lgbm_eval_dict['eer'].append(fnr[idx_eer])
    lgbm_eval_dict['ypred'].append(ypred)
    lgbm_eval_dict['test_label'].append(test_label)
    
