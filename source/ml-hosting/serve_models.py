# This will serve the ML algorithm. Steps:

# for a user:
#     get user's data
#     get some other users' data
#     feed these into the algorithm
#     store result into mongodb
# and somethign else?

# reference: https://spark.apache.org/docs/2.3.0/ml-classification-regression.html
# reference: https://github.com/apache/spark/blob/master/examples/src/main/python/ml/gradient_boosted_tree_classifier_example.py

from mmlspark import LightGBMClassifier

# have df with pivoted tables


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

df_features = 

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
    