import pandas as pd
import category_encoders as ce
from collections import Counter
from model_transformer.utility.loader import load_dataset, load_model

def auto_config(pipeline, data_path, encoders_path, feature_names, features, optimizations, preprocessors):
    # get the sklearn tree object
    model_name = pipeline['model']['model_name']
    trained_model = pipeline['model']['trained_model']

    # Transform initial data into states that can be input into the model
    transformed_data, fitted_info = transform_data(data_path, encoders_path, features, pipeline, preprocessors)

    # travel the tree to get the fusion statistics information
    if model_name == 'DecisionTreeClassifier':
        fusion_statistics = get_fusion_statistics(trained_model, transformed_data, feature_names, fitted_info)
    elif model_name == 'RandomForestClassifier':
        trees = trained_model.estimators_
        total_fusion_statistcs = {}
        for tree_model in trees:
            fusion_statistics = get_fusion_statistics(tree_model, transformed_data, feature_names, fitted_info)
            total_fusion_statistcs = {key: total_fusion_statistcs.get(key, 0) + fusion_statistics.get(key, 0) for key in set(total_fusion_statistcs) | set(fusion_statistics)}
        fusion_statistics = total_fusion_statistcs

    # case statistics
    case_statistics = get_case_statistics(transformed_data, feature_names, fitted_info)

    # for every preprocess transformer, 
    # calculate the estimated cost of the different sql implementation method(case, fusion, join)


    # according to the estimated cost, choose the implementation method
    # by modify the configuration dictionary: optimizations and preprocessors


    return optimizations, preprocessors


def get_fusion_statistics(model, data, features_names, fitted_info):
    statistics = None
    # get the decision paths in the dataset
    paths = model.decision_path(data)
    # get the index of a non-zero element
    row_indices, col_indices = paths.nonzero()
    # get the path of each sample and the features it passes through
    for i in range(len(paths.indptr) - 1):
        start_idx = paths.indptr[i]
        end_idx = paths.indptr[i + 1]
        features_used = [model.tree_.feature[col_indices[j]] for j in range(start_idx, end_idx)]
        counts = Counter(features_used)
        # for idx in counts:
        #     feature = features_names[idx]
        #     if feature in fitted_info:
        #         counts[idx] = counts[idx] * fitted_info[feature][data.loc[data.index[i], feature]]
        if statistics is None:
            statistics = counts
        else:
            statistics = statistics + counts
        
    return {features_names[key]:statistics[key] for key in statistics}



def get_case_statistics(data, features_names, fitted_info):
    lines = len(data)
    statistics = {feature:lines for feature in features_names if feature not in fitted_info}
    for feature in fitted_info:
        statistics[feature] = 0
        feature_counts = Counter(data[feature])
        for key in feature_counts:
            statistics[feature] += feature_counts[key] * fitted_info[feature][key]
    
    return statistics



def transform_data(data_path, encoders_path, features, pipeline, preprocessors):
    df = load_dataset(data_path)
    category_encoders = load_model(encoders_path)
    X = df[features]
    # clean data
    for col in X.columns:
        if len(X[col].isna().unique()) > 1:
            most_common_value = X[col].value_counts().idxmax()  # get the most frequency value
            X[col].fillna(most_common_value, inplace=True)  # imputate the null value
    
    # information of the fitted_transformer
    fitted_info = {}

    # sample the data
    data = X.sample(frac=0.1)
    data_copy = data.copy()

    # transform the data
    transforms = pipeline['transforms']
    for transform in transforms:
        if transform.get('fitted_transform'):
            fitted_transform = transform['fitted_transform']
            if transform['transform_name'] == 'OneHotEncoder':
                for i, feature in enumerate(transform['transform_features']):
                    data_tr = fitted_transform.transform(data[[feature]]).toarray()
                    idx = data.columns.tolist().index(feature)
                    data = pd.concat((data.iloc[:, :idx], pd.DataFrame(data_tr, index=data.index, columns=fitted_transform.categories_[i]), data.iloc[:, idx+1:]), axis=1)
            else:
                data[transform['transform_features']] = fitted_transform.transform(data[transform['transform_features']])
        else:
            if transform['transform_name'] == 'BinaryEncoder':
                binary_encoder = category_encoders['BinaryEncoder']
                data = binary_encoder.transform(data)
            elif transform['transform_name'] == 'FrequencyEncoder':
                counter_encoder = category_encoders['FrequencyEncoder']
                data = counter_encoder.transform(data)
                for feature in transform['transform_features']['attrs'].keys():
                    count_order = {v:(i+1) for i,v in enumerate(counter_encoder.mapping[feature])}
                    count_order[0] = len(counter_encoder.mapping[feature])
                    fitted_info[feature] = count_order
            elif transform['transform_name'] == 'TargetEncoder':
                target_encoder = category_encoders['TargetEncoder']
                data = target_encoder.transform(data)
            elif transform['transform_name'] == 'LeaveOneOutEncoder':
                leaveoneout_encoder = category_encoders['LeaveOneOutEncoder']
                data = leaveoneout_encoder.transform(data)
                # data_copy[transform['transform_features']['target']]
                
    return data, fitted_info
