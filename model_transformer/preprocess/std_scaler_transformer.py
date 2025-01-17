import numpy as np
from sklearn.preprocessing import StandardScaler
from collections import Iterable
from model_transformer.utility.dbms_utils import DBMSUtils


class StandardScalerSQL(object):
    """
    This class implements the SQL wrapper for a Sklearn StardardScaler object.
    """

    def __init__(self):
        self.params = None
        self.dbms = None
        self.mode = None
        self.optimizations = None

    def set_mode(self, mode: str):
        assert isinstance(mode, str), "Wrong data type for param 'mode'."
        self.mode = mode

    def set_dbms(self, dbms: str):
        self.dbms = dbms

    def set_optimizations(self, optimizations):
        self.optimizations = optimizations


    def transform_model_features_in(self, transform, all_features):
        return all_features


    def get_params(self, scaler, norm_features, all_features, preprocess_all_features, prev_transform_features=None, with_mean=True):
        """
        This method extracts the scaling parameters (i.e., the means and the stds) from the fitted Sklearn
        StardardScaler object.

        :param scaler: the fitted Sklearn StardardScaler object
        :param norm_features: the features where to apply the normalization
        :param all_features: all the features
        :param prev_transform_features: (optional) list of features transformed by previous transfomer
        :param with_mean: (optional) boolean flag that indicates whether the scaling averages have to be extracted or
                          have to be ignored (i.e., set to zeros); this parameter has been added to be compliant with
                          ML.NET which ignores the means during the scaling
        :return: the fitted parameters extracted from the Sklearn StardardScaler object (i.e., averages and 1/stds)
        """

        if not isinstance(scaler, StandardScaler):
            raise TypeError("Wrong data type for parameter scaler. Only Sklearn StardardScaler data type is allowed.")

        if not isinstance(norm_features, Iterable):
            raise TypeError("Wrong data type for parameter norm_features. Only iterable objects are allowed.")

        for f in norm_features:
            if not isinstance(f, str):
                raise TypeError("Wrong data type for single norm_features. Only string data type is allowed.")

        if not isinstance(all_features, Iterable):
            raise TypeError("Wrong data type for parameter all_features. Only iterable objects are allowed.")

        for f in all_features:
            if not isinstance(f, str):
                raise TypeError("Wrong data type for single all_features. Only string data type is allowed.")

        if prev_transform_features is not None:
            if not isinstance(prev_transform_features, Iterable):
                raise TypeError(
                    "Wrong data type for parameter prev_transform_features. Only iterable objects are allowed.")

            for f in prev_transform_features:
                if not isinstance(f, str):
                    raise TypeError(
                        "Wrong data type for single prev_transform_features. Only string data type is allowed.")

        if not isinstance(with_mean, bool):
            raise TypeError("Wrong data type for parameter with_mean. Only boolean data type is allowed.")

        # extract the stds from the fitted Sklearn StardardScaler object
        scaler_std = scaler.scale_

        # extract the averages from the fitted Sklearn StardardScaler object
        scaler_mean = np.zeros(len(scaler_std))
        if with_mean:
            scaler_mean = scaler.mean_

        # compute the features after the application of the StandardScaler
        # the scaled features are prepended to the original list of features
        if prev_transform_features is None:
            prev_transform_features = []

        select_norm_features = []
        for f in norm_features:
            if f in prev_transform_features:
                continue
            select_norm_features.append(f)

        other_features = []
        for f in all_features:
            if f in select_norm_features or f in prev_transform_features:
                continue
            other_features.append(f)
        
        preprocess_other_features = []
        for f in preprocess_all_features:
            if f in select_norm_features or f in prev_transform_features:
                continue
            preprocess_other_features.append(f)

        features = prev_transform_features + select_norm_features + other_features

        self.params = {"avgs": scaler_mean, "stds": scaler_std, "out_all_features": features,
                       "norm_features": norm_features, "other_features": prev_transform_features + preprocess_other_features,
                       'out_transform_features': norm_features, 'preprocess_all_features': preprocess_all_features}

        return self.params

    def query(self, table_name):
        """
        This method generates the SQL query that performs in SQL the standard normalization.

        :param table_name: the table name where to read the data
        :return: the SQL query that performs in SQL the standard normalization
        """

        if not self.params:
            raise Exception("No parameters extracted from the fitted StardardScaler. Invoke the get_params method.")

        if not isinstance(table_name, str):
            raise TypeError("Wrong data type for parameter table_name. Only string data type is allowed.")

        # extract the scaling parameters from the Sklearn StardardScaler object

        avgs = self.params["avgs"]
        stds = self.params["stds"]
        norm_features = self.params["norm_features"]
        other_features = self.params["other_features"]
        push_attris = self.optimizations['StandardScaler'].get('push_attris')
        merge_attris = self.optimizations['StandardScaler'].get('merge_attris')
        if push_attris is None and merge_attris is not None:
            push_attris = merge_attris
        elif push_attris is not None and merge_attris is not None:
            push_attris = push_attris + merge_attris

        dbms_util = DBMSUtils()

        # create the SQL query that implements the normalization in SQL
        query = "SELECT "
        # loop over the features to be normalized and create the portion of query that normalized each feature
        for i in range(len(norm_features)):
            if norm_features[i] not in push_attris:
                f = dbms_util.get_delimited_col(self.dbms, norm_features[i])
                if avgs[i] >= 0:
                    query += "({}-{})/({}) AS {},".format(f, avgs[i], stds[i], f)
                else:
                    query += "({}+{})/({}) AS {},".format(f, -avgs[i], stds[i], f)

        # loop over the remaining features and insert them in the select clause
        for f in other_features:
            f = dbms_util.get_delimited_col(self.dbms, f)
            query += "{},".format(f)
        for f in push_attris:
            f = dbms_util.get_delimited_col(self.dbms, f)
            query += "{},".format(f)
        query = query[:-1]  # remove the last ','

        query += " FROM {}".format(table_name)

        return None, query
