import numpy as np
from sklearn.preprocessing import MinMaxScaler
from model_transformer.utility.dbms_utils import DBMSUtils


class MinMaxScalerSQL(object):
    """
    This class implements the SQL wrapper for a Sklearn MinMaxScaler object.
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

    def get_params(self, scaler: MinMaxScaler, norm_features, all_features, preprocess_all_features, prev_transform_features=None):

        data_min = scaler.data_min_
        scale = scaler.scale_
        range_min = scaler.min_ + scaler.data_min_ * scaler.scale_

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

        features = prev_transform_features + select_norm_features + other_features

        preprocess_other_features = []
        for f in preprocess_all_features:
            if f not in select_norm_features:
                preprocess_other_features.append(f)

        self.params = {"data_min": data_min, "scale": scale, "range_min": range_min, "out_all_features": features,
                       "norm_features": norm_features, "other_features": preprocess_other_features,
                       'out_transform_features': prev_transform_features, 'preprocess_all_features': preprocess_all_features}

        return self.params

    def query(self, table_name):
        """
        This method generates the SQL query that performs in SQL the minmax normalization.

        :param table_name: the table name where to read the data
        :return: the SQL query that performs in SQL the minmax normalization
        """

        if not self.params:
            raise Exception("No parameters extracted from the fitted MinMaxScaler. Invoke the get_params method.")

        if not isinstance(table_name, str):
            raise TypeError("Wrong data type for parameter table_name. Only string data type is allowed.")

        # extract the scaling parameters from the Sklearn StardardScaler object

        data_min = self.params["data_min"]
        scale = self.params["scale"]
        range_min = self.params["range_min"]
        norm_features = self.params["norm_features"]
        other_features = self.params["other_features"]
        push_attris = self.optimizations['MinMaxScaler'].get('push_attris')
        merge_attris = self.optimizations['MinMaxScaler'].get('merge_attris')
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
                if data_min[i] >= 0:
                    query += "({}-{}) * {} + {} AS {},".format(f, data_min[i], scale[i], range_min[i], f)
                else:
                    query += "({}+{}) * {} + {} AS {},".format(f, -data_min[i], scale[i], range_min[i], f)

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
