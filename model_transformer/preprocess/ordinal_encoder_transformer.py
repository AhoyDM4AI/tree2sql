from model_transformer.utility.dbms_utils import DBMSUtils

class OrdinalEncoderSQL(object):

    def __init__(self):
        self.dbms = None
        self.params = None
        self.optimizations = None

    def set_dbms(self, dbms: str):
        self.dbms = dbms

    def set_optimizations(self, optimizations):
        self.optimizations = optimizations


    def transform_model_features_in(self, transform, all_features):
        return all_features
    

    def get_params(self, fitted_transformer, transform_features, all_features, preprocess_all_features, prev_transform_features):
        categories = fitted_transformer.categories_
        self.all_features = all_features

        selected_encoder_features = []
        for feature in transform_features:
            if feature not in prev_transform_features:
                selected_encoder_features.append(feature)

        remain_features = []
        for feature in all_features:
            if feature not in prev_transform_features and feature not in selected_encoder_features:
                remain_features.append(feature)

        features = prev_transform_features + selected_encoder_features + remain_features

        other_features = []
        for feature in preprocess_all_features:
            if feature not in transform_features:
                other_features.append(feature)

        self.params = {'out_all_features': features, 'out_transform_features': transform_features,
                        'categories': categories, 'other_features': other_features, 'preprocess_all_features': preprocess_all_features}

        return self.params


    def query(self, table_name):
        dbms_util = DBMSUtils()

        encoder_features = self.params['out_transform_features']
        other_features = self.params['other_features']
        categories = self.params['categories']
        push_attris = self.optimizations['OrdinalEncoder'].get('push_attris')
        merge_attris = self.optimizations['OrdinalEncoder'].get('merge_attris')
        if push_attris is None and merge_attris is not None:
            push_attris = merge_attris
        elif push_attris is not None and merge_attris is not None:
            push_attris = push_attris + merge_attris

        # create the SQL query that implements the OrdinalEncoder in SQL
        query = "SELECT "

        # loop over the preprocess features and insert them in the select clause
        for i in range(len(encoder_features)):
            if encoder_features[i] not in push_attris:
                classes = categories[i]
                f = dbms_util.get_delimited_col(self.dbms, encoder_features[i])
                query += "CASE "
                for j in range(len(classes)):
                    if type(classes[j]) == str:
                        query += f'WHEN {f} = \'{classes[j]}\' THEN {j} '
                    else:
                        query += f'WHEN {f} = {classes[j]} THEN {j} '
                query += f"END AS {f},"

        # loop over the other features and insert them in the select clause
        for f in other_features:
            f = dbms_util.get_delimited_col(self.dbms, f)
            query += "{},".format(f)

        for f in push_attris:
            f = dbms_util.get_delimited_col(self.dbms, f)
            query += "{},".format(f)    
        query = query[:-1]  # remove the last ','

        query += " FROM {}".format(table_name)

        return None, query