from model_transformer.utility.dbms_utils import DBMSUtils

class LabelEncoderSQL(object):

    def __init__(self):
        self.dbms = None
        self.all_features = None
    
    
    def set_dbms(self, dbms: str):
        self.dbms = dbms

    def transform_model_features_in(self, transform, all_features):
        return all_features

    def get_params(self, fitted_transformer, transform_features, all_features, preprocess_all_features, prev_transform_features):
        classes = fitted_transformer.classes_
        self.all_features = all_features
        other_features = []
        for feature in preprocess_all_features:
            if feature not in transform_features:
                other_features.append(feature)

        self.params = {'out_all_features': all_features, 'out_transform_features': transform_features,
                        'classes': classes, 'other_features': other_features, 'preprocess_all_features': preprocess_all_features}

        return self.params

    def query(self, table_name):
        dbms_util = DBMSUtils()

        encoder_features = self.params['out_transform_features']
        other_features = self.params['other_features']
            

        # create the SQL query that implements the preprocess in SQL
        query = "SELECT "

        # loop over the preprocess features and insert them in the select clause
        for f in encoder_features:
            print(1)
        # loop over the other features and insert them in the select clause
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      

        query += " FROM {}".format(table_name)

        return None, query
