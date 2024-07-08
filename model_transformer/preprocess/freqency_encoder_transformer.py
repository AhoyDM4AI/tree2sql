from model_transformer.utility.dbms_utils import DBMSUtils
from collections import Counter
from model_transformer.utility.loader import load_dataset

class FrequencyEncoderSQL(object):

    def __init__(self):
        self.dbms = None
        self.params = None
    
    def set_dbms(self, dbms: str):
        self.dbms = dbms

    def transform_model_features_in(self, transform, all_features):
        return all_features

    def get_params(self, fitted_transformer, infos, all_features, preprocess_all_features, prev_transform_features):
        attrs = infos['attrs']
        train_data_path = infos['train_data_path']
        other_features = [f for f in preprocess_all_features if f not in attrs]
        train_data = load_dataset(train_data_path)
        self.params = {'out_all_features': all_features, 'out_transform_features': prev_transform_features,
                       'transform_features': attrs, 'other_features': other_features, 'train_data':train_data,
                       'train_data_path': train_data_path, 'preprocess_all_features': preprocess_all_features}
        return self.params

    def query(self, table_name):
        dbms_util = DBMSUtils()

        transform_features = self.params['transform_features']
        other_features = self.params['other_features']
        train_data = self.params['train_data']
        

        # create the SQL query that implements the Frequency Encoder in SQL
        query = "SELECT "

        # loop over the preprocess features and insert them in the select clause
        for f in transform_features:
            if not ('is_push' in transform_features[f] and transform_features[f]['is_push'] or 'is_merge' in transform_features[f] and transform_features[f]['is_merge']):
                data_list = train_data[f]
                # get the map of value to frequency
                count = Counter(data_list)
                count = count.most_common()
                f = dbms_util.get_delimited_col(self.dbms, f)
                query += "CASE "
                for item in count:
                    ele, freq = item
                    query += f"WHEN {f} = '{ele}' THEN {freq} "
                query += "ELSE 0 "
                query += f"END AS {f}, "

        # loop over the other features and insert them in the select clause
        for f in other_features:
            f = dbms_util.get_delimited_col(self.dbms, f)
            query += "{},".format(f)

        for f in transform_features:
            if 'is_push' in transform_features[f] and transform_features[f]['is_push'] or 'is_merge' in transform_features[f] and transform_features[f]['is_merge']:
                f = dbms_util.get_delimited_col(self.dbms, f)
                query += "{},".format(f)
                
        query = query[:-1]  # remove the last ','

        query += " FROM {}".format(table_name)

        return None, query