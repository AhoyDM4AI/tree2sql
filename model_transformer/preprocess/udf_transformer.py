from model_transformer.utility.dbms_utils import DBMSUtils

class UDFSQL(object):

    def __init__(self):
        self.params = None
        self.dbms = None

    def set_dbms(self, dbms: str):
        self.dbms = dbms


    def transform_model_features_in(self, transform, all_features):
        return all_features


    def get_params(self, fitted_transformer, udf_infos, all_features, preprocess_all_features, prev_transform_features):
        not_udf_atrributes = []
        for attr_name in preprocess_all_features:
            if attr_name not in udf_infos or udf_infos[attr_name]['is_push']:
                not_udf_atrributes.append(attr_name)

        self.params = {"out_all_features": all_features, 'out_transform_features': prev_transform_features,
                       "udf_infos": udf_infos, 'not_udf_attributes': not_udf_atrributes, 'preprocess_all_features': preprocess_all_features}
        return self.params
    
    def query(self, table_name):
        dbms_util = DBMSUtils()
        not_udf_atrributes = self.params['not_udf_attributes']
        udf_info = self.params['udf_infos']

        # create the SQL query that implements the normalization in SQL
        query = "SELECT "

        # loop over the udf features and insert them in the select clause
        for attri_name in udf_info:
            if not udf_info[attri_name]['is_push']:
                udf_name = udf_info[attri_name]['udf_name']
                attri_name = dbms_util.get_delimited_col(self.dbms, attri_name)
                query += f"{udf_name}({attri_name}) AS {attri_name},"

        # loop over the remaining features and insert them in the select clause
        for f in not_udf_atrributes:
            f = dbms_util.get_delimited_col(self.dbms, f)
            query += "{},".format(f)
        query = query[:-1]  # remove the last ','

        query += " FROM {}".format(table_name)

        return None, query
    

