from model_transformer.utility.dbms_utils import DBMSUtils

class ImputationSQL(object):

    def __init__(self):
        self.params = None
        self.dbms = None

    def set_dbms(self, dbms: str):
        self.dbms = dbms

    def transform_model_features_in(self, transform, all_features):
        return all_features

    def get_params(self, fitted_transformer, imputation_infos, all_features, preprocess_all_features, prev_transform_features):
        not_imputation_atrributes = []
        for attr_name in preprocess_all_features:
            if attr_name not in imputation_infos:
                not_imputation_atrributes.append(attr_name)

        self.params = {"out_all_features": all_features, 'out_transform_features': prev_transform_features,
                       "imputation_infos": imputation_infos, 'not_imputation_atrributes': not_imputation_atrributes
                       , 'preprocess_all_features': preprocess_all_features}
        return self.params
    
    def query(self, table_name):
        dbms_util = DBMSUtils()
        not_imputation_atrributes = self.params['not_imputation_atrributes']
        imputation_infos = self.params['imputation_infos']

        # create the SQL query that implements the normalization in SQL
        query = "SELECT "

        # loop over the udf features and insert them in the select clause
        for attri_name in imputation_infos:
            if not('is_push' in imputation_infos[attri_name] and imputation_infos[attri_name]['is_push']): 
                impuataion_value = imputation_infos[attri_name]['impuataion_value']
                attri_name = dbms_util.get_delimited_col(self.dbms, attri_name)
                if type(impuataion_value) == str:
                    query += f"COALESCE({attri_name}, \'{impuataion_value}\') AS {attri_name},"
                else:
                    query += f"COALESCE({attri_name}, {impuataion_value}) AS {attri_name},"

        # loop over the remaining features and insert them in the select clause
        for f in not_imputation_atrributes:
            f = dbms_util.get_delimited_col(self.dbms, f)
            query += "{},".format(f)
        
        for attri_name in imputation_infos:
            if 'is_push' in imputation_infos[attri_name] and imputation_infos[attri_name]['is_push']: 
                attri_name = dbms_util.get_delimited_col(self.dbms, attri_name)
                query += "{},".format(attri_name)
        query = query[:-1]  # remove the last ','

        query += " FROM {}".format(table_name)

        return None, query