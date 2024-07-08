from model_transformer.utility.dbms_utils import DBMSUtils

class EqualWidthDiscretizationSQL(object):

    def __init__(self):
        self.dbms = None
        self.params = None
    
    def set_dbms(self, dbms: str):
        self.dbms = dbms

    def transform_model_features_in(self, transform, all_features):
        return all_features

    def get_params(self, fitted_transformer, infos, all_features, preprocess_all_features, prev_transform_features):
        not_preprocess_atrributes = []
        for attr_name in preprocess_all_features:
            if attr_name not in infos:
                not_preprocess_atrributes.append(attr_name)
        self.params = {"out_all_features": all_features, 'out_transform_features': prev_transform_features,
                       "infos": infos, 'not_preprocess_atrributes': not_preprocess_atrributes, 'preprocess_all_features':preprocess_all_features}
        return self.params


    def query(self, table_name):
        dbms_util = DBMSUtils()

        not_preprocess_atrributes = self.params['not_preprocess_atrributes']
        infos = self.params['infos']

        # create the SQL query that implements the equal_witdh_descretization in SQL
        query = "SELECT "

        # loop over the equal width descretization features and insert them in the select clause
        for attr_name in infos:
            if not ('is_push' in infos[attr_name] and infos[attr_name]['is_push'] or 'is_merge' in infos[attr_name] and infos[attr_name]['is_merge']):
                bins = infos[attr_name]['bins']
                labels = infos[attr_name]['labels']
                query += "CASE "
                for i in range(len(bins)):
                    query += f"WHEN {dbms_util.get_delimited_col(self.dbms, attr_name)} <= {bins[i]} THEN {labels[i]} "
                query += f"ELSE {labels[-1]} "
                query += f"END AS {dbms_util.get_delimited_col(self.dbms, attr_name)}, "

        # loop over the remaining features and insert them in the select clause
        for f in not_preprocess_atrributes:
            f = dbms_util.get_delimited_col(self.dbms, f)
            query += "{},".format(f)

        for attr_name in infos:
            if 'is_push' in infos[attr_name] and infos[attr_name]['is_push'] or 'is_merge' in infos[attr_name] and infos[attr_name]['is_merge']:
                query += "{},".format(dbms_util.get_delimited_col(self.dbms, attr_name))

        query = query[:-1]  # remove the last ','

        query += " FROM {}".format(table_name)

        return None, query