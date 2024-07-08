from model_transformer.utility.dbms_utils import DBMSUtils

class DefaultPreprocessSQL(object):

    def __init__(self):
        self.dbms = None
        self.all_features = None
    
    
    def set_dbms(self, dbms: str):
        self.dbms = dbms

    def get_params(self, features):
        self.all_features = features

    def query(self, table_name):
        dbms_util = DBMSUtils()
        # create the SQL query that implements the preprocess in SQL
        query = "SELECT "

        # loop over the all features and insert them in the select clause
        for f in self.all_features:
            f = dbms_util.get_delimited_col(self.dbms, f)
            query += "{},".format(f)
        query = query[:-1]  # remove the last ','

        query += " FROM {}".format(table_name)

        return None, query
