from model_transformer.utility.dbms_utils import DBMSUtils

class JoinSQL(object):

    def __init__(self):
        self.dbms = None
        self.params = None
    
    def set_dbms(self, dbms: str):
        self.dbms = dbms

    def get_params(self, features, join_trans_col_map):
        join_cols = [col for col in join_trans_col_map]
        not_join_cols = [col for col in features if col not in join_trans_col_map]

        self.params = {'join_cols':join_cols, 'not_join_cols':not_join_cols, 'join_trans_col_map':join_trans_col_map}
        return self.params


    def query(self, table_name):
        dbms_util = DBMSUtils()

        join_cols = self.params['join_cols']
        not_join_cols = self.params['not_join_cols']
        join_trans_col_map = self.params['join_trans_col_map']

        # create the SQL query that implements the Join transformer in SQL
        query = "SELECT "

        # loop over the preprocess features and insert them in the select clause
        for col in join_cols:
            query += f'{join_trans_col_map[col][1]}.{dbms_util.get_delimited_col(self.dbms, join_trans_col_map[col][0])} AS {dbms_util.get_delimited_col(self.dbms, col)},'

        # loop over the other features and insert them in the select clause
        for col in not_join_cols:
            query += f'{dbms_util.get_delimited_col(self.dbms, col)},'
        query = query[:-1]  # remove the last ','

        query += " FROM {}".format(table_name)

        return None, query