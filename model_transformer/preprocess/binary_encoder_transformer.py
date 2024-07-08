from model_transformer.utility.dbms_utils import DBMSUtils
import pandas as pd
import category_encoders as ce
from model_transformer.utility.loader import load_dataset

class BinaryEncoderSQL(object):
    def __init__(self):
        self.dbms = None
        self.params = None
    
    
    def set_dbms(self, dbms: str):
        self.dbms = dbms

    def transform_model_features_in(self, transform, all_features):
        out_all_features = all_features
        binaryencoder_infos = transform['transform_features']
        binary_atrributes = binaryencoder_infos['attrs']
        train_data_path = binaryencoder_infos['train_data_path']
        train_data = load_dataset(train_data_path)
        binary_enc = ce.BinaryEncoder(cols=binary_atrributes)
        binary_enc.fit(train_data[binary_atrributes])

        for attr in binary_atrributes:
            for m in binary_enc.mapping:
                if m['col'] == attr:
                    binary_mapping = m['mapping']
                    break
            
            index_attr = out_all_features.index(attr)
            out_all_features[index_attr:index_attr+1] = list(binary_mapping.columns)

        return out_all_features


    def get_params(self, fitted_transformer, binaryencoder_infos, all_features, preprocess_all_features, prev_transform_features):
        binary_atrributes = binaryencoder_infos['attrs']
        not_binary_atrributes = []
        out_all_features = []
        preprocess_out_all_features = []
        train_data_path = binaryencoder_infos['train_data_path']
        train_data = pd.read_csv(train_data_path, nrows=10000)
        binary_encoder = ce.BinaryEncoder(cols=binary_atrributes)
        train_data_binary = binary_encoder.fit_transform(train_data[binary_atrributes])
        train_data_join = pd.concat([train_data[binary_atrributes], train_data_binary], axis=1)

        for attr_name in preprocess_all_features:
            if attr_name not in binary_atrributes:
                not_binary_atrributes.append(attr_name)

        for attr_name in all_features:
            if attr_name not in binary_atrributes:
                out_all_features.append(attr_name)
            else:
                for col_name in train_data_binary.columns:
                    if attr_name in col_name:
                        out_all_features.append(col_name)   

        for attr_name in preprocess_all_features:
            if attr_name not in binary_atrributes:
                preprocess_out_all_features.append(attr_name)
            else:
                if not ('is_push' in binary_atrributes[attr_name] and binary_atrributes[attr_name]['is_push'] or 'is_merge' in binary_atrributes[attr_name] and binary_atrributes[attr_name]['is_merge']):
                    for col_name in train_data_binary.columns:
                        if attr_name in col_name:
                            preprocess_out_all_features.append(col_name)  
                else:
                    preprocess_out_all_features.append(attr_name)

        self.params = {"out_all_features": out_all_features, 'out_transform_features': prev_transform_features,
                       "binaryencoder_infos": binaryencoder_infos, 'not_binary_atrributes': not_binary_atrributes,
                       'train_data_binary': train_data_binary, 'train_data_join': train_data_join, 'preprocess_all_features': preprocess_out_all_features}
        return self.params

    def query(self, table_name):
        dbms_util = DBMSUtils()

        # create the SQL query that implements the binaryencode in SQL
        query = "SELECT "

        # loop over the binary encoder features and insert them in the select clause
        not_binary_atrributes = self.params['not_binary_atrributes']
        binaryencoder_infos = self.params['binaryencoder_infos']
        binary_atrributes = binaryencoder_infos['attrs']
        train_data_binary = self.params['train_data_binary']
        train_data_join = self.params['train_data_join']
        binary_preprocess_sql_list = []
        for attr in binary_atrributes:
            if not ('is_push' in binary_atrributes[attr] and binary_atrributes[attr]['is_push'] or 'is_merge' in binary_atrributes[attr] and binary_atrributes[attr]['is_merge']):
                binaryed_col_list = []
                for col_name in train_data_binary.columns:
                    if attr in col_name:
                        binaryed_col_list.append(col_name)   

                train_data_join_groupby = train_data_join.groupby(attr)[binaryed_col_list].first()
                
                begin_str = f'CASE WHEN "{attr}" in '
                for col_name in train_data_join_groupby.columns:
                    one_index_list = []
                    zero_index_list = []
                    # 遍历行
                    for row in range(train_data_join_groupby.shape[0]):
                        if train_data_join_groupby.iloc[row][col_name]:
                            one_index_list.append(f"'{train_data_join_groupby.index[row]}'")
                        else:
                            zero_index_list.append(f"'{train_data_join_groupby.index[row]}'")


                    in_str = ""
                    if(len(one_index_list) < len(zero_index_list)):
                        in_str += f"({','.join(one_index_list)}) THEN 1 ELSE 0 "
                    else:
                        in_str += f"({','.join(zero_index_list)}) THEN 0 ELSE 1 "

                    end_str = f'END AS "{col_name}"'
                    binary_preprocess_sql_list.append(begin_str + in_str + end_str)

        if len(binary_preprocess_sql_list) > 0:
            query += ','.join(binary_preprocess_sql_list)
            query += ','

        # loop over the remaining features and insert them in the select clause
        for f in not_binary_atrributes:
            f = dbms_util.get_delimited_col(self.dbms, f)
            query += "{},".format(f)

        for attr in binary_atrributes:
            if 'is_push' in binary_atrributes[attr] and binary_atrributes[attr]['is_push'] or 'is_merge' in binary_atrributes[attr] and binary_atrributes[attr]['is_merge']:
                attr = dbms_util.get_delimited_col(self.dbms, attr)
                query += "{},".format(attr)

        query = query[:-1]  # remove the last ','

        query += " FROM {}".format(table_name)

        return None, query
