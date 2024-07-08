from collections import Counter
from model_transformer.utility.loader import load_dataset
from category_encoders import TargetEncoder, LeaveOneOutEncoder, BinaryEncoder
import duckdb
import psycopg2
from psycopg2 import OperationalError

# created temp tables
temp_tables = []
# join transforme col map
join_trans_col_map = {}

def join_transformer(table_name, features, optimizations, preprocessors, pipeline):
    table_sql = table_name
    preprocess_features = features
    for preprocessor in preprocessors:
        #------------------------------join the frequency encoder -------------------------------------------#
        if preprocessor == 'FrequencyEncoder':
            if preprocessors[preprocessor]['method'] == 'join':
                attrs = preprocessors[preprocessor]['attrs']
                train_data_path = preprocessors[preprocessor]['train_data_path']
                dbms = preprocessors[preprocessor]['dbms']
                train_data = load_dataset(train_data_path)
                for attr in attrs:
                    # generate the count map
                    data_list = train_data[attr]
                    count = Counter(data_list)
                    # insert the count map to the corresponding database as a temproal table
                    join_table_name = attr + '_count'
                    cols = {attr:'VARCHAR'}
                    cols['count'] = df_type2db_type('int', dbms)
                    data = [(k, v) for k,v in count.items()]
                    insert_db(dbms, join_table_name, cols, data)
                    # change the table name with the join sql statements and update the features list
                    table_sql = f'{table_sql} left join {join_table_name} on {table_name}.{attr}={join_table_name}.{attr}'
                    # add the join column transform
                    add_join_trans_col(attr, 'count', join_table_name)
        
        #------------------------------join the target encoder -------------------------------------------#
        if preprocessor == 'TargetEncoder':
            if preprocessors[preprocessor]['method'] == 'join':
                attrs = preprocessors[preprocessor]['attrs']
                train_data_path = preprocessors[preprocessor]['train_data_path']
                dbms = preprocessors[preprocessor]['dbms']
                target = preprocessors[preprocessor]['target']
                train_data = load_dataset(train_data_path)
                te = TargetEncoder(cols=list(attrs.keys()))
                te.fit(train_data[list(attrs.keys())], train_data[target])
                for attr in attrs:
                    # extract the targetencoder info
                    attr_unique = train_data[attr].unique()
                    te_mapping = te.mapping[attr]
                    oe = te.ordinal_encoder
                    for m in oe.mapping:
                        if m['col'] == attr:
                            oe_mapping = m['mapping']
                            break
                    # insert the count map to the corresponding database as a temproal table
                    join_table_name = attr + '_target'
                    cols = {attr:'VARCHAR'}
                    cols['enc_value'] = df_type2db_type(str(te_mapping.dtype), dbms)
                    data = [(k, te_mapping[oe_mapping[k]]) for k in attr_unique]
                    insert_db(dbms, join_table_name, cols, data)
                    # change the table name with the join sql statements and update the features list
                    table_sql = f'{table_sql} left join {join_table_name} on {table_name}.{attr}={join_table_name}.{attr}'
                    # add the join column transform
                    add_join_trans_col(attr, 'enc_value', join_table_name)
        
        #------------------------------join the LeaveOneOut encoder -------------------------------------------#
        if preprocessor == 'LeaveOneOutEncoder':
            if preprocessors[preprocessor]['method'] == 'join':
                attrs = preprocessors[preprocessor]['attrs']
                train_data_path = preprocessors[preprocessor]['train_data_path']
                dbms = preprocessors[preprocessor]['dbms']
                target = preprocessors[preprocessor]['target']
                train_data = load_dataset(train_data_path)
                looe = LeaveOneOutEncoder(cols=list(attrs.keys()))
                looe.fit(train_data[list(attrs.keys())], train_data[target])
                for attr in attrs:
                    # extract the targetencoder info
                    attr_unique = train_data[attr].unique()
                    looe_mapping = looe.mapping[attr]
                    # insert the count map to the corresponding database as a temproal table
                    join_table_name = attr + '_leave_one_out'
                    cols = {attr:'VARCHAR'}
                    cols['enc_value'] = df_type2db_type(str(looe_mapping['sum'].dtype), dbms)
                    data = [(k, looe_mapping.at[k, 'sum']/looe_mapping.at[k, 'count']) for k in attr_unique]
                    insert_db(dbms, join_table_name, cols, data)
                    # change the table name with the join sql statements and update the features list
                    table_sql = f'{table_sql} left join {join_table_name} on {table_name}.{attr}={join_table_name}.{attr}'
                    # add the join column transform
                    add_join_trans_col(attr, 'enc_value', join_table_name)
        
        #------------------------------join the Binary encoder -------------------------------------------#
        if preprocessor == 'BinaryEncoder':
            if preprocessors[preprocessor]['method'] == 'join':
                attrs = preprocessors[preprocessor]['attrs']
                train_data_path = preprocessors[preprocessor]['train_data_path']
                dbms = preprocessors[preprocessor]['dbms']
                train_data = load_dataset(train_data_path)
                binary_enc = BinaryEncoder(cols=attrs)
                binary_enc.fit(train_data[attrs])
                
                for attr in attrs:
                    for m in binary_enc.ordinal_encoder.category_mapping:
                        if m['col'] == attr:
                            order_mapping = m['mapping']
                            break

                    for m in binary_enc.mapping:
                        if m['col'] == attr:
                            binary_mapping = m['mapping']
                            break
                    
                    cols = {attr:'VARCHAR'}
                    for col in binary_mapping.columns:
                        cols[col] = 'smallint'
                    
                    data = []
                    for value, order in order_mapping.items():
                        data.append((value,) + tuple(binary_mapping.loc[order]))
                    
                    join_table_name = attr + '_binary'
                    insert_db(dbms, join_table_name, cols, data)
                    table_sql = f'{table_sql} left join {join_table_name} on {table_name}.{attr}={join_table_name}.{attr}'
                    index_attr = preprocess_features.index(attr)
                    preprocess_features[index_attr:index_attr+1] = list(binary_mapping.columns)


    for preprocessor in optimizations:
        #------------------------------join the Onehot encoder -------------------------------------------#
        if preprocessor == 'OneHotEncoder':
            if optimizations[preprocessor].get('join_attris'):
                attrs = optimizations[preprocessor]['join_attris']
                dbms = optimizations[preprocessor]['dbms']
                
                for transform in pipeline['transforms']:
                    if transform['transform_name'] == preprocessor:
                        one_enc = transform['fitted_transform']
                        one_features = transform['transform_features']
                        break

                features_after_one = one_enc.get_feature_names()
                count_map = {}
                one_map = {}
                for feature_after_one in features_after_one:
                    # the categorical features after the Sklearn OHE follow the format x<column_id>_<column_val> (e.g., x1_a)
                    feature_item = feature_after_one.split("_")
                    feature = one_features[int(feature_item[0].replace('x', ""))]
                    value = feature_item[1]
                    if feature in count_map:
                        count_map[feature] += 1
                    else:
                        count_map[feature] = 0
                    one_map[feature_after_one] = {"feature_before_one": feature, "value": value,
                                          "alias": f'{feature}_{count_map[feature]}'}
                for attr in attrs:
                    # extract the info
                    join_table_name = attr + '_one'
                    attr_map = {}
                    cols = {attr:'VARCHAR'}
                    data = []
                    for key, value in one_map.items():
                        if value['feature_before_one'] == attr:
                            attr_map[int(value['alias'].split('_')[-1])] = value['value']
                    for i in range(len(attr_map)):
                        cols[attr + f'_{i}'] = 'smallint'
                        data.append((attr_map[i],) + tuple(1 if j==i else 0 for j in range(len(attr_map))))
                    insert_db(dbms, join_table_name, cols, data)
                    # change the table name with the join sql statements and update the features list
                    table_sql = f'{table_sql} left join {join_table_name} on {table_name}.{attr}={join_table_name}.{attr}'
                    # change the one col to muilti cols
                    index_attr = preprocess_features.index(attr)
                    preprocess_features[index_attr:index_attr+1] = [f'{attr}_{i}' for i in range(count_map[attr] + 1)]
            
        #------------------------------join the Ordinal encoder -------------------------------------------#
        if preprocessor == 'OrdinalEncoder':
            if optimizations[preprocessor].get('join_attris'):
                attrs = optimizations[preprocessor]['join_attris']
                dbms = optimizations[preprocessor]['dbms']
                
                for transform in pipeline['transforms']:
                    if transform['transform_name'] == preprocessor:
                        ordinal_enc = transform['fitted_transform']
                        ordinal_features = transform['transform_features']
                        break
                
                categories = ordinal_enc.categories_
                for i, attr in enumerate(ordinal_features):
                    classes = categories[i]
                    join_table_name = attr + '_ordinal'
                    cols = {attr:'VARCHAR'}
                    cols['enc_value'] = 'int'
                    data = [(cla, j) for j, cla in enumerate(classes)]
                    insert_db(dbms, join_table_name, cols, data)
                    table_sql = f'{table_sql} left join {join_table_name} on {table_name}.{attr}={join_table_name}.{attr}'
                    add_join_trans_col(attr, 'enc_value', join_table_name)
                    
                    


    return table_sql, preprocess_features


def insert_db(db, table_name, cols, data):
    if db == 'duckdb':
        duckdb_file_path = '/root/volume/duckdb/mydb'
        conn = duckdb.connect(database=duckdb_file_path, read_only=False)
        # create table
        cols_sql = ','.join([f'{col} {cols[col]}' for col in cols])
        create_sql = f'DROP TABLE IF EXISTS {table_name}; CREATE TABLE {table_name} ({cols_sql});'
        conn.execute(create_sql)
        temp_tables.append(table_name)
        # insert data
        slot_sql = ','.join(['?'] * len(data[0]))
        insert_sql = f'INSERT INTO {table_name} VALUES ({slot_sql});'
        conn.executemany(insert_sql, data)
        conn.commit()
        conn.close()
    
    if db == 'pg':
        # database parameters
        dbname = 'postgres'
        user = 'postgres'
        host = 'localhost'  
        port = 5432  

        # connect to pg
        try:
            connection = psycopg2.connect(
                dbname=dbname,
                user=user,
                host=host,
                port=port
            )
            cursor = connection.cursor()
        except OperationalError as e:
            print(f"The error '{e}' occurred")

        # create table
        cols_sql = ','.join([f'{col} {cols[col]}' for col in cols])
        create_sql = f'DROP TABLE IF EXISTS {table_name}; CREATE TABLE {table_name} ({cols_sql});'
        try:
            cursor.execute(create_sql)
            connection.commit()
        except OperationalError as e:
            print(f"The error '{e}' occurred")

        # insert data
        slot_sql = ','.join(['%s'] * len(data[0]))
        insert_sql = f'INSERT INTO {table_name} VALUES ({slot_sql});'
        cursor.executemany(insert_sql, data)
        connection.commit()

        # close connect
        cursor.close()
        connection.close()



def df_type2db_type(df_dtype, db):
    db_type = ''

    if db in ('duckdb', 'pg'):
        if df_dtype == 'object':
            db_type = 'VARCHAR'

        if 'int' in df_dtype:
            db_type = 'INT'
        
        if 'float' in df_dtype:
            db_type = 'FLOAT'

        if 'smallint' in df_dtype:
            db_type = 'SMALLINT'

        if 'boolean' in df_dtype:
            db_type = 'BOOLEAN'

    return db_type



def del_temp_join_tables(pre_sql):
    if not temp_tables:
        return pre_sql
    else:
        delete_sql = ';'
        for temp_table in temp_tables:
            delete_sql += f'DROP TABLE {temp_table};'
        
        return pre_sql + delete_sql



def add_join_trans_col(origin_col, encoder_col, table_name):
    join_trans_col_map[origin_col] = (encoder_col, table_name)



def get_join_trans_col_map():
    return join_trans_col_map
