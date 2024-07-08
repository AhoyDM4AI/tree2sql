from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.preprocessing import StandardScaler, OneHotEncoder, MinMaxScaler, LabelEncoder, OrdinalEncoder
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
from model_transformer.utility.loader import load_model
from model_transformer.preprocess.binary_encoder_transformer import BinaryEncoderSQL
from model_transformer.preprocess.std_scaler_transformer import StandardScalerSQL
from model_transformer.preprocess.one_hot_encoder_transformer import OneHotEncoderSQL
from model_transformer.preprocess.default_preprocess_transformer import DefaultPreprocessSQL
from model_transformer.preprocess.udf_transformer import UDFSQL
from model_transformer.preprocess.imputatation_transformer import ImputationSQL
from model_transformer.preprocess.equal_width_discretization_transformer import EqualWidthDiscretizationSQL
from model_transformer.preprocess.minmax_scaler_transformer import MinMaxScalerSQL
from model_transformer.preprocess.label_encoder_transformer import LabelEncoderSQL
from model_transformer.preprocess.ordinal_encoder_transformer import OrdinalEncoderSQL
from model_transformer.preprocess.freqency_encoder_transformer import FrequencyEncoderSQL
from model_transformer.preprocess.join_transformer import JoinSQL
from model_transformer.preprocess.target_encoder_transformer import TargetEncoderSQL
from model_transformer.preprocess.leave_one_out_encoder_transformer import LeaveOneOutEncoderSQL
from model_transformer.model.decision_tree_transformer import DTMSQL
from model_transformer.model.random_forest_transformer import RFMSQL
from model_transformer.utility.join_utils import join_transformer, del_temp_join_tables, get_join_trans_col_map
from model_transformer.auto_select.auto_config import auto_config
import numpy as np


class TransformerManager(object):
    model_types = {
        'DecisionTreeClassifier': DecisionTreeClassifier(max_depth=3, min_samples_leaf=10,
                                                         random_state=24),
        'DecisionTreeRegressor': DecisionTreeRegressor(max_depth=3, random_state=42),
        'RandomForestClassifier': RandomForestClassifier(max_depth=10, n_estimators=4, random_state=24),
        'RandomForestRegressor': RandomForestRegressor(max_depth=3, max_leaf_nodes=20, n_estimators=100,
                                                       min_samples_leaf=10, random_state=24),

    }

    transform_types = {
        'StandardScaler': StandardScaler(with_mean=False),
        'OneHotEncoder': OneHotEncoder(handle_unknown='ignore'),
        'MinMaxScaler': MinMaxScaler(),
        'LabelEncoder': LabelEncoder(),
        'OrdinalEncoder': OrdinalEncoder()
    }

    sql_model_types = {
        'DecisionTreeClassifier': DTMSQL(classification=True),
        'DecisionTreeRegressor': DTMSQL(),
        'RandomForestClassifier': RFMSQL(classification=True),
        'RandomForestRegressor': RFMSQL(),
    }

    sql_transform_types = {
        'StandardScaler': StandardScalerSQL(),
        'OneHotEncoder': OneHotEncoderSQL(),
        'DefaultPreprocess': DefaultPreprocessSQL(),
        'UDF': UDFSQL(),
        'Imputation': ImputationSQL(),
        'BinaryEncoder': BinaryEncoderSQL(),
        'EqualWidthDiscretization': EqualWidthDiscretizationSQL(),
        'MinMaxScaler': MinMaxScalerSQL(),
        'LabelEncoder': LabelEncoderSQL(),
        'OrdinalEncoder': OrdinalEncoderSQL(),
        'FrequencyEncoder': FrequencyEncoderSQL(),
        'TargetEncoder': TargetEncoderSQL(),
        'LeaveOneOutEncoder': LeaveOneOutEncoderSQL()
    }


    model_name = None
    transforms = []

    @staticmethod
    def extract_pipeline(pipeline, preprocessors=None):
        model_name, trained_model = pipeline.steps[1]
        transforms = []

        if preprocessors is not None:
            for key in preprocessors: 
                transforms.append({
                    'transform_name': key,
                    'transform_features': preprocessors[key],
                })

        for idx in range(len(pipeline.steps[0][1].transformers)):
            a, b, c = pipeline.steps[0][1].transformers_[idx]
            transforms.append({
                'transform_name': a,
                'fitted_transform': b,
                'transform_features': c,
            })

        return {
            'model': {
                'model_name': model_name,
                'trained_model': trained_model
            },
            'transforms': transforms
        }

    def get_model_features_in(self, features, pipeline):
        model_features_in = features.copy()
        transforms = pipeline['transforms']
        for transform in transforms:
            transform_name = transform['transform_name']
            sql_transformer = self.sql_transform_types[transform_name]
            model_features_in = sql_transformer.transform_model_features_in(transform, model_features_in)
        return model_features_in


    def generate_query(self, model_file, table_name, features, dbms, pre_sql=None, optimizations=None, preprocessors=None, auto_gen=False, test_data_path=None, encoders_path=None):
        """
        Generate a query statement based on provided model file, table name, features, and DBMS.

        This function constructs a SQL query statement using the input model data, table name,
        features, and database management system information.
        
        Parameters:
        model_file : dict
            A dictionary containing model-related data.
        table_name : str
            The name of the target SQL table.
        features : list
            model features.
        dbms : str
            The type of the target database management system, such as 'pg', 'duckdb', 'clickhouse', etc.
        pre_sql : str
            head SQL statement to be executed before the query.
        optimizations : list, optional
            A list of SKL-pre-op config, default is None.
        preprocessors : list, optional
            A list of Non-SKL-pre-op config, default is None.
        auto_gen : bool, optional
            A flag indicating whether to automatically generate the query statement, default is False.

        Returns: str
            The generated query statement.
        """

        # Ensure that parameters is provided when auto_gen is True
        assert not auto_gen or test_data_path is not None, "sample_dataset must be provided when auto_gen is True"
        assert not auto_gen or encoders_path is not None, "sample_dataset must be provided when auto_gen is True"

        # some load and extract tasks
        model = load_model(model_file)
        pipeline = self.extract_pipeline(model, preprocessors)
        input_table = table_name

        # transform the input features to the model_features_in_
        model_features_in = self.get_model_features_in(features, pipeline)

        # automatically genarate the SQL genarating configurations according to the cost model of "craftsman"
        if auto_gen:
            optimizations, preprocessors = auto_config(pipeline, test_data_path, encoders_path, model_features_in, features, optimizations, preprocessors)

        # add the join opreations
        input_table, preprocess_features = join_transformer(input_table, features, optimizations, preprocessors, pipeline)

        # initialize the SOL Transformers
        opt = Optimizer(pipeline, features, preprocess_features, dbms, optimizations)
        pipeline = opt.optimize()

        # create an SQL query for each transformer
        sql_transformers = pipeline["transforms"]
        preliminary_queries = []
        for sql_transformer in sql_transformers:
            transf_pre_queries, transformation_query = sql_transformer.query(input_table)
            if transf_pre_queries is not None:
                preliminary_queries += transf_pre_queries

            # the input table for the possible next transformer is the output of the current transformer
            input_table = "({}) AS data".format(transformation_query)

        # create the SQL query that performs the model inference
        model = pipeline['model']
        fitted_model = model["trained_model"]
        model_sql_wrapper = model['model_sql_wrapper']
        assert 'model_features' in model
        if model['model_features'] is not None:
            features = model['model_features']

        model_pre_queries, query = model_sql_wrapper.query(fitted_model, features, input_table)

        queries = preliminary_queries.copy()
        out_query = ''

        # add the pre sql
        out_query = pre_sql + out_query

        for pre_q in preliminary_queries:
            out_query += f'{pre_q}\n'
        if model_pre_queries is not None:
            out_query += '\n'.join(model_pre_queries)
            queries += model_pre_queries
            
        out_query += query
        queries.append(query)

        # delete sql for temp tables
        out_query = del_temp_join_tables(out_query)

        return queries, out_query


class Optimizer(object):
    def __init__(self, pipeline: dict, features: list, preprocess_features: list, dbms: str, optimizations):
        assert isinstance(pipeline, dict)
        assert 'model' in pipeline
        assert 'transforms' in pipeline
        assert 'model_name' in pipeline['model']
        assert 'trained_model' in pipeline['model']
        assert isinstance(pipeline['transforms'], list)
        assert isinstance(features, list)
        assert len(features) > 0
        for f in features:
            assert isinstance(f, str)
        assert isinstance(dbms, str)

        self.pipeline = pipeline
        self.features = features
        self.preprocess_features = preprocess_features

        self.transformers = self.pipeline["transforms"]
        self.model = self.pipeline['model']
        self.transformer_names = [transformer["transform_name"] for transformer in self.transformers]
        self.model_name = self.model["model_name"]
        self.tree_based_model_keys = ['Gradient', 'Tree', 'Forest']

        self.ml_manager = TransformerManager()
        
        self.one_gen_preprocess = False
        self.one_optimization = False

        self.standard_gen_preprocess = False
        self.standard_optimization = False

        self.minmax_gen_preprocess = False
        self.minmax_optimization = False

        self.ordinal_gen_preprocess = False
        self.ordinal_optimization = False
                
        self.udf_need_gen_preprocess = True
        self.udf_need_merge = False

        self.equal_need_gen_preprocess = True
        self.equal_need_merge = False

        self.imputation_need_gen_preprocess = True
        self.imputation_need_merge = False

        self.binary_need_gen_preprocess = True
        self.binary_need_merge = False

        self.frequency_need_gen_preprocess = True
        self.frequency_need_merge = False

        self.target_need_gen_preprocess = True
        self.target_need_merge = False

        self.leave_need_gen_preprocess = True
        self.leave_need_merge = False

        self.optimizations = optimizations
        self.dbms = dbms


    def optimize(self):

        out_pipeline = self.pipeline.copy()
        optimizations = self.optimizations
        features = self.features
        transformers_to_merge = []
        transformers_to_join = []

        if 'OneHotEncoder' in optimizations:
            if optimizations['OneHotEncoder'].get('join_attris'):
                transformers_to_join.append('OneHotEncoder')
            else:
                if optimizations['OneHotEncoder'].get('push_attris') or optimizations['OneHotEncoder'].get('merge_attris'):
                    self.one_optimization = True
                if optimizations['OneHotEncoder'].get('other_attris'):
                    self.one_gen_preprocess = True
            

        if 'StandardScaler' in optimizations:
            if optimizations['StandardScaler'].get('push_attris') or optimizations['StandardScaler'].get('merge_attris'):
                self.standard_optimization = True
            if optimizations['StandardScaler'].get('other_attris'):
                self.standard_gen_preprocess = True
            

        if 'MinMaxScaler' in optimizations:
            if optimizations['MinMaxScaler'].get('push_attris') or optimizations['MinMaxScaler'].get('merge_attris'):
                self.minmax_optimization = True
            if optimizations['MinMaxScaler'].get('other_attris'):
                self.minmax_gen_preprocess = True
                
        
        if 'OrdinalEncoder' in optimizations:
            if optimizations['OrdinalEncoder'].get('join_attris'):
                transformers_to_join.append('OrdinalEncoder')
            else:
                if optimizations['OrdinalEncoder'].get('push_attris') or optimizations['OrdinalEncoder'].get('merge_attris'):
                    self.ordinal_optimization = True
                if optimizations['OrdinalEncoder'].get('other_attris'):
                    self.ordinal_gen_preprocess = True

        
        for transform in out_pipeline['transforms']:
            if transform['transform_name'] == 'UDF':
                transform_features = transform['transform_features']
                merge_num = 0
                for attr_name in transform_features:
                    if transform_features[attr_name].get('is_push') and transform_features[attr_name]['is_push']:
                        merge_num += 1
                if merge_num == len(transform_features):
                    self.udf_need_gen_preprocess = False
                    self.udf_need_merge = True
                elif merge_num < len(transform_features) and merge_num > 0:
                    self.udf_need_merge = True
            
            elif transform['transform_name'] == 'EqualWidthDiscretization':
                transform_features = transform['transform_features']
                merge_num = 0
                for attr_name in transform_features:
                    if (transform_features[attr_name].get('is_push') and transform_features[attr_name]['is_push'])\
                        or (transform_features[attr_name].get('is_merge') and transform_features[attr_name]['is_merge']):
                        merge_num += 1
                if merge_num == len(transform_features):
                    self.equal_need_gen_preprocess = False
                    self.equal_need_merge = True
                elif merge_num < len(transform_features) and merge_num > 0:
                    self.equal_need_merge = True
            
            elif transform['transform_name'] == 'Imputation':
                transform_features = transform['transform_features']
                merge_num = 0
                for attr_name in transform_features:
                    if transform_features[attr_name].get('is_push') and transform_features[attr_name]['is_push']:
                        merge_num += 1
                if merge_num == len(transform_features):
                    self.imputation_need_gen_preprocess = False
                    self.imputation_need_merge = True
                elif merge_num < len(transform_features) and merge_num > 0:
                    self.imputation_need_merge = True

            elif transform['transform_name'] == 'BinaryEncoder':
                transform_features = transform['transform_features']
                # if use join, then dont need the preprocess step and the push-up
                if transform_features.get('method') == 'join':
                    transformers_to_join.append(transform['transform_name'])
                    self.binary_need_gen_preprocess = False
                else:
                    # dont join
                    merge_num = 0
                    transform_features = transform_features['attrs']
                    for attr_name in transform_features:
                        if (transform_features[attr_name].get('is_push') and transform_features[attr_name]['is_push'])\
                            or (transform_features[attr_name].get('is_merge') and transform_features[attr_name]['is_merge']):
                            merge_num += 1
                    if merge_num == len(transform_features):
                        self.binary_need_gen_preprocess = False
                        self.binary_need_merge = True
                    elif merge_num < len(transform_features) and merge_num > 0:
                        self.binary_need_merge = True
        
            elif transform['transform_name'] == 'FrequencyEncoder':
                transform_features = transform['transform_features']
                # if use join, then dont need the preprocess step and the push-up
                if transform_features.get('method') == 'join':
                    transformers_to_join.append(transform['transform_name'])
                    self.frequency_need_gen_preprocess = False
                else:
                    # dont join
                    merge_num = 0
                    transform_features = transform_features['attrs']
                    for attr_name in transform_features:
                        if (transform_features[attr_name].get('is_push') and transform_features[attr_name]['is_push'])\
                            or (transform_features[attr_name].get('is_merge') and transform_features[attr_name]['is_merge']):
                            merge_num += 1
                    if merge_num == len(transform_features):
                        self.frequency_need_gen_preprocess = False
                        self.frequency_need_merge = True
                    elif merge_num < len(transform_features) and merge_num > 0:
                        self.frequency_need_merge = True

            elif transform['transform_name'] == 'TargetEncoder':
                transform_features = transform['transform_features']
                # if use join, then dont need the preprocess step and the push-up
                if transform_features.get('method') == 'join':
                    transformers_to_join.append(transform['transform_name'])
                    self.target_need_gen_preprocess = False
                else:
                    # dont join
                    merge_num = 0
                    transform_features = transform_features['attrs']
                    for attr_name in transform_features:
                        if (transform_features[attr_name].get('is_push') and transform_features[attr_name]['is_push'])\
                            or (transform_features[attr_name].get('is_merge') and transform_features[attr_name]['is_merge']):
                            merge_num += 1
                    if merge_num == len(transform_features):
                        self.target_need_gen_preprocess = False
                        self.target_need_merge = True
                    elif merge_num < len(transform_features) and merge_num > 0:
                        self.target_need_merge = True

            elif transform['transform_name'] == 'LeaveOneOutEncoder':
                transform_features = transform['transform_features']
                # if use join, then dont need the preprocess step and the push-up
                if transform_features.get('method') == 'join':
                    transformers_to_join.append(transform['transform_name'])
                    self.leave_need_gen_preprocess = False
                else:
                    # dont join
                    merge_num = 0
                    transform_features = transform_features['attrs']
                    for attr_name in transform_features:
                        if (transform_features[attr_name].get('is_push') and transform_features[attr_name]['is_push'])\
                            or (transform_features[attr_name].get('is_merge') and transform_features[attr_name]['is_merge']):
                            merge_num += 1
                    if merge_num == len(transform_features):
                        self.leave_need_gen_preprocess = False
                        self.leave_need_merge = True
                    elif merge_num < len(transform_features) and merge_num > 0:
                        self.leave_need_merge = True

        # if the optimization is enabled then check if the operator fusion can be applied
        if self.one_optimization:
            # if the pipeline includes an OneHotEncoder and a tree-based model then apply the one-hot encoding directly
            # in the decision tree rules
            if 'OneHotEncoder' in self.transformer_names and \
                    any(key in self.model_name for key in self.tree_based_model_keys):
                transformers_to_merge.append('OneHotEncoder')
        
        if self.standard_optimization:
            if 'StandardScaler' in self.transformer_names and \
                    any(key in self.model_name for key in self.tree_based_model_keys):
                transformers_to_merge.append('StandardScaler')
        
        if self.minmax_optimization:
            if 'MinMaxScaler' in self.transformer_names and \
                    any(key in self.model_name for key in self.tree_based_model_keys):
                transformers_to_merge.append('MinMaxScaler')

        if self.ordinal_optimization:
            if 'OrdinalEncoder' in self.transformer_names and \
                    any(key in self.model_name for key in self.tree_based_model_keys):
                transformers_to_merge.append('OrdinalEncoder')

        if self.udf_need_merge:
            if 'UDF' in self.transformer_names and \
                    any(key in self.model_name for key in self.tree_based_model_keys):
                transformers_to_merge.append('UDF')

        if self.equal_need_merge:
            if 'EqualWidthDiscretization' in self.transformer_names and \
                    any(key in self.model_name for key in self.tree_based_model_keys):
                transformers_to_merge.append('EqualWidthDiscretization')

        if self.imputation_need_merge:
            if 'Imputation' in self.transformer_names and \
                    any(key in self.model_name for key in self.tree_based_model_keys):
                transformers_to_merge.append('Imputation')

        if self.binary_need_merge:
            if 'BinaryEncoder' in self.transformer_names and \
                    any(key in self.model_name for key in self.tree_based_model_keys):
                transformers_to_merge.append('BinaryEncoder')

        if self.frequency_need_merge:
            if 'FrequencyEncoder' in self.transformer_names and \
                    any(key in self.model_name for key in self.tree_based_model_keys):
                transformers_to_merge.append('FrequencyEncoder')

        if self.target_need_merge:
            if 'TargetEncoder' in self.transformer_names and \
                    any(key in self.model_name for key in self.tree_based_model_keys):
                transformers_to_merge.append('TargetEncoder')

        if self.leave_need_merge:
            if 'LeaveOneOutEncoder' in self.transformer_names and \
                    any(key in self.model_name for key in self.tree_based_model_keys):
                transformers_to_merge.append('LeaveOneOutEncoder')
                
        
        # get the fitted transformers from the pipeline
        prev_transform_features = []
        new_transformers = []
        sql_transformers_to_merge = []
        preprocess_all_features = self.preprocess_features
        join_transformers_parms = {}

        # if some encoder is using join, then add the join_transformer layer
        join_trans_col_map = get_join_trans_col_map()
        if join_trans_col_map:
            join_transformer = JoinSQL()
            join_transformer.set_dbms(self.dbms)
            join_transformer.get_params(features, join_trans_col_map)
            new_transformers.append(join_transformer)

        # resolve the preprocess transformers
        for transformer in self.transformers:
            transformer_name = transformer["transform_name"]
            if "fitted_transform" in transformer:
                fitted_transformer = transformer["fitted_transform"]
            else:
                fitted_transformer = None
            transformer_features = transformer["transform_features"]

            # retrieve the SQL wrapper related to the current transformer and extract its fitted params
            transformer_sql_wrapper = self.ml_manager.sql_transform_types[transformer_name]
            # pass the optimizations parameters to the preprocess transformer
            if transformer_name in self.optimizations:
                transformer_sql_wrapper.set_optimizations(self.optimizations)
            transformer_params = transformer_sql_wrapper.get_params(fitted_transformer, transformer_features,
                                                                    features, preprocess_all_features, prev_transform_features)
            features = transformer_params["out_all_features"]
            preprocess_all_features = transformer_params['preprocess_all_features']
            # transformers that have to be merged with the model are ignored in this phase
            if transformer_name not in transformers_to_join and transformer_name not in transformers_to_merge or (transformer_name == 'UDF' and self.udf_need_gen_preprocess)\
                or (transformer_name == 'StandardScaler' and self.standard_gen_preprocess)\
                    or (transformer_name == 'MinMaxScaler' and self.minmax_gen_preprocess)\
                        or (transformer_name == 'OneHotEncoder' and self.one_gen_preprocess)\
                            or (transformer_name == 'OrdinalEncoder' and self.ordinal_gen_preprocess)\
                                or (transformer_name == 'EqualWidthDiscretization' and self.equal_need_gen_preprocess)\
                                    or (transformer_name == 'Imputation' and self.imputation_need_gen_preprocess)\
                                        or (transformer_name == 'BinaryEncoder' and self.binary_need_gen_preprocess)\
                                            or (transformer_name == 'FrequencyEncoder' and self.frequency_need_gen_preprocess)\
                                                or (transformer_name == 'TargetEncoder' and self.target_need_gen_preprocess)\
                                                    or (transformer_name == 'LeaveOneOutEncoder' and self.leave_need_gen_preprocess):
                transformer_sql_wrapper.set_dbms(self.dbms)
                new_transformers.append(transformer_sql_wrapper)

            if transformer_name in transformers_to_join:
                join_transformers_parms[transformer_name] = transformer_params

            if transformer_name in transformers_to_merge:
                sql_transformers_to_merge.append((transformer_name, transformer_sql_wrapper, transformer_params))
            

            prev_transform_features = prev_transform_features + transformer_params["out_transform_features"]
        # if all the preprocess is fusion to the tree, then use the deafaultpreprecess to select all needed attributes
        if not new_transformers:
            transformer_sql_wrapper = self.ml_manager.sql_transform_types['DefaultPreprocess']
            transformer_sql_wrapper.get_params(self.features)
            transformer_sql_wrapper.set_dbms(self.dbms)
            new_transformers.append(transformer_sql_wrapper)


        # get the fitted model from the pipeline and retrieve its SQL wrapper
        fitted_model = self.model["trained_model"]
        model_sql_wrapper = self.ml_manager.sql_model_types[self.model_name]
        model_sql_wrapper.set_dbms(self.dbms)
        # pass the optimizations parameters to sql tree model
        if self.model_name in ['RandomForestClassifier','RandomForestRegressor',
                               'DecisionTreeClassifier','DecisionTreeRegressor']:
            model_sql_wrapper.set_optimizations(self.optimizations)


        # if dbms limits are reached then set the sparse implementation for some transformers and for the model
        # in the opposite case enable dense modality for all the pipeline components
        for nt in new_transformers:
            if 'OneHotEncoder' in str(type(nt)):
                    nt.set_mode('dense')
        model_sql_wrapper.set_mode('dense')


        # if there are operators to merge and the sparse implementation is disabled then merge the previously
        # selected transformers with the model
        if len(sql_transformers_to_merge) > 0:
            for sql_transf_to_merge in sql_transformers_to_merge:
                transf_name = sql_transf_to_merge[0]
                transf_sql = sql_transf_to_merge[1]
                transf_params = sql_transf_to_merge[2]

                if transf_name == 'OneHotEncoder':
                    print("OneHot Operator fusion enabled.")
                    model_sql_wrapper.merge_ohe_with_trees(transf_params['ohe_encoding'])
                
                if transf_name == 'StandardScaler':
                    print("StandardScaler Operator fusion enabled.")
                    model_sql_wrapper.merge_standard_with_trees(transf_params)
                
                if transf_name == 'UDF':
                    print("UDF Operator fusion enabled.")
                    model_sql_wrapper.merge_udf_with_trees(transf_params)
                
                if transf_name == 'MinMaxScaler':
                    print("MinMaxScaler Operator fusion enabled.")
                    model_sql_wrapper.merge_minmax_with_trees(transf_params)

                if transf_name == 'OrdinalEncoder':
                    print("OrdinalEncoder Operator fusion enabled.")
                    model_sql_wrapper.merge_ordinal_with_trees(transf_params)
                
                if transf_name == 'EqualWidthDiscretization':
                    print("EqualWidthDiscretization Operator fusion enabled.")
                    model_sql_wrapper.merge_equal_with_trees(transf_params)
                
                if transf_name == 'Imputation':
                    print("Imputation Operator fusion enabled.")
                    model_sql_wrapper.merge_imputation_with_trees(transf_params)

                if transf_name == 'BinaryEncoder':
                    print("BinaryEncoder Operator fusion enabled.")
                    model_sql_wrapper.merge_binary_with_trees(transf_params)

                if transf_name == 'FrequencyEncoder':
                    print("FrequencyEncoder Operator fusion enabled.")
                    model_sql_wrapper.merge_frequency_with_trees(transf_params)

                if transf_name == 'TargetEncoder':
                    print("TargetEncoder Operator fusion enabled.")
                    model_sql_wrapper.merge_target_with_trees(transf_params)

                if transf_name == 'LeaveOneOutEncoder':
                    print("LeaveOneOutEncoder Operator fusion enabled.")
                    model_sql_wrapper.merge_leave_with_trees(transf_params)


        new_model = {
            'trained_model': fitted_model,
            'model_sql_wrapper': model_sql_wrapper,
            'model_features': features,
        }

        out_pipeline['transforms'] = new_transformers
        out_pipeline['model'] = new_model
        out_pipeline['join_transformers_parms'] = join_transformers_parms

        return out_pipeline
