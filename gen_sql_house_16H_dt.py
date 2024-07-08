from model_transformer.transformer_manager import TransformerManager
from model_transformer.utility.dbms_utils import DBMSUtils


if __name__ == '__main__':
    manager = TransformerManager()

    model_file = '/volumn/tree2sql/house_16H/model_0_24/dt_reg.joblib'
    dataset_name = 'house_16H' 
    
    features = ['P1','P5p1','P6p2','P11p4','P14p9','P15p1','P15p3','P16p2','P18p2','P27p4','H2p2','H8p2','H10p1','H13p1','H18pA','H40p4']

    dbms = DBMSUtils.get_dbms_from_str_connection('postgresql://postgres:@localhost/postgres')
    preprocessors = {}


    optimizations = {
        
    }

    pre_sql = ""

    queries, query = manager.generate_query(model_file, dataset_name, features, dbms, pre_sql
                                            , optimizations, preprocessors)
    
    with open('/volumn/tree2sql/house_16H/model_0_24/dt_reg.sql', 'w') as sql_file:
        sql_file.write(query)
