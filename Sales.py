
from DP_CSV_MYSQL.dp_csv_2_mysql import *

def main(schema_file, database_config, datafile):
    set_logging()
    schema = get_config_schema(schema_file)


    dataframe = extract_csv(datafile, schema)
    print(dataframe.head())

    error_rows = validate_data(dataframe, schema)
    temp_error_rows = error_rows
    for ter in temp_error_rows:
       print(ter)

    print(f'Records to be cleansed:{len(dataframe.index)}')
    clean_dataframe = cleanse_data(dataframe, error_rows,schema)


    conn_parameters = get_config_db(database_config)
    print(f'Records to be inserted into database:{len(clean_dataframe.index)}')
    conn_obj = connect_mysql(conn_parameters)
    load_database(clean_dataframe, schema, conn_obj)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Please pass the following parameters: schema_file , database_config_file, data_file")
        exit(-1)
    main(schema_file=sys.argv[1], database_config=sys.argv[2], datafile=sys.argv[3])


