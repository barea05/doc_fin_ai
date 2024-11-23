from datetime import datetime
from pathlib import Path

import psutil
from table_extractor import table_extractor
import muni_table_standardiser as standardiser
from muni_utils import table_inserter

# For analysis purposes only
# import pandas as pd
# import duckdb


import ray

import time
from pathlib import Path

from functools import wraps
def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Function {func.__name__} took {end_time - start_time} seconds to execute.")
        return result
    return wrapper

op_date_prefix = datetime.now().strftime("%d_%b_%H_%M_%S")


@ray.remote
def extract_tables_from_muni(id):
    PDF_FILE = f"/FERack11_FE_documents2/EMMA_Official_Statement/{id}.pdf"


    for i in range(0, 5):
        try:
            if not Path(PDF_FILE).is_file():
                return [], id
        except Exception as e:
            print("FATAL: Error occured when trying to open the file: %s" % e)
            if "Host is down" in str(e):
                print("WARN: Host is down. Sleeping for: %s seconds" % (5* (i + 1)))
                time.sleep(5* (i + 1))
                continue
        
        break
    
    print("Opening file %s" % PDF_FILE)

    try:
        extractor = table_extractor.TableExtractor(PDF_FILE)
    except Exception as e:
        print(f"=====EXCEPTION: {e}==========")
        return [], id

    df_map = extractor.extract_tables(page_range=range(5))

    with open(f'/home/factentry/otto_ml/src/antlr/mondal/pdf_table_extractor/results_{op_date_prefix}/{id}.csv', 'w+') as f:
        print(f'"File ID","{id}"', file=f)
    
    df_op_list = []
    for page_no, dfs in df_map.items():
        for df in dfs:

            if df.shape[1] == 1:
                continue
            if not df.shape[0]:
                continue

            if len(df.iloc[0]) < 2:
                continue

            row_count = df.shape[0]

            is_line = False

            for ri in range(row_count):
                words = str(df.iloc[ri][0]).split()
                avg_word_length = sum(len(word) for word in words) / len(words)
                if len(words) > 10 and avg_word_length > 5:
                    is_line = True
                    continue
            
            if is_line:
                continue
            

            with open(f'/home/factentry/otto_ml/src/antlr/mondal/pdf_table_extractor/results_{op_date_prefix}/{id}.csv', 'a') as f:
                print(f"Processing page {page_no} of file {PDF_FILE}")
                print(f'\n', file=f)
                df = standardiser.clean_df(df)
                header_list = list(df.columns.values)
                header_list = ["" if header.lower().startswith("unnamed") else header for header in header_list]
                df.to_csv(f, index=False, header=header_list)
                try:
                    table_inserter(df, id)
                except Exception as e:
                    print(f"=======TABLE INSERTION FAILED===== {e}")
                # df_op_list.append(df)
    try:
        extractor.close()
    except Exception as e:
        print(f"=====EXCEPTION: {e}==========")
    # Depricated, to remove
    return df_op_list, id



@timer
def main():
    # cursor = duckdb.connect("/home/factentry/otto_ml/src/antlr/mondal/pdf_table_extractor/extraction.db")
    # cursor.sql("DROP TABLE IF EXISTS extraction")
    # cursor.sql("CREATE TABLE IF NOT EXISTS extraction (file_id VARCHAR)")
    done_tracker = open(f"/home/factentry/otto_ml/src/antlr/mondal/pdf_table_extractor/completed_{op_date_prefix}.csv", "w+", encoding="utf8")
    result_ids = []
    Path(f"/home/factentry/otto_ml/src/antlr/mondal/pdf_table_extractor/results_{op_date_prefix}").mkdir(parents=True, exist_ok=True)
    with open("/home/factentry/otto_ml/src/antlr/mondal/muni_table_extractor_v2/FileLink_AMT_Aug12.csv", encoding="utf8") as idf:
        for id in idf:
            id  = id.upper().strip()
            result_ids.append(extract_tables_from_muni.remote(id))
    

    print("Starting processing of results....")
    total_jobs = len(result_ids)
    while len(result_ids):
        print(f"Pending: {len(result_ids)}")
        done_ids, result_ids = ray.wait(result_ids)
        print(f"Done: {len(done_ids)}")
        for done_task in done_ids:
            _, id = ray.get(done_task)
            print(id, file=done_tracker)
            # for df in dfs:
            #     try:
            #         table_inserter(cursor, df, id)
            #     except Exception as e:
            #         print("Failed to insert to table. Error: %s" % e)

    done_tracker.close()
    print("All done!")



if __name__ == '__main__':
    # 80% of the total CPU
    cpu_limit = (psutil.cpu_count() // 100) * 80
    # 70% of the unutilised RAM
    memory_limit = (psutil.virtual_memory().free // 100) * 70
    ray.init(num_cpus=cpu_limit, object_store_memory=((memory_limit // 100) * 60))
    main()
    ray.shutdown()





