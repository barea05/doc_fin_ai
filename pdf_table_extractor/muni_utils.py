
import re
import string
import pandas as pd

from dateutil import parser
from pymongo import MongoClient

def try_parse_date(str):
    if len(str) < 4:
        return None
    try:
        str_to_remove = (string.punctuation + string.whitespace).translate(str.maketrans('', '', ''.join(['.', ',', '/', '-', ':', '%', '$'])))
        # Reserve date separators and whatever symbols that are importanat for figuring out other types. e.g. % for  rate, $ for amount etc.
        return parser.parse(str.translate(str.maketrans(str_to_remove, ' '*len(str_to_remove))))
    except:
        return None

def can_be_cusip_part(str):
    str = re.sub(r'[\@\* ]', '', str)
    return len(str) == 3 and str[-1].isdigit()

def can_be_cusip(str):
    str = re.sub(r'[\@\* ]', '', str)
    return len(str) == 9 and can_be_cusip_part(str[-3:])

def table_inserter(df, file_id):
    # NOTE: For now, only add tables that have cusip info in the duckdb, exclude anything else
    if not "cusip" in " ".join(df.columns.values).lower():
        return
    # Activate only during final processing
    if "cusip" in df.columns.values:
        df["cusip"] = df["cusip"].map(lambda x: x if can_be_cusip(x) or can_be_cusip_part(x) else None)
    if "maturity" in df.columns.values:
        df["maturity"] = df["maturity"].map(lambda x: try_parse_date(x).strftime("%Y-%m-%d") if try_parse_date(x) else None)
    # Create a new DataFrame with vertically stacked data (if any)
    new_df = pd.DataFrame()

    # Validate if there are duplicate columns
    if len(df.columns) > len(df.columns.drop_duplicates()):
        # Total column count must be completely divisible by the no of de-duplicated columns
        # Otherwise we won't be able to find a proper pivot.
        # If condition is satisfied, our pivot will be no of de-duplicated columns
        if (len(df.columns) % len(df.columns.drop_duplicates())) == 0:
            num_cols = len(df.columns)
            part_size = len(df.columns.drop_duplicates())
            N = num_cols // part_size
            parts = []
            for i in range(N):
                start_col = i * part_size
                end_col = min((i + 1) * part_size, num_cols)
                part = df.iloc[:, start_col:end_col]
                parts.append(part)
            new_df = pd.concat(parts)
    else:
        new_df = df
    # seen_columns = set()

    # for col in df.columns:
    #     if col not in seen_columns:
    #         seen_columns.add(col)
    #         if not isinstance(df[col], pd.core.series.Series) and len(df[col].columns) > 1:
    #             for i in range(len(df[col].columns)):
    #                 new_df[col] = df[col].iloc[:,i]
    #         else:
    #             new_df[col] = df[col]
    print("Storing to MongoDB!")
    if (len(new_df.columns.values) < 1) or len(df.index) < 1:
        return
    # cursor.execute("BEGIN TRANSACTION;")
    try:
        df['file_id'] = file_id.strip()
        with MongoClient('mongodb://192.168.1.245:27017/?ssl=false') as mongo_client:
            mongo_db = mongo_client['otto_ml']
            mongo_collection = mongo_db['muni_tables']
            mongo_collection.insert_many(df.to_dict('records'))
        # for header in new_df.columns.values:
        #     print("Created new column: %s" % header.strip())
        #     cursor.execute(f"""AlTER TABLE extraction ADD COLUMN IF NOT EXISTS "{header.strip()}" VARCHAR""")
        # cursor.execute(f"INSERT INTO extraction BY NAME SELECT '{file_id.strip()}' as file_id, * FROM new_df")
        # cursor.execute("COMMIT;")
    except Exception as e:
        raise e
