# import duckdb
import pandas as pd
from dhivakar.pdf_table_extractor.table_extractor import table_extractor
import dhivakar.pdf_table_extractor.muni_table_standardiser as standardiser
from dhivakar.pdf_table_extractor.muni_utils import table_inserter
import re 
import sys
sys.path.append('/home/')
from factentry.otto_ml.src.utils import db_class
import pandas as pd
FERACK13 = db_class.Database_Manager("FERACK13", "OTTO_MUNI")

def flagged_cusip(df, footer = "*"):
    flagged_cusip = {"flag_Y": set(),"flag_N": set()}
    footer_len = len(footer)
    for index, row in df.iterrows():
        cusip_flag_no = True
        # print(row)
        for val in row:
            if footer in str(val).lower()[:footer_len]  or footer in str(val).lower()[-footer_len:]:
                # print(val,index)
                cusip = str(df.loc[index]['cusip']).lower().replace(" ","").replace(footer,"")
                if cusip == "nan":
                    continue
                flagged_cusip["flag_Y"].add(cusip)
                cusip_flag_no = False                    
                break
        if cusip_flag_no:
            cusip = str(df.loc[index]['cusip']).lower().replace(" ","").replace(footer,"")
            if cusip == "nan":
                continue
            flagged_cusip["flag_N"].add(cusip)
    
    if flagged_cusip:
        for key, value in flagged_cusip.items():
            flagged_cusip[key] = tuple(value) 
        return flagged_cusip

def update_callable_db(basename= None,page_num = None,footer_pattern= None,callable_meta_data= None, collection_cusips = None):                            
    if collection_cusips:
        all_cusip = collection_cusips["flag_Y"] + collection_cusips["flag_N"]
        if all_cusip:
            set_clauses = ', '.join([f"{key} = '{value}'" for key, value in callable_meta_data.items() if value and (key != 'callprice' and key != 'calldate' and key != 'callable' and key != 'callable_line')])

            qry = ""
            if len(all_cusip[0]) > 3:
                qry = """UPDATE OTTO_MUNI.dbo.EMMA_Table_Extraction 
                        SET {0} 
                        WHERE cusip IN {1} AND FileName = '{2}'""".format(set_clauses, all_cusip, basename)
                FERACK13.query(qry)
            else:
                if (cusip_len:= len(all_cusip[0])) <= 3:
                    qry = """UPDATE OTTO_MUNI.dbo.EMMA_Table_Extraction 
                            SET {0}
                            WHERE RIGHT(cusip, {3}) IN {1} AND FileName = '{2}'""".format(set_clauses, all_cusip, basename,cusip_len)
                    FERACK13.query(qry)

            
            if collection_cusips["flag_N"]:
                if len(collection_cusips["flag_N"]) == 1:
                    collection_cusips["flag_N"] = f"('{''.join([cusip for cusip in collection_cusips['flag_N']])}')"
                
                elif cusip_len:= len(collection_cusips["flag_N"][0]):
                    qry = f""" UPDATE OTTO_MUNI.dbo.EMMA_Table_Extraction SET callable = 'N' , callprice = null where RIGHT(cusip, {cusip_len}) in {collection_cusips["flag_N"]} and FileName = '{basename}' """ 
                    FERACK13.query(qry)
                    print(qry)
                    stop_fileprocess_flag = True
            
            if collection_cusips["flag_Y"]:
                if len(collection_cusips["flag_Y"]) == 1:
                    collection_cusips["flag_Y"] = f"('{''.join([cusip for cusip in collection_cusips['flag_Y']])}')"

                if cusip_len:= len(collection_cusips["flag_Y"][0]):
                    if callable_meta_data['callprice']:
                        qry = f""" UPDATE OTTO_MUNI.dbo.EMMA_Table_Extraction SET callable = 'Y', callprice = '{callable_meta_data['callprice']}' and callable_line = '{callable_meta_data['callable_line']}' where RIGHT(cusip, {cusip_len}) in {collection_cusips["flag_Y"]} and FileName = '{basename}' """ 
                    
                    FERACK13.query(qry)
                    print(qry)
                    stop_fileprocess_flag = True


def table_inserter2(df=pd.DataFrame(),basename= None,page_num = None,footer_pattern= None,callable_meta_data= None):
    new_df = pd.DataFrame()
    # Validate if there are duplicate columns
    if len(df.columns) > len(df.columns.drop_duplicates()):
        header_list = [header for header in list(df.columns.values)]
        first_cond = True
        
        if len(df.columns) % len(df.columns.drop_duplicates()) == 0:
            num_cols = len(df.columns)
            part_size = len(df.columns.drop_duplicates())
            N = num_cols // part_size
            parts = []
            for i in range(N):
                start_col = i * part_size
                end_col = min((i + 1) * part_size, num_cols)
                part = df.iloc[:, start_col:end_col]
                print(part)
                collection_cusips = flagged_cusip(df = part,footer=footer_pattern)
                if collection_cusips: 
                    update_callable_db(basename= basename, page_num = page_num, footer_pattern = footer_pattern, callable_meta_data = callable_meta_data, collection_cusips = collection_cusips)
                first_cond = False


        duplicates = [col for col in df.columns if df.columns.tolist().count(col) > 1]
        if first_cond and duplicates:
            identify_lst_slicing = [idx for idx, col in enumerate(df.columns) if col == df.columns[0]]
            
            if len(identify_lst_slicing) == 1:
                #slice based on end column values 
                identify_lst_slicing = [0]                 
                identify_lst_slicing += [idx + 1 for idx, col in enumerate(df.columns) if (idx > 0 and (idx < len(df.columns.tolist()) -1))  and col in df.columns.tolist()[-1]]
                  
            start = 0
            parts = []
            # Step 3: Iterate over the column indices, slice the parts, and append to `parts`
            for i in range(len(identify_lst_slicing)):
                # If it's not the last column slice, get the next index for `end_col`
                next_end = identify_lst_slicing[i + 1] if i + 1 < len(identify_lst_slicing) else len(df.columns) 
                # Slice the columns from `start` to `next_end`
                new_df = df.iloc[:, start: next_end]
                start = next_end
                collection_cusips = flagged_cusip(df = new_df,footer=footer_pattern)
                if collection_cusips: 
                    update_callable_db(basename= basename, page_num = page_num, footer_pattern = footer_pattern, callable_meta_data = callable_meta_data , collection_cusips = collection_cusips)
                                
        if first_cond:
            new_df = df
            collection_cusips = flagged_cusip(df= new_df,footer=footer_pattern)
            if collection_cusips: 
                update_callable_db(basename= basename, page_num = page_num, footer_pattern = footer_pattern, callable_meta_data = callable_meta_data, collection_cusips = collection_cusips)

    else:
        new_df = df
        collection_cusips = flagged_cusip(df = new_df,footer=footer_pattern)
        if collection_cusips: 
            update_callable_db(basename= basename, page_num = page_num, footer_pattern = footer_pattern, callable_meta_data = callable_meta_data, collection_cusips = collection_cusips)


def callable_cusip_extractor(basename=None,page_num= 0,footer_pattern=None,callable_meta_data=None):    
    id = "table"
    PDF_FILE = f"/FERack11_FE_documents2/EMMA_Official_Statement/{basename}.pdf"
    extractor = table_extractor.TableExtractor(PDF_FILE)
    df_map = extractor.extract_tables(page_range=[page_num-1])

    for page_no, dfs in df_map.items():
        for df in dfs:
            if df.shape[1] == 1:
                continue
            if not df.shape[0]:
                continue

            if len(df.iloc[0]) < 2:
                continue
            
            df = standardiser.clean_df(df)
            header_list = list(df.columns.values)
            header_list = ["" if header.lower().startswith("unnamed") else header for header in header_list]
            print(f" header_list***************** {header_list}")
            if any(True for header in header_list if header.startswith("cusip")):
                # try:
                if footer_pattern[-1] == " ":
                    footer_pattern = footer_pattern[:-1]
                # print(df)     
                table_inserter2(df,basename = basename, page_num = page_num, footer_pattern = footer_pattern, callable_meta_data = callable_meta_data)