import duckdb
import pandas as pd
from dhivakar.pdf_table_extractor.table_extractor import table_extractor
import dhivakar.pdf_table_extractor.muni_table_standardiser as standardiser
from dhivakar.pdf_table_extractor.muni_utils import table_inserter

def flagged_cusip(df, footer = "*"):
    """Identifies rows containing only asterisks or specific characters.
    Args:
    df: The DataFrame to check.
    Returns:
    A list of indices of footer rows.
    """

    flagged_cusip = {"flag_Y": set(),"flag_N": set()}
    footer_len = len(footer)
    for index, row in df.iterrows():
        cusip_flag_no = True
        # print(row)
        for val in row:
            # print(val)
            
            if footer in str(val).lower()[:footer_len]  or footer in str(val).lower()[-footer_len:]:#.strip() footer in str(val) or 
                # print(val,index)
                cusip = str(df.loc[index]['cusip']).lower().replace(" ","").replace(footer,"")#.replace("(","").replace(")","")
                if cusip == "nan":
                    continue
                flagged_cusip["flag_Y"].add(cusip)
                cusip_flag_no = False                    
                break
        if cusip_flag_no:
            cusip = str(df.loc[index]['cusip']).lower().replace(" ","").replace(footer,"")#.replace("(","").replace(")","")
            if cusip == "nan":
                continue
            flagged_cusip["flag_N"].add(cusip)
    
    if flagged_cusip:
        for key, value in flagged_cusip.items():
            flagged_cusip[key] = tuple(value) 

        return flagged_cusip


def table_inserter(df):
    # Create a new DataFrame with vertically stacked data (if any)
    new_df = pd.DataFrame()
    # Validate if there are duplicate columns
    if len(df.columns) > len(df.columns.drop_duplicates()):
        # Total column count must be completely divisible by the no of de-duplicated columns
        # Otherwise we won't be able to find a proper pivot.
        # If condition is satisfied, our pivot will be no of de-duplicated 
        # header_list = [True for header in list(df.columns.values) if header.lower().startswith("nan")]
        # if header_list:
        #     df = df.drop(columns=["nan"])
        
        # find_repeated_header = { for header in list(df.columns.values) if header.lower().startswith("nan")}

        # header_list = [True for header in list(df.columns.values) if header.lower().startswith("nan")]

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
                parts.append(part)
            new_df = pd.concat(parts,ignore_index=True)
            first_cond = False

        duplicates = [col for col in df.columns if df.columns.tolist().count(col) > 1]
        if first_cond and duplicates:

            # identify_lst_slicing = []
            # #slice based on cusip if it is at the end 
            if "cusip" in df.columns.tolist()[-1]:
                identify_lst_slicing = [idx for idx, col in enumerate(df.columns) if "cusip" in df.columns.tolist()[-1]]
            #silce based on first indexing
            else:    
                identify_lst_slicing = [idx for idx, col in enumerate(df.columns) if col == df.columns[0]]
            start = 0
            parts = []
            identify_lst_slicing = [idx for idx, col in enumerate(df.columns) if col == df.columns[0]]

            all_df = []
            first_df = pd.DataFrame()
            # Step 3: Iterate over the column indices, slice the parts, and append to `parts`
            for i in range(len(identify_lst_slicing)):
                # If it's not the last column slice, get the next index for `end_col`
                next_end = identify_lst_slicing[i + 1] if i + 1 < len(identify_lst_slicing) else len(df.columns) 
                # Slice the columns from `start` to `next_end`
                part_df = df.iloc[:, start: next_end]
                                
                # parts.append(f"parts_df_{i}")
                start = next_end  # Update start for the next 
                
                if i == 0:
                    pass
                else:
                    print(part_df.columns)
                    print(all_df[0].columns)
                    for col in part_df.columns:
                        print(col)
                        if col not in all_df[0].columns:
                            all_df[0][col] = pd.NA  # Add missing column with NaN values
            
                    if (first_df_len:= len(all_df[0].columns)) < (second_df_len:=len(part_df.columns)):
                        part_df = part_df.iloc[:, 0:first_df_len]                           
                    
                all_df.append(part_df)

            # Step 4: Concatenate all parts into a new DataFrame
            new_df = pd.concat(all_df, ignore_index=True)
            first_cond = False

        if first_cond:
            new_df = df
    else:
        new_df = df
    print(new_df)
    return new_df

def callable_cusip_extractor(basename,page_num= 0,footer_pattern=None):
    PDF_FILE = f"/FERack11_FE_documents2/EMMA_Official_Statement/{basename}.pdf"

    extractor = table_extractor.TableExtractor(PDF_FILE)

    # df_map = extractor.extract_tables(page_range=[2])
    df_map = extractor.extract_tables(page_range=[page_num-1])

    with open(f'/home/factentry/otto_ml/src/antlr/mondal/pdf_table_extractor/{id}.csv', 'w+') as f:
        # print(f'"File Name","{PDF_FILE}"', file=f)
        print(f'"File Name","{PDF_FILE}"', file=f)

    for page_no, dfs in df_map.items():
        for df in dfs:
            if df.shape[1] == 1:
                continue
            if not df.shape[0]:
                continue

            if len(df.iloc[0]) < 2:
                continue
            
            with open(f'/home/factentry/otto_ml/src/antlr/mondal/pdf_table_extractor/{id}.csv', 'a') as f:
                print(f'\n', file=f)
                # print(df)
                df = standardiser.clean_df(df)
                header_list = list(df.columns.values)
                header_list = ["" if header.lower().startswith("unnamed") else header for header in header_list]
                df.to_csv(f, index=False, header=header_list)
                print(f" header_list***************** {header_list}")
                if any(True for header in header_list if header.startswith("cusip")):
                    # try:
                    if footer_pattern[-1] == " ":
                        footer_pattern = footer_pattern[:-1]
                    # print(df)     
                    df = table_inserter(df)
                    collection_cusips = flagged_cusip(df,footer_pattern)
                    if collection_cusips: 
                        return collection_cusips

                    # except Exception as e:
                    #     print("Failed to insert to table. Error: %s" % e)