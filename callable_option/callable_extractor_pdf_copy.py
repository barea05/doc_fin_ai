import re 
import sys
sys.path.append('/home/')
from factentry.otto_ml.src.utils import db_class
FERACK13 = db_class.Database_Manager("FERACK13", "OTTO_MUNI")
from tqdm import tqdm
from dhivakar.pdf_table_extractor.example_copy import callable_cusip_extractor
from concurrent.futures import ProcessPoolExecutor 
import logging
import pdfplumber
import pytesseract 
from pdf2image import convert_from_path
import multiprocessing
from tqdm import tqdm


# from callable_option import process_check
import process_check
if process_check.check_if_running(__file__):
    print ('Schedule started')

else:
    sys.exit('Another schedule is running')

filename='/home/dhivakar/callable_option/collect _footer_not_matched_lines.log'
logging.basicConfig(filename=filename, filemode='a', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

call_date_pattern = r"((\d{1,4}[-\/]\d{1,2}[-\/]\d{2,4})|((?:January|Januaryy|Janaury|Janauryy|February|Februaryy|March|Mareh|April|Appril|Mayy|June|July|Julyy|August|Auggust|September|Sepptember|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|jan|feb|mar|apr|may|mayy|jun|jul|aug|sep|oct|nov|dec)[\s.,-]*\d{1,2}?(?:st|nd|rd|th)?[\s.,-]*\d{4})|(\d{1,2}[\s.,-]*(?:January|Janaury|Janauryy|February|Februaryy|March|Mareh|April|Appril|May|Mayy|June|July|Julyy|August|Auggust|September|Sepptember|October|November|December|Jan|Feb|Mar|Apr|May|Mayy|Jun|Jul|Aug|Sep|Oct|Nov|Dec|jan|feb|mar|apr|may|mayy|jun|jul|aug|sep|oct|nov|dec)[\s.,-]*\d{4}))"

def extractor_footer(line):
    # footer_pattern = r"[**|*|†||°|©|†|+|]|c{2,3}|\(c{1,2}\)|\(a{1,3}\)|\(1\)|\(2\)|\(3\)|\(4\)|%°|%c" 
    # ftr_ptn_bt_snt = r"[**|*|†||°|©|†|+|]|c{3}|\(c{1,3}\)|\(a{1,3}\)|\(1\)|\(2\)|\(3\)|\(4\)|%°|%c"
    line = line.lower()
    footer_pattern = r"(\*{1,2}|\†{1,2}|\{1,2}|\°{1,2}|\©{1,2}|\†{1,2}|\+{1,2})|( c{1,3} |\(c{1,2}\)|\(a{1,3}\)|\(1\)|\(2\)|\(3\)|\(4\)|%°|%c |cc | c |c )" 
    ftr_ptn_bt_snt = r"(\*{1,2}|\†{1,2}|\{1,2}|\°{1,2}|\©{1,2}|\†{1,2}|\+{1,2})|(c{2,3} |\(c{1,2}\)|\(a{1,3}\)|\(1\)|\(2\)|\(3\)|\(4\)|%°|%c |cc | c )" #|(c{2,3}


    first_condition = True
    if matches:= re.search(footer_pattern, line[:2]): #Accured word: case fails --> (Accrued Interest from February 15, 2013 to be added)  
        if matches.group().strip() == "cc":
            return matches.group().strip()
            first_condition = False

        elif matches.group().strip() == "c":
            return matches.group().strip()
            first_condition = False

    if first_condition:
        if matches:= re.search(footer_pattern, line[:5]):
            print(line)
            if matches.group() == "\uf02a": # --> "\uf02a" BUT i WILL CONSIDER AS A '*'
                return "*"
            elif matches.group():
                return matches.group()#.strip()

        elif matches:= re.search(ftr_ptn_bt_snt, line,re.IGNORECASE):
            return matches.group().replace("%","")#.strip()

    return None 


def extractor_callable_lines2(line="", footer_pattern = None):

    #$5,795,000 3.250% Term Bonds due August 1, 2036, Priced at 98.226% to Yield 3.360% - 564096UF0(1) 75
    call_opt_kwd_ptn2_with_date = rf"(Term Bonds due) {call_date_pattern}"

    if line_matches:= re.search(call_opt_kwd_ptn2_with_date, line,re.IGNORECASE):
        # print("NEWLY MATCHED:    ", line )
        # logging.info(f"{line}")
        return line


def extractor_callable_lines3(line="", footer_pattern = None):

    #$5,795,000 3.250% Term Bonds due August 1, 2036, Priced at 98.226% to Yield 3.360% - 564096UF0(1) 75
    call_opt_kwd_ptn2_with_date = rf"{call_date_pattern} optional redemption date"

    if line_matches:= re.search(call_opt_kwd_ptn2_with_date, line,re.IGNORECASE):
        # print("NEWLY MATCHED:    ", line )
        # logging.info(f"{line}")
        return line




def extractor_callable_lines(line="", footer_pattern = None):
    sliced_line = re.sub(r" at the | to the | the | on | to | of | at | and | is | as | or | are | by | a ", " ", line)
    
    call_opt_kwd_ptn_ful_line = r"((?:Priced|Price|Prized) (?:to the optional call|to call|to the call date|to call|to par call|to the first call date |at the stated yield|to|assuming redemption|at stated yield))|((?:Yield to|Y ield to) (?:call at|first call date|call date|a par call|par call|first call date|first optional redemption date|the first optional redemption|Optional Redemption|date of earliest optional redemption|assuming optional redemption|first optional par call date|earliest call date|first par call))|(?:Yield (?:and price|calculated to the first optional call|calculated to first optional call|shown is to first optional redemption date|calculated to|shown to first optional redemption date|is calculated to|calculated assuming redemption|assuming redemption|on the Two Hundred Fortieth Series|to the call date of|computed to earliest redemption date|to call on|computed to the first optional redemption date))|(first optional call|calculated to first optional call|calculated to the first optional redemption date|resized from original par amount|Pricing assumes redemption|Price and yield information provided by the underwriters|Yield to Call|Yield as of first optional redemption date|Yields calculated to|The yields on these maturities are calculated|Callable premium bond|Yields shown are to the first optional redemption date|Yield calculated to the first optional redemption date|Subject to redemption prior to maturity|Calculated to the first call date|Yield shown to first call date|Yield to Purchase Date|Yield to optional par call)" 

    call_opt_kwd_ptn_sliced_line = r"((?:Priced|Price|Prized|Yield|Y ield)(?: optional call| call| first call| call date| par call| stated yield| assuming redemption| call date| par call| first call date| first optional redemption| Optional Redemption| first optional redemption date| date earliest optional redemption| assuming optional redemption| reflects pricing first optional redemption date| shown first optional redemption date| shown optional redemption| first optional par redemption date| based pricing first optional redemption| first optional par call date| earliest call date| first par call| price| calculated first optional call| calculated| date earliest optional redemption| calculated assuming redemption| assuming redemption| Two Hundred Fortieth Series| computed earliest redemption date| earliest redemption date| earliest optional redemption date| computed first optional redemption | calculated based| based pricing))|(first optional call|calculated first optional call|calculated first optional redemption date|resized from original par amount|Pricing assumes redemption|Price yield information provided by underwriters|Yield Call|Yields calculated|yields these maturities calculated|Callable premium bond|Yields shown first optional redemption date|Yield calculated first optional redemption date|Subject redemption prior maturity|Calculated first call date|Yield shown first call date|Yield Purchase Date|Yield optional par call)"
        

    call_opt_kwd_ptn2_with_date = rf"(?:Yield|Yield calculated|Yield first optional par call date|Yield first call date of|Redemption Date —|Bonds maturing after|Priced|Yield par call|Yidd call par|Yield shown|Calculated first optional redemption date|Yield shown|Yield first redemption date|Yield first par call|Priced assuming optional redemption|Yield shown first available call date|Yield\/Price|Price Yield first call date|Yield first optional prepayment date|Yield based pricing|Calculated par call date|Yields Prices first optional redemption date|Yields prices calculated first optional prepayment|Priced call date|Bonds maturing after) {call_date_pattern}"

        
    if line:
        if line_matches:= re.search(call_opt_kwd_ptn_sliced_line, sliced_line,re.IGNORECASE):
            # print("MATCHED:    ", line )
            # logging.info(f"{line}")
            return line            

        elif line_matches:= re.search(call_opt_kwd_ptn_ful_line.replace(" ",""), line.replace(" ",""),re.IGNORECASE):
            # print("MATCHED:    ", line )
            # logging.info(f"{line}")
            return line         

        #and [re.search(diff_keys, line.lower(),re.IGNORECASE) for diff_keys in ["optional redemption date","call date"]] 
        elif line_matches:= re.search(call_opt_kwd_ptn2_with_date, sliced_line,re.IGNORECASE):
            # print("NEWLY MATCHED:    ", line )
            # logging.info(f"{line}")
            return line

        else:
            # logging.info(TXT_FILE)
            # logging.info("\n")
            logging.info(f"NOT MATCHED: {line}")
            return None

    return None

def extractor_calldate(line):
    if matches:= re.search(call_date_pattern, line):
        return matches.group()

def extractor_callprice(line):
    callprice_pattern = r" \d*%|par"
    if matches:= re.search(callprice_pattern, line):
        if matches:= re.search(" \d*%",matches.group()):
            return matches.group()
        else:
            return "100%"
    else:
        return "100%"    

def direct_callable_extractor(line, footer = None):
    line = line.strip()
    call_option_dict = {"callfooter": footer,
                        "calldate": None,
                        "callable": 'N',
                        "callprice": None,
                        
    }#"callable_line": ""
    
    if footer_pattern:= extractor_footer(line):        
        if line_matches:= extractor_callable_lines(line):
            call_option_dict["callable"] = "Y"
            call_option_dict["callfooter"] = footer_pattern
            call_option_dict["calldate"] = extractor_calldate(line)
            call_option_dict["callprice"] = extractor_callprice(line)
            call_option_dict["callable_line"] = line
            # print(line)            
        # elif extractor_callable_lines2(line, footer_pattern=footer_pattern):
        #         call_option_dict["callable"] = "Y"
        #         call_option_dict["callfooter"] = footer_pattern
        #         call_option_dict["calldate"] = extractor_calldate(line)
        #         call_option_dict["callprice"] = "100%"
        #         call_option_dict["callable_line"] = line
        elif extractor_callable_lines3(line, footer_pattern=footer_pattern):
                call_option_dict["callable"] = "Y"
                call_option_dict["callfooter"] = footer_pattern
                call_option_dict["calldate"] = extractor_calldate(line)
                call_option_dict["callprice"] = "100%"
                call_option_dict["callable_line"] = line
        else:
            print(f"Not matched: {line}")
            logging.info(f"Not matched: {line}")

    # print(call_option_dict)
    return call_option_dict




def callable_extractor(basename):
    # basename = "ES822325-ES645208-ES1040379"
    # TXT_FILE = f"/OTTO-Project/FE_Documents_Txt/EMMA_Official_Statement/{basename}.txt"
    # print(TXT_FILE)
    pdf_path = rf"/FERack11_FE_documents2/EMMA_Official_Statement/{basename}.pdf"
    print(pdf_path)
     
    with pdfplumber.open(pdf_path) as pdf:
        pdf_pagenum = 0
        stop_iteration = 0
        text = ""
        for page in pdf.pages:
            pdf_pagenum += 1
            if pdf_pagenum > 3:
                break                
            text = page.extract_text()

        # images = convert_from_path(pdf_path, poppler_path='/usr/bin')                

            if text:
                page_content = text.split("\n")
                
                for line in page_content:
                    line = line.replace("\n","").strip()
                    stop_iteration +=1
                    if stop_iteration > 200:
                        break

                    if line == "":
                        continue

                    if "term bond" in line.lower():
                        continue
                    
                    if "Optional Redemption Date" in line:
                        print(line, stop_iteration, pdf_pagenum)
                        pass 
                    # if "Southwest Securities, Inc" in line:
                        # pass 
                    if footer_pattern:= extractor_footer(line):
                        print(f"footer_pattern       {footer_pattern}")
                        callable_yes = False
                        stop_fileprocess_flag = False 
                        callable_meta_data = direct_callable_extractor(line,footer_pattern)
                        if callable_meta_data["callable"] == 'Y':
                            print(f"line        {line}")
                            callable_yes = True
                        # elif callable_meta_data["callable"] != 'Y':
                        #     callable_meta_data = direct_callable_extractor(line,footer_pattern)
                        #     print(line)
                        if callable_yes:
                            cusip_collections = callable_cusip_extractor(basename,page_num = pdf_pagenum,footer_pattern=footer_pattern)
                            if cusip_collections:
                                all_cusip = cusip_collections["flag_Y"] + cusip_collections["flag_N"]
                                if all_cusip:
                                    # Dynamically create the SET part for the UPDATE clause
                                    set_clauses = ', '.join([f"{key} = '{value}'" for key, value in callable_meta_data.items() if value and (key != 'callprice' or key != 'calldate')])

                                    # for key, value in callable_meta_data.items():
                                        # if value:
                                    # qry = f""" UPDATE OTTO_MUNI.dbo.EMMA_Table_Extraction SET {key} = '{value}' .format{key, value in callable_meta_data.items()} where cusip in {all_cusip} and FileName = '{basename}' """ 
                                    qry = ""
                                    if len(all_cusip[0]) > 3:
                                        # Construct the full query using .format() with the dynamically generated set_clauses
                                        qry = """UPDATE OTTO_MUNI.dbo.EMMA_Table_Extraction 
                                                SET {0} 
                                                WHERE cusip IN {1} AND FileName = '{2}'""".format(set_clauses, all_cusip, basename)
                                        FERACK13.query(qry)

                                        # ## Dynamically create the VALUES part for the USING clause
                                        # values_clause = ',\n        '.join([f"('{cusip}', '{line}', '{basename}')" for cusip in all_cusip])


                                        # # Dynamically create the columns and values for the INSERT clause, excluding 'callprice' and None values
                                        # columns = ', '.join([key for key, value in callable_meta_data.items() if value and key != 'callprice'])
                                        # values = ', '.join([repr(value) for key, value in callable_meta_data.items() if value and key != 'callprice'])

                                        # # Construct the subquery for the INSERT statement
                                        # insert_sub_qry = f"""
                                        # INSERT INTO OTTO_MUNI.dbo.EMMA_Table_Extraction (cusip, callable_line, FileName, {columns})
                                        # VALUES (source.cusip, source.callable_line, source.FileName, {values});
                                        # """

                                        # # Construct the MERGE query
                                        # qry = f"""
                                        # MERGE INTO OTTO_MUNI.dbo.EMMA_Table_Extraction AS target
                                        # USING (
                                        #     VALUES 
                                        #         {values_clause}
                                        # ) AS source(cusip, callable_line, FileName)
                                        # ON target.cusip = source.cusip AND target.FileName = source.FileName
                                        # WHEN MATCHED THEN
                                        #     UPDATE SET {set_clauses}, callable_line = source.callable_line
                                        # WHEN NOT MATCHED THEN
                                        #     {insert_sub_qry};
                                        # """
                                    else:
                                        # Output the generated query for debugging
                                        if (cusip_len:= len(all_cusip[0])) <= 3:
                                            # Construct the full query using .format() with the dynamically generated set_clauses
                                            qry = """UPDATE OTTO_MUNI.dbo.EMMA_Table_Extraction 
                                                    SET {0}
                                                    WHERE RIGHT(cusip, {3}) IN {1} AND FileName = '{2}'""".format(set_clauses, all_cusip, basename,cusip_len)
                                            FERACK13.query(qry)

                                    
                                    if cusip_collections["flag_N"]:
                                        if len(cusip_collections["flag_N"]) == 1:
                                            cusip_collections["flag_N"] = f"('{''.join([cusip for cusip in cusip_collections['flag_N']])}')"
                                        
                                        elif cusip_len:= len(cusip_collections["flag_N"][0]):
                                            qry = f""" UPDATE OTTO_MUNI.dbo.EMMA_Table_Extraction SET callable = 'N' , callprice = null where RIGHT(cusip, {cusip_len}) in {cusip_collections["flag_N"]} and FileName = '{basename}' """ 
                                            FERACK13.query(qry)
                                            print(qry)
                                            stop_fileprocess_flag = True
                                    
                                    if cusip_collections["flag_Y"]:
                                        if len(cusip_collections["flag_Y"]) == 1:
                                            cusip_collections["flag_Y"] = f"('{''.join([cusip for cusip in cusip_collections['flag_Y']])}')"

                                        if cusip_len:= len(cusip_collections["flag_Y"][0]):
                                            if callable_meta_data['callprice']:
                                                qry = f""" UPDATE OTTO_MUNI.dbo.EMMA_Table_Extraction SET callable = 'Y', callprice = '{callable_meta_data['callprice']}' where RIGHT(cusip, {cusip_len}) in {cusip_collections["flag_Y"]} and FileName = '{basename}' """ 
                                            
                                            FERACK13.query(qry)
                                            print(qry)
                                            stop_fileprocess_flag = True

                                    # if stop_fileprocess_flag: #MIGHT NOT WORK mulitple tables PAGES TOO SO MEASURE THE PAGES TOO 
                                    #     break

query = "select distinct FileName from OTTO_MUNI.dbo.EMMA_Table_Extraction where callable is null"
isin_queue = [files['FileName'] for files in FERACK13.fetchArray_withKey(query)]#[:1]

for basename in tqdm(isin_queue):    
    try:
        callable_extractor(basename)
    except:
        pass


# # for basename in tqdm(isin_queue):
# #     try:
# #         callable_extractor(basename)
# #     except Exception as e:
# #         print(f"overall error  {e}")

    


#'ER824509-ER642574-ER1044496',
# isin_queue = ['ER1109686-ER867955-ER1268665','EA657900-EA515084-EA911239']

# with ProcessPoolExecutor(max_workers=60) as executor:    
#     for basename in tqdm(isin_queue):
#         executor.submit(callable_extractor,basename)





# # Define your function to process each file
# def process_file(basename):
#     try:
#         callable_extractor(basename)
#     except Exception as e:
#         # Optionally, log the error if needed
#         print(f"overall error  {e}")
#         pass

# def process_in_batches(isin_queue, batch_size=40):
#     # Split the isin_queue into batches
#     for i in range(0, len(isin_queue), batch_size):
#         batch = isin_queue[i:i + batch_size]
        
#         # Create a multiprocessing pool with the number of workers as the batch size
#         with multiprocessing.Pool(processes=batch_size) as pool:
#             # Use tqdm to show progress with the pool map
#             list(tqdm(pool.imap(process_file, batch), total=len(batch), desc=f"Processing batch {i//batch_size + 1}"))

# if __name__ == "__main__":
#     # Example usage
#     query = "select distinct FileName from OTTO_MUNI.dbo.EMMA_Table_Extraction where callable is null"
#     isin_queue = [files['FileName'] for files in FERACK13.fetchArray_withKey(query)]#[:1]        
#     process_in_batches(isin_queue)





# lines = [
# "(d) Priced to the call date of August 15, 2024",
# "* Priced to call at par on September 1, 2014",
# "* Priced to par call on December 1, 2024",
# "* Priced to the first call date of December 1, 2025",
# "* The Bonds maturing in 2023 and 2024 were priced to the first call date",
# "* Yield calculated to the first optional redemption date (December 1, 2027)",
# "* Yield to call at par on August 1, 2018",
# "* Yield to First Call Date",
# "*Priced to the optional call date of August 1, 2013 at Par",
# "*Yield and price calculated to the first optional redemption date of June 1, 2031",
# "† Priced to the first call date of December 1, 2027",
# "† Priced to the first optional call date of December 1, 2029",
# "†Priced to the first par call date of December 1, 2031",
# "2 Priced to the first optional redemption date of May 15, 2027, at par.",
# "C Priced at the stated yield to the May 15, 2029 optional redemption date at par.",
# "c Priced to the August 1, 2029, par call date",
# "C: Yield to call at 103% of par at first optional redemption date of September 1, 2025",
# "C: Yield to call at 103% on September 1, 2026"
# ]

# """
# $105,000 4.500% Term Bond due April 1, 2017 Priced at Par CUSIP: 855048 AG9 

# $305,000 6.500% Term Bonds due August 1, 2027; Yield: 4.650%C; CUSIP†: DR2

# $180,0001.50% Term Bonds due June 1, 2017. Price: 100. CUSIP No. 344461GS1 

# $245,000 Term Bonds due August 1, 2048; Interest Rate: 3.000%; Price: 97.000%; CUSIP No.1 280173DD4
#$5,795,000 3.250% Term Bonds due August 1, 2036, Priced at 98.226% to Yield 3.360% - 564096UF0(1) 75
# """


            # if line_matches.group():
            #     if matches:= re.search(footer_pattern, line[:2]): #Accured word: case fails --> (Accrued Interest from February 15, 2013 to be added)  
            #         if matches.group() == "cc": # --> "\uf02a" --> CONSIDER AS A '*'
            #             call_option_dict["callfooter"] = matches.group()
            #             call_option_dict["callable"] = "Y"

            #     elif matches:= re.search(footer_pattern, line[:5]):
            #         print(line)
            #         if matches.group() == "\uf02a": # --> "\uf02a" BUT i WILL CONSIDER AS A '*'
            #             call_option_dict["callfooter"] = "*"
            #             call_option_dict["callable"] = "Y"
            #         elif matches.group():
            #             call_option_dict["callfooter"] = matches.group()
            #             call_option_dict["callable"] = "Y"

            #     if matches:= re.search(ftr_ptn_bt_snt, line,re.IGNORECASE):
            #         call_option_dict["callfooter"] = matches.group().replace("%","")
            #         call_option_dict["callable"] = "Y"

            #     if matches:= re.search(call_date_pattern, line):
            #         call_option_dict["calldate"] = matches.group()
                
            #     if matches:= re.search(callprice_pattern, line):
            #         if matches:= re.search("\d*%",matches.group()):
            #             call_option_dict["callprice"] = matches.group()
            #         else:
            #             call_option_dict["callprice"] = "100%"
