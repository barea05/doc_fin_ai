import re 
import sys
sys.path.append('/home/')
from factentry.otto_ml.src.utils import db_class
FERACK13 = db_class.Database_Manager("FERACK13", "OTTO_MUNI")
from tqdm import tqdm
from dhivakar.pdf_table_extractor.example__copy import callable_cusip_extractor
from concurrent.futures import ProcessPoolExecutor 
import logging
import pdfplumber
import pytesseract 
from pdf2image import convert_from_path
import multiprocessing
from tqdm import tqdm
import concurrent.futures
from tqdm import tqdm
import os
import sys
# from callable_option import process_check
import process_check
import concurrent.futures
from tqdm import tqdm

# if process_check.check_if_running(__file__):
#     print ('Schedule started')

# else:
#     sys.exit('Another schedule is running')

filename='/home/dhivakar/callable_option/collect _footer_not_matched_lines.log'
logging.basicConfig(filename=filename, filemode='a', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

call_date_pattern = r"((\d{1,4}[-\/]\d{1,2}[-\/]\d{2,4})|((?:January|Januaryy|Janaury|Janauryy|February|Februaryy|March|Mareh|April|Appril|Mayy|June|July|Julyy|August|Auggust|September|Sepptember|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|jan|feb|mar|apr|may|mayy|jun|jul|aug|sep|oct|nov|dec)[\s.,-]*\d{1,2}?(?:st|nd|rd|th)?[\s.,-]*\d{4})|(\d{1,2}[\s.,-]*(?:January|Janaury|Janauryy|February|Februaryy|March|Mareh|April|Appril|May|Mayy|June|July|Julyy|August|Auggust|September|Sepptember|October|November|December|Jan|Feb|Mar|Apr|May|Mayy|Jun|Jul|Aug|Sep|Oct|Nov|Dec|jan|feb|mar|apr|may|mayy|jun|jul|aug|sep|oct|nov|dec)[\s.,-]*\d{4}))"

def extractor_footer(line):

    line = line.lower()
    footer_pattern = r"(\*{1,3}|\†{1,3}|\‡|\{1,2}|\°{1,2}|\©{1,2}|\†{1,2}|\+{1,2})|( c{1,3} |\(c{1,2}\)|\(a{1,3}\)|\(1\)|\(2\)|\(3\)|\(4\)|%°|%c |cc | c |c )" 
    ftr_ptn_bt_snt = r"(\*{1,3}|\†{1,3}|\{1,2}|\°{1,2}|\©{1,2}|\†{1,2}|\+{1,2})|(c{2,3} |\(c{1,2}\)|\(a{1,3}\)|\(1\)|\(2\)|\(3\)|\(4\)|%°|%c |cc | c )" #|(c{2,3}


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


#The Bonds are subject to redemption at the option and check dates 

def extractor_callable_lines(line="", footer_pattern = None, basename = None):
    sliced_line = re.sub(r" at the | to the | the | on | to | of | at | and | is | as | or | are | by | a | in ", " ", line)        
    call_opt_kwd_ptn_ful_line = r"((?:Priced|Price|Prized) (?:to the optional call|to call|to the call date|to call|to par call|to the first call date |at the stated yield|to|assuming redemption|at stated yield))|((?:Yield to|Y ield to) (?:call at|first call date|call date|a par call|par call|first call date|first optional redemption date|the first optional redemption|Optional Redemption|date of earliest optional redemption|assuming optional redemption|first optional par call date|earliest call date|first par call))|(?:Yield (?:and price|calculated to the first optional call|calculated to first optional call|shown is to first optional redemption date|calculated to|shown to first optional redemption date|is calculated to|calculated assuming redemption|assuming redemption|on the Two Hundred Fortieth Series|to the call date of|computed to earliest redemption date|to call on|computed to the first optional redemption date))|(first optional call|calculated to first optional call|calculated to the first optional redemption date|resized from original par amount|Pricing assumes redemption|Price and yield information provided by the underwriters|Yield to Call|Yield as of first optional redemption date|Yields calculated to|The yields on these maturities are calculated|Callable premium bond|Yields shown are to the first optional redemption date|Yield calculated to the first optional redemption date|Subject to redemption prior to maturity|Calculated to the first call date|Yield shown to first call date|Yield to Purchase Date|Yield to optional par call|The Bonds are subject to redemption at the option|par call date|first call date|optional redemption date)" 
    call_opt_kwd_ptn_sliced_line = r"((?:Priced|Price|Prized|Yield|Y ield)(?: optional call| call| first call| call date| par call| stated yield| assuming redemption| call date| par call| first call date| first optional redemption| Optional Redemption| first optional redemption date| date earliest optional redemption| assuming optional redemption| reflects pricing first optional redemption date| shown first optional redemption date| shown optional redemption| first optional par redemption date| based pricing first optional redemption| first optional par call date| earliest call date| first par call| price| calculated first optional call| calculated| date earliest optional redemption| calculated assuming redemption| assuming redemption| Two Hundred Fortieth Series| computed earliest redemption date| earliest redemption date| earliest optional redemption date| computed first optional redemption | calculated based| based pricing))|(first optional call|calculated first optional call|calculated first optional redemption date|resized from original par amount|Pricing assumes redemption|Price yield information provided by underwriters|Yield Call|Yields calculated|yields these maturities calculated|Callable premium bond|Yields shown first optional redemption date|Yield calculated first optional redemption date|Subject redemption prior maturity|Calculated first call date|Yield shown first call date|Yield Purchase Date|Yield optional par call|Bonds subject redemption option)"        
    call_opt_kwd_ptn2_with_date = rf"(?:Yield|Yield calculated|Yield first optional par call date|Yield first call date of|Redemption Date —|Bonds maturing after|Priced|Yield par call|Yidd call par|Yield shown|Calculated first optional redemption date|Yield shown|Yield first redemption date|Yield first par call|Priced assuming optional redemption|Yield shown first available call date|Yield\/Price|Price Yield first call date|Yield first optional prepayment date|Yield based pricing|Calculated par call date|Yields Prices first optional redemption date|Yields prices calculated first optional prepayment|Priced call date|Bonds maturing after|subject optional redemption whole part|Bonds maturing|first optional redemption date|first call date,|optional redemption date) {call_date_pattern}"

#The Bonds maturing on or after 
# The Notes maturing on or after
# calculated to the optional call date of
#The Bonds maturing on September 1, 2032, and thereafter shall be subject to redemption and payment at the option of the District, in whole or from
# print("MATCHED:    ", line )
# logging.info(f"{line}")

    if line:
        if line_matches:= re.search(call_opt_kwd_ptn_sliced_line, sliced_line,re.IGNORECASE):
            pass

        elif line_matches:= re.search(call_opt_kwd_ptn_ful_line.replace(" ",""), line.replace(" ",""),re.IGNORECASE):
            pass

        elif line_matches:= re.search(call_opt_kwd_ptn2_with_date, sliced_line,re.IGNORECASE):
            pass

        else:
            logging.info(f"NTM*** {basename}: {line}   ")

    return line

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


def extractor_termbond_callprice(line):
    if 'price' in line.lower():
        line_idx = line.lower().find('price')    
        line = line[line_idx: ]
        callprice_pattern = r"price (?:\d*%|\d*\.\d*% due)"
        if matches:= re.search(callprice_pattern, line, re.IGNORECASE):
            
            if matches:= re.search("(\d*%|\d*\.\d*%)",matches.group()):
                return matches.group()
        #     else:
        #         return "100%"
        # else:
        #     return "100%"    


def direct_callable_extractor(line, footer = None,basename=None):
    line = line.strip()
    call_option_dict = {"callfooter": footer,
                        "calldate": None,
                        "callable": 'N',
                        "callprice": None,
                        
    }#"callable_line": ""
    
    if footer_pattern:= extractor_footer(line):        
        if line_matches:= extractor_callable_lines(line,basename=basename):
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
            logging.info(f"NTM*** {basename}: {line}")

    # print(call_option_dict)
    return call_option_dict


def term_bond_cusip(line, basename):
    
    call_option_dict = {
        "callfooter": "",
        "calldate": None,
        "callable": 'N',
        "callable_line" : ""        
        
    }
    
    lower_line = line.lower().replace(" ","")
    
    if 'termbond' in lower_line and 'cusip' in lower_line:    
        # query = f"select CUSIP from otto_muni.dbo.EMMA_Table_Extraction where callable is null and filename = '{basename}' "
        query = f"select CUSIP from otto_muni.dbo.EMMA_Table_Extraction where filename = '{basename}' and callable is null"
        cusip_lst = [CUSIP["CUSIP"] for CUSIP in FERACK13.fetchArray_withKey(query)]

        if cusip_lst:
            for cusip in cusip_lst:
                n = 2
                if len(cusip) >= 3:
                    n = 3
                else:
                    n = 2
                # if cusip.lower()[-n:].replace(" ","") in lower_line: 
                if cusip.replace(" ","")[-n:] in line: 
                    if footer:=extractor_footer(line):
                        print(cusip, "    ", cusip[-n:],     line)                        
                        call_option_dict["callfooter"] = footer
                        call_option_dict["callable"] = 'Y'                                                
                        # call_option_dict["callprice"] = extractor_termbond_callprice(line)
                        call_option_dict["callable_line"] = line
                        set_clause = ', '.join([f"{key} = '{value}'" for key, value in call_option_dict.items() if value])
                        query = f"""update otto_muni.dbo.EMMA_Table_Extraction set {set_clause} where RIGHT(cusip,{len(cusip)}) in ('{cusip}') and filename = '{basename}' """                        
                        FERACK13.query(query)
                    # else:    
                    #     call_option_dict["callfooter"] = 'N'
                    #     query = f"""update otto_muni.dbo.EMMA_Table_Extraction set callable = y where cusip in '{cusip}' """


def callable_extractor(basename):
    # basename = "EA608205-EA475820-EA872339"
    # "EP643831-EP503109-EP904023"
    # TXT_FILE = f"/OTTO-Project/FE_Documents_Txt/EMMA_Official_Statement/{basename}.txt"
    # print(TXT_FILE)
    pdf_path = rf"/FERack11_FE_documents2/EMMA_Official_Statement/{basename}.pdf"
    # print(pdf_path)
    # logging.info(f"Basename      {basename}")
     
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
                        # print(line)
                        term_bond_cusip(line, basename)
                        pass
                    
                    
                    # if "Optional Redemption Date" in line:
                    #     print(line, stop_iteration, pdf_pagenum)
                    #     pass 
                    # # if "Southwest Securities, Inc" in line:
                    #     # pass 
                    # if footer_pattern:= extractor_footer(line):
                    #     print(f"footer_pattern       {footer_pattern}")
                    #     callable_yes = False
                    #     stop_fileprocess_flag = False 
                    #     callable_meta_data = direct_callable_extractor(line,footer_pattern,basename)
                    #     if callable_meta_data["callable"] == 'Y':
                    #         print(f"line        {line}")
                    #         callable_yes = True
                    #     # elif callable_meta_data["callable"] != 'Y':
                    #     #     callable_meta_data = direct_callable_extractor(line,footer_pattern)
                    #     #     print(line)
                    #     if callable_yes:
                    #             callable_cusip_extractor(basename=basename,page_num = pdf_pagenum,footer_pattern=footer_pattern,callable_meta_data=callable_meta_data)
                                

query = "select distinct FileName from OTTO_MUNI.dbo.EMMA_Table_Extraction where callable is null"
# query = "select distinct FileName from OTTO_MUNI.dbo.EMMA_Table_Extraction where callable_line like '%term bond%' "

isin_queue = [files['FileName'] for files in FERACK13.fetchArray_withKey(query)]#[:1]

# for basename in tqdm(isin_queue):    
#     try:
#         callable_extractor(basename)
#     except Exception as e:
#         print(f"Error    {e}")
#         pass


# for basename in tqdm(isin_queue):
#     callable_extractor(basename)

    


# #'ER824509-ER642574-ER1044496',
# # isin_queue = ['ER1109686-ER867955-ER1268665','EA657900-EA515084-EA911239']

# Define your function to process each file
def process_file(basename):
    try:
        callable_extractor(basename)
    except Exception as e:
        # Optionally, log the error if needed
        print(f"overall error  {e}")
        pass

with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:    
    for basename in tqdm(isin_queue):
        executor.submit(process_file,basename)




# def process_in_batches(isin_queue, batch_size=10000):
#     # Split the isin_queue into batches
#     for i in range(0, len(isin_queue), batch_size):
#         batch = isin_queue[i:i + batch_size]
        
#         # Create a multiprocessing pool with the number of workers as the batch size
#         with multiprocessing.Pool(processes=batch_size) as pool:
#             # Use tqdm to show progress with the pool map
#             list(tqdm(pool.imap(process_file, batch), total=len(batch), desc=f"Processing batch {i//batch_size + 1}"))


# def process_in_batches(isin_queue, batch_size=10000):
#     # Split the isin_queue into batches
#     for i in range(0, len(isin_queue), batch_size):
#         batch = isin_queue[i:i + batch_size]
        
#         # Create a ProcessPoolExecutor with the number of workers as the batch size
#         with concurrent.futures.ProcessPoolExecutor(max_workers=batch_size) as executor:
#             # Use tqdm to show progress with the executor map
#             list(tqdm(executor.map(process_file, batch), total=len(batch), desc=f"Processing batch {i // batch_size + 1}"))



# def process_in_batches(isin_queue, batch_size=1000000):
#     # Split the isin_queue into batches
#     for i in range(0, len(isin_queue), batch_size):
#         batch = isin_queue[i:i + batch_size]
        
#         # Create a ProcessPoolExecutor with the number of workers as the batch size
#         with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
#             # List to store the Future objects for each task
#             futures = []
            
#             # Submit each task (process_file) to the executor for each item in the batch
#             for item in batch:
#                 future = executor.submit(process_file, item)
#                 futures.append(future)

#             # Use tqdm to show progress with the futures
#             for future in tqdm(concurrent.futures.as_completed(futures), total=len(batch), desc=f"Processing batch {i // batch_size + 1}"):
#                 future.result()  # Get the result of each task (this will raise exceptions if any occurred)


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
