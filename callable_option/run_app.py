import sys, os
path = '/home/fedocker/otto/'
sys.path.append(path)
from migration import db_class  
from utils import base_class, constants, get_content
from otto_extractors import misc
from models import spider
import re 
from otto_extractors.data_extract_with_ml import data_extract_with_ml_20_fields
from utils.get_content import convert_to_text
from tqdm import tqdm 
from concurrent.futures import ProcessPoolExecutor
from utils import process_check
import requests
from utils.get_date import get_date_regex_for_issuedate
from datetime import datetime
from pdf2image import convert_from_path
from contextlib import contextmanager
import signal

FERACK34 = db_class.Database_Manager('FERACK34','ISIN')
FERACK54 = db_class.Database_Manager('FERACK54','ISIN')
    
def load_filenames(query):
    return FERACK34.fetchArray_withKey(query)

map_date = {
   "Jan" :"January",
    "Feb" :"February",
    "Mar" :"March",
    "Apr" :"April",
    "May" :"May",
    "Jun" :"June",
    "Jul" :"July",
    "Aug" :"August",
    "Sep" :"September",
    "Oct" :"October",
    "Nov" :"November",
    "Dec" :"December"
} 

french_month_map_short_long_term = {
    "jan" : "janvier",
    "fév" : "février",
    "mars" : "mars",
    "avr" : "avril",
    "mai" : "mai",
    "juin" : "juin",
    "juil" : "juillet",
    "août" : "août",
    "aouˆt" : "aouˆt",
    "sept" : "septembre",
    "oct" : "octobre",
    "nov" : "novembre",
    "déc" : "décembre"
}

french_month_map = {
    "jan" : "JAN",
    "fév" : "FEB",
    "fe´vrier": "FEB",
    "mars" : "MAR",
    "avr" : "APR",
    "mai" : "MAY",
    "juin" : "JUN",
    "juil" : "JUL",
    "août" : "AUG",
    "aouˆt" : "AUG",
    "aodt" : "AUG",
    "sept" : "SEP",
    "oct" : "OCT",
    "nov" : "NOV",
    "déc" : "DEC"
}


# data = {
#     "Issue" : "Canada Mortgage and Housing Corporation",	
#     "FirstPaymentDate" : None,
#     "IssueDate" : None,
#     "MaturityDate" : None,
#     "SettlementDate" : None,
#     "FinalPaymentDate" : None,
#     "Coupon" : None,
#     "CouponFrequency" : "Monthly",
#     "TotalIssuance" : None,
#     "Outstanding" : None,
#     "IssueCurrency" : "CAD",
#     "CouponType" : None,
#     "BondType" : None,
#     "DocExtracted" : "3"
# }

def get_unique_date_format(map_date, date_format_string, language = None):
    year_pattern = r"\d{4}"
    match = re.search(year_pattern, date_format_string)
    year = match.group()
    result = ""
    date_without_year = date_format_string.replace(year, "")
    
    found = False
    for key  in map_date:
        if key.lower() in date_format_string:
            for month in range(31,-1, -1):
                if str(month) in date_without_year:
                    if language == "French":
                        date_str = french_month_map.get(key).upper() + str("-") + str(month) + str("-") + str(year)
                        result = datetime.strptime(date_str, "%b-%d-%Y")
                        found = True  
                        break                        
                    else:    
                        date_str = key.upper() + str("-") + str(month) + str("-") + str(year)
                        result = datetime.strptime(date_str, "%b-%d-%Y")
                        found = True  
                        break
        if found:
            break         
        
    return result

# yet to add in data dict "FloatingRateBonds" : None,
def extract_value_regex_pattern(pattern, line, keyword,slice_lines=None,end_slice_lines = None):
    #extract value after keyword
    match = None
    end_idx = 999
    if slice_lines:
        if any(keys.lower().replace(" ","") in line.lower() for keys in slice_lines):
            idx = line.lower().find(keyword.lower())
            if end_slice_lines:
                if any((keyword:=keys.lower().replace(" ","")) in line.lower() for keys in end_slice_lines):
                    end_idx = line.lower().find(keyword.lower())                
                    
            line = line[idx:end_idx]
            match = re.search(pattern, line)
            if match:
                return match.group().lower()
        # else:
        #     if end_slice_lines:
        #         if any((keyword:=keys.lower().replace(" ","")) in line.lower() for keys in end_slice_lines):
        #             end_idx = line.lower().find(keyword.lower())                
                    
        #             line = line[:end_idx]
        #             match = re.search(pattern, line)
        #             if match:
        #                 return match.group().lower()
        #             match = re.search(pattern, line)
        #             if match:
        #                 return match.group().lower()
        #             else:
        #                 return match
                    
    #extract value before/after keyword
    else:
        if end_slice_lines:
            if any((keyword:=keys.lower().replace(" ","")) in line.lower() for keys in end_slice_lines):
                end_idx = line.lower().find(keyword.lower())                
                
                line = line[:end_idx]
                match = re.search(pattern, line)
                if match:
                    return match.group().lower()
                match = re.search(pattern, line)
                if match:
                    return match.group().lower()
                else:
                    return match
                
        match = re.search(pattern, line)
        if match:
            return match.group().lower()
        else:
            return match
        
def format_coupon_value(val, ele):
    if "+" in  val:
        val = val.replace("+","").replace("-","")
        val = ele.upper() + "+" + val
        
    elif "-" in  val or "–" in  val or "-" in val:
        val = val.replace("+","").replace("-","")
        val = ele.upper() + "-" + val
    else:
        if ele:
            val = ele.upper() + val
    
    return val


def extract_data(isin, path, txt_path,reschedule = None):    
    data = {
        "Issue" : "Canada Mortgage and Housing Corporation",	
        "FirstPaymentDate" : None,
        "IssueDate" : None,
        "MaturityDate" : None,
        "SettlementDate" : None,
        "FinalPaymentDate" : None,
        "Coupon" : None,
        "CouponFrequency" : "Monthly",
        "TotalIssuance" : None,
        "Outstanding" : None,
        "IssueCurrency" : "CAD",
        "CouponType" : None,
        "BondType" : None,
        "DocExtracted" : "3"
    }

    datepattern = date_pattern(language = "English")
    French_datepattern = date_pattern(language = "French")
    decimalpercentagepattern = decimal_percentage_pattern()
    aggreagatepattern = aggreagate_pattern()
     
    lst_field_search = ["aggregate", "mortgage-backedsecurities","firstpaymentdue","issuedate","maturitydate","settlementdate","finalpaymentdue"]#, "issuedby"
    # monthly_update = ["Semi-annually","Semi-anteach","Semtannually","Monthly","Sem-annually"]
    
    #slice words --> first payment , maturity, finalpayment
    #KEYWORDS TO SEARCH
    aggregate_lst = ["aggregate", "montant"]
    extract_mortgage = ["mortgage-backed securities","moltgage-backed securities","backed securities", "titres hypothécaires", "titres hypothe´","titres hypothécaires"]
    coupon_slice_lines = ["mortgage","moltgage","backed securities", "titres"]
    french_slice_lines = ["DATE DU PREMIER PAIEMENT","DATE D’ÉCHÉANCE","DATE D'ÉCHÉANCE","DATE D'ECHEANCE","DATE DU DERNIER PAIEMENT","E´CHE´ANCE DU 1er PAIEMENT","E´CHE´ANCE DU DERNIER PAIEMENT","DATE D’E´CHE´ANCE DES TITRES"]          
    english_slice_lines = ["first payment due","first payment date","maturity date","final payment due","final payment date","final payment' due"]
    first_payment_due_keywords = ["first payment due","first payment date","DATE DU PREMIER PAIEMENT","E´CHE´ANCE DU 1er PAIEMENT"]
    issue_date_keywords = ["issue date","DATE D’ÉMISSION","DATE D’E´MISSION","DATE D'ÉMISSION","DATE D'EMISSION"]
    maturity_date_keywords = ["maturity date","DATE D’ÉCHÉANCE","DATE D’E´CHE´ANCE DES TITRES","DATE D'ÉCHÉANCE","DATE D'ECHEANCE"]
    settlement_date_keywords = ["settlement date","DATE DE RÈGLEMENT","DATE DE RE`GLEMENT","DATE DE REGLEMENT"]
    final_payment_due_keywords = ["final payment due","final payment date","final payment' due","DATE DU DERNIER PAIEMENT","E´CHE´ANCE DU DERNIER PAIEMENT"]
    
    english_issue_date_keywords = ["issuedate"]
    english_settlement_date_keywords = ["settlementdate"]    

    english_maturity_date_keywords = ["maturitydate"]
    english_final_payment_due_keywords = ["finalpaymentdue","finalpaymentdate","finalpayment'due"]#"FINAL PAYMENT' DUE"

    english_first_payment_due_keywords = ["firstpaymentdue","firstpaymentdate"]

    # end_slice_lines = maturity_date_keywords + final_payment_due_keywords
    
    fields_to_remove = lst_field_search.copy()
    print (isin)    
    page_img = convert_from_path(path,dpi=500,first_page = 1, last_page= 2)

    #cond: if data["DocExtracted"] = "9" in database
    # based on above condition says: Manuall entry requires
    if reschedule:
        data["DocExtracted"] = "9"
        
    if len(page_img) >= 1:
        #search in first else second page 
        flag1 = True
        flag2 = True        
        bondtype = misc.extract_bondtype_coupontype_checkbox_TPICAP_CAD_version2(page_img[0], filename =isin)

        if bondtype[0]['value'] and bondtype[0] != 'None':
            data["BondType"] = bondtype[0]['value']
            flag1 = False     
        if bondtype[1]['value']  and bondtype[1] != 'None':
            data["CouponType"] = bondtype[1]['value']
            flag2 = False
        
        
        if flag1 or flag2:
            if len(page_img) >=2:
                bondtype = misc.extract_bondtype_coupontype_checkbox_TPICAP_CAD_version2(page_img[1],filename= isin)
                if flag1:
                    if bondtype[0]['value'] and bondtype[0] != 'None':
                        data["BondType"] = bondtype[0]['value']
                if flag2:
                    if bondtype[1]['value']  and bondtype[1] != 'None':
                        data["CouponType"] = bondtype[1]['value']
        
    
    content = ""
    if not os.path.exists(txt_path):
        content  = convert_to_text(path)[0]
        constants.write_path(txt_path, content)
    
    elif os.path.exists(txt_path) and os.path.getsize(txt_path) == 0:
        content  = convert_to_text(path)[0]
        constants.write_path(txt_path, content)
        
    if os.path.exists(txt_path) and os.path.getsize(txt_path) > 10:
        if content:
            content = constants.read_content(txt_path)
            
        for idx, line in enumerate(content[:100]):
            print(line,idx)
            raw_idx = idx
            line = re.sub(r'\s+',' ', line)
            cond_line = line.lower().replace(" ", "")
            #text conversion illegal formating to readable format
            cond_line = cond_line.replace(",,",",")
            cond_line = cond_line.replace("*","")
            cond_line = cond_line.replace("I*\"","1").replace("I°\"","1")
            cond_line = cond_line.replace("|","1")
                        
            if fields_to_remove:
                lst_field_search = fields_to_remove.copy()
                for field in lst_field_search:
                    
                    flag =  False
                    if field == "aggregate" and any((keyword:= keys) for keys in aggregate_lst if keys.lower().replace(" ","") in cond_line):
                        pattern = aggreagatepattern                    
                        if pattern:
                            amount_value = None
                            #iterating reverse from captured line to till first line 
                            for i in range(idx, -1, -1):
                                if match:= re.search(pattern, content[i]):
                                    amount_value = match.group().replace(" ","")
                                    # if amount_value.count(",") == 1:
                                    #     amount_value = amount_value.replace(",", ".")
                                    if "$" in amount_value:
                                        amount_value = "$" + amount_value.replace("$","")
                                    
                                    data["TotalIssuance"] = amount_value  
                                    data["Outstanding"] = amount_value
                                    fields_to_remove.remove(field)
                                    flag = True
                                    break
                            
                            if not amount_value:
                                for i in range(idx, -1, -1):
                                    pattern = r"(\s*\d[\d, .]*\d\b)"
                                    if match:= re.search(pattern, content[i]):
                                        amount_value = match.group().replace(" ","")
                                        if len(amount_value) > 6:
                                            amount_value = "$" + amount_value
                                            data["TotalIssuance"] = amount_value  
                                            data["Outstanding"] = amount_value
                                            break
                                
                    if flag:
                        break

                    elif field == "mortgage-backedsecurities" and any((keyword:= keys.lower().replace(" ", "")) for keys in extract_mortgage if keys.lower().replace(" ","") in cond_line):
                        filter_line = ""
                        if any((identifier:= key.lower().replace(" ","")) in keyword for key in coupon_slice_lines):
                            idx = line.lower().find(identifier)
                            filter_line = line.lower()[:idx]
                        else:
                            filter_line = line

                        cond_line = cond_line.replace("/","").replace("\\","")
            
                        val = extract_value_regex_pattern(decimalpercentagepattern, cond_line, keyword)
                        if val:
                            val = val.replace(" ", "").replace(",",".")
                            lst = ["cdor", "wac", "corra"]
                            if not any(ele for ele in lst if ele in cond_line):
                                data["CouponType"] = "Fixed"   
                                data["BondType"] =   "Fixed"                                             

                            else:
                                for ele in lst:
                                    if ele in cond_line:                                   
                                    # if not data["CouponType"]:                                        
                                        if ele == "corra" or ele == "cdor": 
                                            data["CouponType"] = "Floating"   
                                            data["BondType"] =   "Floating"
                                            val = format_coupon_value(val, ele) 
                                            break 
                                        
                                        elif ele == "wac":     
                                            data["CouponType"] = "Variable"   
                                            data["BondType"] =   "Variable"
                                            val = format_coupon_value(val, ele)
                                            break
                                                                                                                                                                                                                                                                            
                            data["Coupon"] = val 
                            fields_to_remove.remove(field)
                            break
                                 
                    elif field == "firstpaymentdue" and any((keyword := key.lower().replace(" ", "")) in cond_line for key in first_payment_due_keywords):
                        print(keyword)
                        #english keywords: english pattern
                        extracted_date = extract_value_regex_pattern(datepattern, cond_line, keyword,english_slice_lines)
                        if extracted_date:
                            data["FirstPaymentDate"] = get_unique_date_format(map_date,extracted_date)
                            
                        else:
                            #english keywords: frenchpattern
                            if keyword in english_first_payment_due_keywords:
                                extracted_date = extract_value_regex_pattern(French_datepattern, cond_line, keyword,english_slice_lines)
                                if extracted_date:
                                    data["FirstPaymentDate"] = get_unique_date_format(french_month_map,extracted_date,language = "French")
                            #french keywords: frenchpattern
                            else:    
                                extracted_date = extract_value_regex_pattern(French_datepattern, cond_line, keyword,french_slice_lines)
                                if extracted_date:
                                    date = get_unique_date_format(french_month_map,extracted_date,language = "French")
                                    data["FirstPaymentDate"] = date 

                        fields_to_remove.remove(field)
                        break                            
                                         
                    else:
                        if field == "issuedate" and any((keyword := key.lower().replace(" ", "")) in cond_line for key in issue_date_keywords):
                            print(keyword)
                            #english keywords: english pattern 
                            #tell where line should start and end through end_slice_lines
                            extracted_date = extract_value_regex_pattern(datepattern, cond_line, keyword,english_slice_lines,end_slice_lines=maturity_date_keywords)
                            if extracted_date:
                                data["IssueDate"] = get_unique_date_format(map_date,extracted_date)
                                
                            else:
                                #english keywords: frenchpattern
                                if keyword in english_issue_date_keywords:
                                    extracted_date = extract_value_regex_pattern(French_datepattern, cond_line, keyword,english_slice_lines,end_slice_lines=maturity_date_keywords)
                                    if extracted_date:
                                        data["IssueDate"] = get_unique_date_format(french_month_map,extracted_date,language = "French")
                                #french keywords: frenchpattern
                                else:
                                    extracted_date = extract_value_regex_pattern(French_datepattern, cond_line, keyword,french_slice_lines,end_slice_lines=maturity_date_keywords)
                                    if extracted_date:
                                        date = get_unique_date_format(french_month_map,extracted_date,language = "French")
                                        data["IssueDate"] = date
                                                            
                            fields_to_remove.remove(field)  
                            continue                    
                            
                        elif field == "maturitydate" and any((keyword := key.lower().replace(" ", "")) in cond_line for key in maturity_date_keywords):
                            print(keyword)
                            #english keywords: english pattern
                            extracted_date = extract_value_regex_pattern(datepattern, cond_line, keyword,english_slice_lines)
                            if extracted_date:
                                data["MaturityDate"] = get_unique_date_format(map_date,extracted_date)
                            else:
                                #english keywords: frenchpattern
                                if keyword in english_maturity_date_keywords:
                                    extracted_date = extract_value_regex_pattern(French_datepattern, cond_line, keyword,english_slice_lines)
                                    if extracted_date:
                                        date = get_unique_date_format(french_month_map,extracted_date,language = "French")
                                        data["MaturityDate"] = date
                                    
                                #french keywords: frenchpattern
                                else:
                                    extracted_date = extract_value_regex_pattern(French_datepattern, cond_line, keyword,french_slice_lines)
                                    if extracted_date:
                                        date = get_unique_date_format(french_month_map,extracted_date,language = "French")
                                        data["MaturityDate"] = date
                            
                                
                            fields_to_remove.remove(field)
                            break
                            
                        elif field == "settlementdate" and any((keyword := key.lower().replace(" ", "")) in cond_line for key in settlement_date_keywords):
                            print(keyword, cond_line)
                            #english keywords: english pattern
                            extracted_date = extract_value_regex_pattern(datepattern, cond_line, keyword,english_slice_lines,end_slice_lines=final_payment_due_keywords) 
                            #end_slice_lines=final_payment_due_keywords
                            if extracted_date:
                                data["SettlementDate"] = get_unique_date_format(map_date,extracted_date)
                                    
                            else:
                                #english keywords: frenchpattern
                                if keyword in english_settlement_date_keywords: 
                                    extracted_date = extract_value_regex_pattern(French_datepattern, cond_line, english_slice_lines,end_slice_lines=final_payment_due_keywords)
                                    if extracted_date:
                                        date = get_unique_date_format(french_month_map,extracted_date,language = "French")
                                        data["SettlementDate"] = date
                                #french keywords: frenchpattern
                                else:
                                    extracted_date = extract_value_regex_pattern(French_datepattern, cond_line, french_slice_lines,end_slice_lines=final_payment_due_keywords)
                                    if extracted_date:
                                        date = get_unique_date_format(french_month_map,extracted_date,language = "French")
                                        data["SettlementDate"] = date

                            fields_to_remove.remove(field)
                            continue

                        elif field == "finalpaymentdue" and any((keyword := key.lower().replace(" ", "")) in cond_line for key in final_payment_due_keywords):
                            print(keyword)
                            #english keywords: english pattern
                            extracted_date = extract_value_regex_pattern(datepattern, cond_line, keyword,english_slice_lines)
                            if extracted_date:
                                data["FinalPaymentDate"] = get_unique_date_format(map_date,extracted_date)
                                
                            else:
                                #english keywords: frenchpattern
                                if keyword in english_final_payment_due_keywords:
                                    extracted_date = extract_value_regex_pattern(French_datepattern, cond_line, keyword,english_slice_lines)
                                    if extracted_date:
                                        date = get_unique_date_format(french_month_map,extracted_date,language = "French")
                                        data["FinalPaymentDate"] = date
                                #french keywords: frenchpattern                                
                                else: 
                                    extracted_date = extract_value_regex_pattern(French_datepattern, cond_line, keyword,french_slice_lines)
                                    if extracted_date:
                                        date = get_unique_date_format(french_month_map,extracted_date,language = "French")
                                        data["FinalPaymentDate"] = date
                                    
                                    
                            fields_to_remove.remove(field)
                            break
                    
            else:
                break
            
    update_query(data, isin,reschedule=None)
    
# Define a custom exception for timeouts
class TimeoutException(Exception):
    pass

@contextmanager
def timeout(seconds):
    def signal_handler(signum, frame):
        raise TimeoutException("Script execution timed out")
    
    # Set the signal handler for SIGALRM
    signal.signal(signal.SIGALRM, signal_handler)
    # Schedule an alarm signal after 'seconds'
    signal.alarm(seconds)
    try:
        yield
    finally:
        # Cancel the alarm
        signal.alarm(0)
        
def call_extraction(isin,path, txt_path,reschedule):
    try:
        with timeout(seconds=1000):
            extract_data(isin, path, txt_path,reschedule=reschedule)                
    except TimeoutException:
        print(f"Timeout Error: {isin}")
        #status='5' flag error in db
        error_update_query(isin, status='9',reschedule=reschedule)
        pass
    except Exception as e:
        try:
            with timeout(seconds=1000):
                #converting again coz some files are converted through php which is not in correct txt format
                content  = convert_to_text(path)[0]
                constants.write_path(txt_path, content)
                extract_data(isin, path, txt_path,reschedule=reschedule)
                print(f"Error but converted : {isin}: {e}")
        
        except TimeoutException:
            print(f"Timeout Error: {isin}")
            #status='5' flag error in db
            error_update_query(isin, status='9',reschedule=reschedule)
        
        except Exception as e:
            print(f"conversion Error: {isin}: {e}")
            #status='5' flag error in db
            error_update_query(isin, status='5',reschedule=reschedule)


def save_pdf_from_link(url, file_name):
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_name, 'wb') as f:
            f.write(response.content)
        print(f"PDF saved successfully as '{file_name}'")
    else:
        print(f"Failed to save PDF from '{url}'")

def load_files(filenames,reschedule=None):    
    # with ProcessPoolExecutor() as executor:
    #     for row in tqdm(filenames):
    #         isin = row['ISIN']
    #         path = constants.getPDFDocumentPath(isin)
    #         txt_path =  constants.getTXTDocumentPath(isin)
            
    #         if not os.path.exists(path):
    #             try:
    #                 url = row['FileLink']  
    #                 save_pdf_from_link(url, path)
    #             except:
    #                 pass                
    #         if os.path.exists(path):
    #             executor.submit(call_extraction,isin,path, txt_path,reschedule)
    #         else:
    #             #status='4' flag pdf path is not found
    #             error_update_query(isin, status='4',reschedule=reschedule)
    #         # break    

    for row in tqdm(filenames):
        isin = row['ISIN']
        path = constants.getPDFDocumentPath(isin)
        txt_path =  constants.getTXTDocumentPath(isin)
        
        try:
            if not os.path.exists(path):
                url = row['FileLink']
                if url != "#":
                    save_pdf_from_link(url, path)                
            if os.path.exists(path):
                call_extraction(isin,path, txt_path,reschedule)
            else:
                #status='4' flag pdf path is not found
                error_update_query(isin, status='4',reschedule=reschedule)
            # break    
        except:
            pass

def error_update_query(isin, status = '5',reschedule=None):
    if reschedule:
        status = '9'
    query = f"UPDATE ISIN.dbo.Data_cmhc SET docextracted = '{status}',extracted_date = GetDate() where ISIN = '{isin}';"
    print(query)
    FERACK34.query(query)
    FERACK54.query(query)

def format_value(val):
    if val:
        return f"'{val}'"
    else:
        return "null"
                                                
def update_query(data, isin,reschedule=None):
    if reschedule:
        data['DocExtracted'] = '9'    
    # Constructing the SET clause of the SQL query
    prev_data_len = 14
    data_len = sum([1 for key, value in data.items() if value is not None])
    
    if prev_data_len == data_len:
        data['DocExtracted'] = '6'
    # set_clause = ", ".join([f"{key} = '{value}'" for key, value in data.items() if value is not None])
    set_clause = ", ".join([f"{key} = {format_value(value)}" for key, value in data.items()])
    
    print(f"{isin}: {data}")
    # Constructing the UPDATE query
    query = f"UPDATE ISIN.dbo.Data_cmhc SET {set_clause},extracted_date = GetDate() where ISIN = '{isin}';"
    print(query)
    FERACK34.query(query)
    FERACK54.query(query)
    check_maturity_issue_date_equal(data,isin)

def check_maturity_issue_date_equal(data,isin):  
    if data["IssueDate"] ==  data["MaturityDate"]:
        query = f"UPDATE ISIN.dbo.Data_cmhc SET DocExtracted = '9' where ISIN = '{isin}' "
        FERACK34.query(query)
        FERACK54.query(query)
                
def date_pattern(language = "English"):
    if language == "English":
        # date_pattern = r"(\d{1,4}[-\/]\d{1,2}[-\/]\d{2,4})|((?:January|Januaryy|Janaury|Janauryy|February|Februaryy|March|Mareh|April|Appril|Mayy|June|July|Julyy|August|September|Sepptember|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|jan|feb|mar|apr|may|mayy|jun|jul|aug|sep|oct|nov|dec)[\s.,-]*\d{1,2}?(?:st|nd|rd|th)?[\s.,-]*\d{4})|(\d{1,2}[\s.,-]*(?:January|Janaury|Janauryy|February|Februaryy|March|Mareh|April|Appril|May|Mayy|June|July|Julyy|August|September|Sepptember|October|November|December|Jan|Feb|Mar|Apr|May|Mayy|Jun|Jul|Aug|Sep|Oct|Nov|Dec|jan|feb|mar|apr|may|mayy|jun|jul|aug|sep|oct|nov|dec)[\s.,-]*\d{4})"
        date_pattern = r"(\d{1,4}[-\/]\d{1,2}[-\/]\d{2,4})|((?:January|Januaryy|Janaury|Janauryy|February|Februaryy|March|Mareh|April|Appril|Mayy|June|July|Julyy|August|Auggust|September|Sepptember|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|jan|feb|mar|apr|may|mayy|jun|jul|aug|sep|oct|nov|dec)[\s.,-]*\d{1,2}?(?:st|nd|rd|th)?[\s.,-]*\d{4})|(\d{1,2}[\s.,-]*(?:January|Janaury|Janauryy|February|Februaryy|March|Mareh|April|Appril|May|Mayy|June|July|Julyy|August|Auggust|September|Sepptember|October|November|December|Jan|Feb|Mar|Apr|May|Mayy|Jun|Jul|Aug|Sep|Oct|Nov|Dec|jan|feb|mar|apr|may|mayy|jun|jul|aug|sep|oct|nov|dec)[\s.,-]*\d{4})"
        return date_pattern.lower()
    
    elif language == "French":
        date_pattern = r"(\d{1,4}[-\/]\d{1,2}[-\/]\d{2,4})|((?:janvier|février|fe´vrier|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|november|décembre|decembre|janv|fév|mars|avr|mai|juin|juil|août|aouˆt|aodt|sept|oct|nov|déc|janvier|février|fe´vrier|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre|janv|fév|mars|avr|mai|juin|juil|août|sept|oct|nov|déc)[\s.,-]*\d{1,2}(?:st|nd|rd|th|er)?[\s.,-]*\d{4})|(\d{1,2}[\s.,-]*(?:st|nd|rd|th|er)?\s?(?:janvier|février|fe´vrier|mars|avril|mai|juin|juillet|août|aouˆt|aodt|septembre|octobre|novembre|november|décembre|decembre|janv|fév|mars|avr|mai|juin|juil|août|sept|oct|nov|déc|janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|november|décembre|de´cembre|janv|fév|mars|avr|mai|juin|juil|août|sept|oct|nov|déc)[\s.,-]*\d{4})"
        return date_pattern.lower()

def decimal_percentage_pattern():
    return r"([+|-|–|-]?\s?\d*[\.,]{1}?\d*\s?%)"

def aggreagate_pattern():
    return r"(\$\s*\d[\d, .]*\d\b|\s*\d[\d, .]*\d\s?\$)"


if __name__ == "__main__":    
    file_name = "/home/fedocker/otto/CanDealSecurities/run_app.py"
    if not process_check.check_if_running(file_name):
        sys.exit()
    else:
        query = ''' select * from ISIN.dbo.Data_cmhc where DocExtracted is null'''
        # query = """select * from ferack54.ISIN.DBO.Data_CMHC where isin in 
# ('CA62980ZKN55')"""
        # query = '''
        # select * from ISIN.DBO.Data_CMHC where isin in ('CA62966Z4S01',
        # 'CA13514ZAB00',
        # 'CA62943ZLW00',
        # 'CA62958ZV686',
        # 'CA62958ZW593',
        # 'CA62966Z4Q45',
        # 'CA62966Z4R28',
        # 'CA62966Z4S01',
        # 'CA62966ZP657',
        # 'CA62966ZP731',
        # 'CA62966ZP814',
        # 'CA62966ZP996',
        # 'CA62967ZSH78',
        # 'CA62972ZC463',
        # 'CA62974ZHY30',
        # 'CA62974ZHZ05',
        # 'CA62974ZJD74',
        # 'CA62975ZE256',
        # 'CA62975ZE330',
        # 'CA62975ZE413',
        # 'CA62975ZE587',
        # 'CA62977Z3R00')
        # '''

        # query ='''select * from ISIN.DBO.Data_CMHC where CouponType ='Fixed' and coupon like '%CDOR%' '''
        # query = ''' select * from ISIN.dbo.Data_cmhc where docextracted in ('5','3') and Txt_Reconversion = 'Y' '''
        filenames = load_filenames(query)#[50:]
        print(len(filenames))
        if len(filenames) >=1:
            load_files(filenames, reschedule = None)
        # filenames = [{"ISIN":"CA62920ZDC01"}] #CA62952ZPH78    
        # load_files(filenames, reschedule = None)
        # load_files(filenames, reschdule = True)        