import re 
import re 
import sys
sys.path.append('/home/')
from factentry.otto_ml.src.utils import db_class
FERACK13 = db_class.Database_Manager("FERACK13", "OTTO_MUNI")
from tqdm import tqdm
import logging

filename='/home/dhivakar/callable_option/collect _footer_not_matched_lines.log'
logging.basicConfig(filename=filename, filemode='a', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

logging.basicConfig(filename=filename)

def extractor_footer(line):
    line = line.strip()
    call_option_dict = {"callfooter": None,
                        "calldate": None,
                        "callable": 'N',
                        "callprice": None
    }
    # footer_pattern = r"[**|*|†|C|c||°|©]"
    # footer_pattern = r"[**|*|†||°|©]|(\(c{1,2}\)|\(a{1,2}\))" #© °) °°) “) (1) (2) ( c 2 † + + >  ‘ (1) (2) (3) (4) ¢ ccc cc 
    footer_pattern = r"[**|*|†||°|©|†|+|]|c{2,3}|\(c{1,2}\)|\(a{1,3}\)|\(1\)|\(2\)|\(3\)|\(4\)"
    footer_pattern_bet_sentence = r"%°|%c"
    callprice_pattern = r"\d*%|par"
    

    if matches:= re.search(footer_pattern, line[:4]):
        if matches.group() == "\uf02a": # --> "\uf02a" BUT i WILL CONSIDER AS A '*'
            call_option_dict["callfooter"] = "*"
            call_option_dict["callable"] = "Y"
            return True
        elif matches.group(): # --> "\uf02a" BUT i WILL CONSIDER AS A '*'
            call_option_dict["callfooter"] = matches.group()
            call_option_dict["callable"] = "Y"
            # print(matches.group(), ": ", f"{line}")
            return True
    # if matches:= re.search(footer_pattern_bet_sentence, line,re.IGNORECASE):
    #     call_option_dict["callfooter"] = matches.

    return False


query = "select distinct FileName from OTTO_MUNI.dbo.EMMA_Table_Extraction "
isin_queue = [files['FileName'] for files in FERACK13.fetchArray_withKey(query)]
for basename in tqdm(isin_queue):     
    TXT_FILE = f"/OTTO-Project/FE_Documents_Txt/EMMA_Official_Statement/{basename}.txt"
    # logging.info(TXT_FILE)
    # * Priced first optional redemption date May 15, 2019.
    print(TXT_FILE)
    with open(TXT_FILE, 'r') as file:
        for line in file:
            line = line.replace("\n","").strip()
            # line = "* Yield 10 October 1, 2014."

            if line == "":
                continue
            elif extractor_footer(line):                
                #transform/preprocess lines 
                sliced_line = re.sub(r" at the | to the | the | on | to | of | at | and | is | as | or | are | by | a ", " ", line) #a | 

                call_opt_kword_pattn_with_full_line = r"((?:Priced|Price|Prized) (?:to the optional call|to call|to the call date|to call|to par call|to the first call date |at the stated yield|to|assuming redemption|at stated yield))|((?:Yield to|Y ield to) (?:call at|first call date|call date|a par call|par call|first call date|first optional redemption date|the first optional redemption|Optional Redemption|date of earliest optional redemption|assuming optional redemption|first optional par call date|earliest call date|first par call))|(?:Yield (?:and price|calculated to the first optional call|calculated to first optional call|shown is to first optional redemption date|calculated to|shown to first optional redemption date|is calculated to|calculated assuming redemption|assuming redemption|on the Two Hundred Fortieth Series|to the call date of|computed to earliest redemption date|to call on|computed to the first optional redemption date))|(first optional call|calculated to first optional call|calculated to the first optional redemption date|resized from original par amount|Pricing assumes redemption|Price and yield information provided by the underwriters|Yield to Call|Yield as of first optional redemption date|Yields calculated to|The yields on these maturities are calculated|Callable premium bond|Yields shown are to the first optional redemption date|Yield calculated to the first optional redemption date|Subject to redemption prior to maturity|Calculated to the first call date|Yield shown to first call date|Yield to Purchase Date|Yield to optional par call)" 

                call_opt_kword_pattn_with_sliced_line = r"((?:Priced|Price|Prized|Yield|Y ield)(?: optional call| call| first call| call date| par call| stated yield| assuming redemption| call date| par call| first call date| first optional redemption| Optional Redemption| first optional redemption date| date earliest optional redemption| assuming optional redemption| reflects pricing first optional redemption date| shown first optional redemption date| shown optional redemption| first optional par redemption date| based pricing first optional redemption| first optional par call date| earliest call date| first par call| price| calculated first optional call| calculated| date earliest optional redemption| calculated assuming redemption| assuming redemption| Two Hundred Fortieth Series| computed earliest redemption date| earliest redemption date| earliest optional redemption date| computed first optional redemption | calculated based| based pricing))|(first optional call|calculated first optional call|calculated first optional redemption date|resized from original par amount|Pricing assumes redemption|Price yield information provided by underwriters|Yield Call|Yields calculated|yields these maturities calculated|Callable premium bond|Yields shown first optional redemption date|Yield calculated first optional redemption date|Subject redemption prior maturity|Calculated first call date|Yield shown first call date|Yield Purchase Date|Yield optional par call)"
                # Optional Redemption date
                # Y ield to date of earliest optional redemption
                
                # date pattern
                call_date_pattern = r"(\d{1,4}[-\/]\d{1,2}[-\/]\d{2,4})|((?:January|Januaryy|Janaury|Janauryy|February|Februaryy|March|Mareh|April|Appril|Mayy|June|July|Julyy|August|Auggust|September|Sepptember|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|jan|feb|mar|apr|may|mayy|jun|jul|aug|sep|oct|nov|dec)[\s.,-]*\d{1,2}?(?:st|nd|rd|th)?[\s.,-]*\d{4})|(\d{1,2}[\s.,-]*(?:January|Janaury|Janauryy|February|Februaryy|March|Mareh|April|Appril|May|Mayy|June|July|Julyy|August|Auggust|September|Sepptember|October|November|December|Jan|Feb|Mar|Apr|May|Mayy|Jun|Jul|Aug|Sep|Oct|Nov|Dec|jan|feb|mar|apr|may|mayy|jun|jul|aug|sep|oct|nov|dec)[\s.,-]*\d{4})"
                
                if date_patern_match:= re.search(call_date_pattern,line,re.IGNORECASE):
                    date_patern_match = date_patern_match.group()
                
                call_option_keyword_pattern2 = rf"(?:Yield|Yield calculated|Yield first optional par call date|Yield first call date of|Redemption Date —|Bonds maturing after|Priced|Yield par call|Yidd call par|Yield shown|Calculated first optional redemption date|Yield shown|Yield first redemption date|Yield first par call|Priced assuming optional redemption|Yield shown first available call date|Yield/Price|Price Yield first call date|Yield first optional prepayment date|Yield based pricing|Calculated par call date|Yields Prices first optional redemption date|Yields prices calculated first optional prepayment|Priced call date) {date_patern_match}"
        

                if matches:= re.search(call_opt_kword_pattn_with_sliced_line, sliced_line,re.IGNORECASE):
                    # print("MATCHED:    ", line )
                    logging.info(f"{line}")            
                    pass

                elif matches:= re.search(call_opt_kword_pattn_with_full_line.replace(" ",""), line.replace(" ",""),re.IGNORECASE):
                    # print("MATCHED:    ", line )
                    logging.info(f"{line}")            
                    pass
                
                elif matches:= re.search(call_option_keyword_pattern2, sliced_line,re.IGNORECASE) and [re.search(diff_keys, line.lower(),re.IGNORECASE) for diff_keys in ["optional redemption date","call date"]]:#"first optional call date", 
                    print("NEWLY MATCHED:    ", line )
                    logging.info(f"{line}")            
                    # pass
                # elif matches:= re.search(call_option_keyword_pattern2.replace(" ",""), sliced_line.replace(" ",""),re.IGNORECASE) and [re.search(diff_keys, line.lower().replace(" ",""),re.IGNORECASE) for diff_keys in ["optional redemption date","call date","first optional call date"]]:
                #     print("NEWLY MATCHED:    ", line )
                #     logging.info(f"{line}")

                else:
                    # logging.info(TXT_FILE)
                    # logging.info("\n")
                    # logging.info(f"{line}")
                    print("NOT MATCHED:    ", line )
                break    

#NOT MATCHED:     * Yield to January 1, 2030 optional redemption date.
#  *Yield to the September 1, 2013 call date
#* Yield to August 15, 2029 first optional call date.


#NOT MATCHED:     † Yield calculated to October 1, 2030, the first optional redemption date.


#NOT MATCHED:     *The Series 2023 Bonds maturing September 1, 2030 and thereafter are subject to redemption


# 2024-11-11 18:11:24,836 - INFO - † Yield to January 1, 2031 call at 100%

#c = priced to first optional redemption date of May 15, 2023


# YieldtofirstparcalldateofAugust1,2021



"""
*Callable on or after September 1, 2022 at Par. See “REDEMPTION - Optional Redemption” herein.

*Redemption Date — May 1, 2018; Redemption Price at Par.

*Yield to Optional Redemption Date: March 1, 2028CLEARFIELD AREA SCHOOL DISTRICT
Yield toJuly 1, 2027, first optional redemption date

© Yield calculated based on the assumption that the Bonds denoted and sold at a premium will be redeemed on August 15, 2032, the



write functionality
-------------------

$22,880,000 5.00% Term Bonds, Due October 1, 2039 Price: 103.119% Yield:4.61%* CUSIP 482065BB4
*Callable premium bond. Yield shown is yield to the lowest yielding call date.



CUSIP* 91412GUG8 $39,250,000 4.250% Term Bond due May 15, 2039 Price: 100% 
CUSIP* 91412GUR4 $87,500,000 5.000% Term Bond due May 15, 2044 Yield: 4.110%** 
CUSIP* 91412GUM5 $91,305,000 5.250% Term Bond due May 15, 2044 Yield: 4.010%** 
CUSIP* 91412GUN3 $23,810,000 5.000% Term Bond due May 15, 2049 Yield: 4.210%**

"""


# call_option_keyword_pattern = r"((?:Priced|Price|Prized)(?: optional call| call| call date| par call| first call date | stated yield| to| assuming redemption| at stated yield))|((?:Yield to| Y ield to)(?: call at| first call date| call date| par call| par call| first call date| first optional redemption date| first optional redemption| Optional Redemption| date earliest optional redemption| assuming optional redemption| first optional par call date| earliest call date| first par call))|(?:Yield(?: price| calculated first optional call| calculated first optional call| shown first optional redemption date| calculated to| calculated to| calculated assuming redemption| assuming redemption| Two Hundred Fortieth Series| call date of| computed earliest redemption date| call| computed first optional redemption date))|(first optional call|calculated first optional call|calculated first optional redemption date|resized from original par amount|Pricing assumes redemption|Price yield information provided by underwriters|Yield Call|Yield first optional redemption date|Yields calculated to|yields these maturities are calculated|Callable premium bond|Yields shown are first optional redemption date|Yield calculated first optional redemption date|Subject redemption prior maturity|Calculated first call date|Yield shown first call date|Yield Purchase Date|Yield optional par call)"