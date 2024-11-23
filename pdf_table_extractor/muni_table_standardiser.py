import itertools
import re
import string
import numpy as np
from pandas import DataFrame
from dateutil import parser
from rapidfuzz import fuzz
from rapidfuzz import utils as fuzzutils

COLUMN_DTYPE_MAP = {
    "and": ["ANY"],
    "cusip": ["CUSIP", "CUSIP_PART"],
    "date": ["YEAR", "DATE", "SENTENCE"],
    "maturity": ["YEAR", "DATE", "SENTENCE"],
    "rate": ["RATE", "NUMBER"],
    "interest_rate": ["RATE", "NUMBER"],
    "yield": ["RATE", "NUMBER"],
    "initial offering": ["RATE", "NUMBER"],
    "offering": ["RATE", "NUMBER"],
    "amount": ["CURRENCY"],
    "price": ["RATE"],
    "nan": [None],
    "null": [None],
    "none": [None]
}

COLUMN_NAME_MAP = {
    "maturity": ["due", "maturity", "stated maturity", "maturity date", "maturing","year"] + [m[0].lower() for m in parser.parserinfo.MONTHS],
    "amount": ["principal amount", "amount", "principal"],
    "interest_rate": ["interest rate", "rate", "interest"],
    "yield": ["yield", "initial offering"],
    "cusip": ["cusip", "cusip issue number", "cusip base no","number","base","suffix","suffix no.","csp"],#"CUSIP No."|"Cusip Num*"|"CUSIP"|"CUSIP**"|"Number"|(\(\d{6}\))|(\d{6})|\d{6}
    "price": ["price"],
    "redemption_date": ["redemption date"]  
}

#Unnamed
for k in COLUMN_NAME_MAP:
    COLUMN_NAME_MAP[k].sort(reverse=True)

def infer_text_pattern(str):
    str_pattern=[]
    literal_identifier_map = {
        "-": "DASH",
        "$": "DOLLAR",
        "%": "PERCENT",
        ",": "COMMA",
        ".": "DOT",
    }
    for c in str:
        if c.isdigit():
            str_pattern.append("DIGIT")
        elif c.isspace():
            str_pattern.append("SPACE")
        elif c.isalpha():
            str_pattern.append("CHAR")
        elif c in literal_identifier_map:
            str_pattern.append(literal_identifier_map[c])
        else:
            str_pattern.append("SPLCHAR")
    return str_pattern


def can_be_currency(str):
    if str[0] == "$":
        return True
    elif re.sub(r'[,\.\*]', '', str).isnumeric() and len(str) > 3:
        return True
    return False

def try_parse_date(str):
    if len(str) < 4:
        return None
    try:
        # Reserve date separators and whatever symbols that are importanat for figuring out other types. e.g. % for  rate, $ for amount etc.
        return parser.parse(str.translate(str.maketrans('', '', (string.punctuation + string.whitespace).translate(str.maketrans('', '', ''.join(['.', ',', '/', '-', ':', '%', '$']))))))
    except:
        return None

def can_be_rate(str):
    return re.sub(r'[%\*\.]', '', str).isnumeric()

def can_be_cusip_part(str):
    str = re.sub(r'[\@\* ]', '', str)
    return len(str) == 3 and str[-1].isdigit()

def can_be_cusip(str):
    str = re.sub(r'[\@\* ]', '', str)
    return len(str) == 9 and can_be_cusip_part(str[-3:])

def infer_type(str):
    str = str.encode('ascii', errors='ignore').decode() # Filter out non-ascii characters for now
    if not str or str.lower() == "nan":
        return "NAN"
    elif str.replace('.', '', 1).removeprefix('-').isnumeric():
        if int(float(str)) > 1900 and int(float(str)) < 3000:
            return "YEAR"
        return "NUMBER"
    elif str.isalpha():
        return "STRING"
    elif str.isascii():
        if len(str.translate(str.maketrans('', '', string.punctuation + string.whitespace))) > 15:
            return "SENTENCE"
        elif can_be_currency(str):
            return "CURRENCY"
        elif try_parse_date(str):
            return "DATE"
        elif can_be_rate(str):
            return "RATE"
        elif can_be_cusip(str):
            return "CUSIP"
        elif can_be_cusip_part(str):
            return "CUSIP_PART"
        
    return " ".join(infer_text_pattern(str))

def merge_and_arrange_cell_values(df: DataFrame):
    """
    NOTE: Please standardise the headers before calling this function. Being a best-effort solution,
    the more standard header you provide, more consistent result you get
    """
    # If the column name is not under pre-defined list, do not do anything
    if not any([coln in map(str.lower, df.columns.values) for coln in list(COLUMN_DTYPE_MAP.keys()) + ["nan", "unnamed", "none"]]):
        return df
    
    for row_idx, vals in df.iterrows():
        # print(vals)
        # cleaned_vals = list(map(lambda x: x if str(x).lower().strip() not in ["nan", "none", "null"] else "", list(vals)))
        cleaned_vals = list(map(lambda x: x if str(x).lower() not in ["nan", "none", "null"] else "", list(vals)))
        inferred_type_list = [infer_type(e) for e in cleaned_vals]
        expected_type_list = [COLUMN_DTYPE_MAP.get(colname.lower(), ["ANY"]) for colname in df.columns.values]

        # print("row_idx %s" % row_idx)
        # print("cleaned_vals %s" % cleaned_vals)
        # print("Expected type list %s " % expected_type_list)
        # print("Inferred type list %s " % inferred_type_list)
        freezed_columns = []
        missmatch_cols = {}

        for col_idx, (exp_type, inferred_type) in enumerate(zip(expected_type_list, inferred_type_list)):
            if exp_type == [None]:
                missmatch_cols[col_idx] = "U"
            elif exp_type == ["ANY"]:
                freezed_columns.append(col_idx)
                print("WARN: Found column type of ANY. Considering as freezed column")
            elif inferred_type in expected_type_list[col_idx]:
                freezed_columns.append(col_idx)
            elif inferred_type not in expected_type_list[col_idx]:
                missmatch_cols[col_idx] = "M"
        
        # print("Mismatched cols: %s" % missmatch_cols)
        
        start_idx = -1
        end_idx = -1
        last_mismatch_idx = -1
        merged_column_contents = {}
        merge_region = {}
        for col_idx, miss_type in missmatch_cols.items():
            if miss_type == "U":
                if start_idx == -1:
                    start_idx = col_idx
                if end_idx == -1:
                    mismatch_col_type = missmatch_cols.get(min(col_idx + 1, len(missmatch_cols) - 1), "F")
                    if mismatch_col_type == "M" or mismatch_col_type == "F":
                        end_idx = col_idx
            elif miss_type == "M":
                # If prev is not a unnamed column, do not consider for merging with this mismatched type column
                if col_idx - 1 != end_idx:
                    end_idx = -1
                    break
                merged_column_contents[col_idx] = " ".join(cleaned_vals[start_idx:end_idx+1] + [cleaned_vals[col_idx]])
                merge_region[col_idx] = (start_idx, end_idx)
                start_idx = -1
                end_idx = -1
                last_mismatch_idx = col_idx

        # print("merge_region %s" % merge_region)
        # print("last mismatched col %s" % last_mismatch_idx)
        # No match found on left side. Merge <- direction
        if end_idx == -1 and last_mismatch_idx != -1:
            unnamed_col_list = [k for k, v in missmatch_cols.items() if "U" in v]
            if unnamed_col_list:
                merged_column_contents[last_mismatch_idx] = (merged_column_contents[last_mismatch_idx] + " " + " ".join(cleaned_vals[start_idx:unnamed_col_list[-1] + 1])).strip()
                merge_region[last_mismatch_idx] = (merge_region[last_mismatch_idx][0], unnamed_col_list[-1])

        # print("expected type %s" % expected_type_list)
        # print("inferred type %s" % inferred_type_list)
        # print("merge_region %s" % merge_region)
        # print("merged_column_contents %s" % merged_column_contents)
        # print("freezed_columns %s" % freezed_columns)
        for col_idx, content in merged_column_contents.items():
            if infer_type(content) in expected_type_list[col_idx]:
                replacement_range = merge_region.get(col_idx, None)
                # print(df.iloc[row_idx - 1])
                if replacement_range:
                    df.iloc[row_idx - 1, replacement_range[0]:replacement_range[1] + 1] = np.empty((abs(replacement_range[0] - replacement_range[1]) + 1), dtype=object)
                df.iloc[row_idx - 1, col_idx] = content
                # print(df.iloc[row_idx - 1])
                print("!!!! Merge success !!!!")
    
    # Drop all empty columns
    df.dropna(axis='columns', how='all', inplace=True)

    return df


def standardise_column_names(df: DataFrame):
    std_name_map = {}
    for col_idx, name in enumerate(df.columns.values):
        if len(fuzzutils.default_process(name)) > 40:
            break
        processed_name = fuzzutils.default_process(name)
        for std_name, kwlist in COLUMN_NAME_MAP.items(): 
            try:
                matched_kw = next(kw for kw in kwlist if kw in processed_name)
                if abs(len(processed_name) - len(matched_kw)) < 15:
                    std_name_map[name] = std_name
                    break
            except StopIteration:
                # No match is found
                pass
            
            for kw in kwlist:
                if fuzz.WRatio(name, kw, processor=fuzzutils.default_process, score_cutoff=86.0):
                    std_name_map[name] = std_name
                    break
    
    if std_name_map:
        df.rename(columns=std_name_map, inplace=True)
    return df

def complete_cusips_from_header(df: DataFrame):
    possible_cusip_columns = [x for x in df.columns.values if "cusip" in x.lower()]
    if len(possible_cusip_columns) != 1:
        return df

    p = re.compile(r"\d{6}")
    result = p.search(possible_cusip_columns[0])
    if not result:
        return df
    
    cusip_prefix = result.group(0)
    if len(cusip_prefix + df[possible_cusip_columns[0]].astype(str)) <= 9:
        df[possible_cusip_columns[0]] = cusip_prefix + df[possible_cusip_columns[0]].astype(str)
    else:
        df[possible_cusip_columns[0]] = df[possible_cusip_columns[0]].astype(str)
    return df


def complete_dates_from_header(df: DataFrame):
    months = [m[0].lower() for m in parser.parserinfo.MONTHS]
    possible_maturity_columns = [x for x in df.columns.values if "maturity" in x.lower() or any(substring.lower() in x.lower() for substring in months)]
    if len(possible_maturity_columns) != 1:
        return df
    

    try:
        possible_date, deleted_elems = parser.parse(possible_maturity_columns[0], fuzzy_with_tokens=True)
        if possible_date:
            # It should have at least consumed 4 characters
            if abs(len(possible_maturity_columns[0]) - sum(list(map(len, deleted_elems)))) > 4:
                df[possible_maturity_columns[0]] = possible_maturity_columns[0].replace(deleted_elems[0] if len(deleted_elems) else '', '') + " " + df[possible_maturity_columns[0]].astype(str)
    except:
        return df
    return df

def merge_similar_dtypes(column_value_dtypes):
    """
    Input: List of (DTYPE, Frequency)
    Output: Merged list of DTYPE and frequency
    e.g. Input: [('RATE', 1), ('NUMBER', 21)], Ouput: [('RATE', 22)]
    """
    if len(column_value_dtypes) == 1:
        return column_value_dtypes
    first_dtype = ""
    for idx, (dt, freq) in enumerate(column_value_dtypes):
        if idx == 0:
            first_dtype = dt
        else:
            if (first_dtype == "RATE" and dt in ["NUMBER", "RATE"]) or (
                first_dtype == "CURRENCY" and dt in ["NUMBER", "CURRENCY"]):
                column_value_dtypes[0] = (column_value_dtypes[0][0], column_value_dtypes[0][1] + freq)
                del column_value_dtypes[idx]
    
    return column_value_dtypes

def clean_df(df: DataFrame):
    is_multiline_header = False
    multiline_header_span_count = 0
    # Scan for data type column wise
    for column in df.columns:
        if column.lower().startswith("unnamed"):
            pass
        column_value_dtypes = [(k, len(list(g))) for k, g in itertools.groupby([infer_type(str(x)) for x in df[column]])]
        column_value_dtypes = merge_similar_dtypes(column_value_dtypes)
        column_value_dtypes_wo_nan = [(k, g) for k, g in column_value_dtypes if k != 'NAN']
        if len(column_value_dtypes_wo_nan) == 2:
            if column_value_dtypes_wo_nan[0][1] <= 2:
                is_multiline_header = True

                if not multiline_header_span_count:
                    stats = {k:v for k, v in column_value_dtypes}
                    dominant_type = max(stats, key=stats.get)
                    for t, freq in column_value_dtypes:
                        if t == dominant_type:
                            break
                        multiline_header_span_count += freq


            else:
                is_multiline_header = False
    
    all_null_columns = df.isnull().values.all(axis=0)

    all_nan_ranges = []
    consecutive_nan_range = (None, None)
    for idx, col in enumerate(df.columns):
        if col.lower() == "nan" or "unnamed" in col.lower():
            next_col_name = df.columns[min(idx + 1, len(df.columns) - 1)].lower()
            if next_col_name != "nan" or "unnamed" not in next_col_name:
                if not consecutive_nan_range[0]:
                    consecutive_nan_range = (idx, None)
            else:
                consecutive_nan_range = (consecutive_nan_range[0], idx)
                all_nan_ranges.append(consecutive_nan_range)
                consecutive_nan_range = (None, None)
    

    for _ in range(multiline_header_span_count):
        if is_multiline_header:
            current_headers = ["" if header.lower().startswith("unnamed") else header.replace("nan", '') for header in df.columns.tolist()]
            first_row_values = df.iloc[0].tolist()
            new_headers = [f"{header} {value}".strip() for header, value in zip(current_headers, first_row_values)]
            df.columns = new_headers
            df = df.tail(-1)
    
    # Get rid of all rows that are all NaN
    df = df.dropna(axis = 0, how = 'all')
    try:
        df = complete_dates_from_header(df)
    except Exception as e:
        print("ERROR while doing complete_dates_from_header: %s" % e)
    
    try:
        df = complete_cusips_from_header(df)
    except Exception as e:
        print("ERROR while doing complete_cusips_from_header: %s" % e)
    
    try:
        df = standardise_column_names(df)
        df = merge_and_arrange_cell_values(df)
    except Exception as e:
        print("ERROR while doing stardising and merging: %s" % e)
    
    return df