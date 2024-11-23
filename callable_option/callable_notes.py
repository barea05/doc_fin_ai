"""#double
Maturity Date Principal Interest CUSIP   
(May 1) Amount Rate Yield Number!

Due Principal Interest
February 15 Amount Rate Yield



Stated Maturity Principal Interest CUSIP
September 1 Amount Rate Yield Number


Maturity Date Principal
(August 1) Amount Coupon Yield Price CUSIP†

due principaL interest due principaL interest
oct. 1 aMount rate yieLd cusip oct. 1 aMount rate yieLd cusip

Interest CUSIP(a) Interest CUSIP(a)
Year Amount Rate Price No. 246213 Year Amount Rate Price No. 246213

Maturity Interest Yield CUSIP = Maturity Interest Yield CUSIP
(February 1) Amount Rate orPrice 288002 (February 1) Amount Rate orPrice 288002

Maturity Date Principal Initial
(July 1) Amount Interest Rate Yield Price CUSIP No.(1)

Maturity Principal Interest Price or CUSIP
Guly Amount Rate Yield Numbers(1
                               
Maturity Date Principal Interest CUSIP†
(December 1) Amount Rate Yield Price (Base 447447)

Maturity Principal Interest Initial
(August 15) Amount Rate Price Yield CUSIP No.(1)

DATE OF DATE OF
MATURITY PRINCIPAL INTEREST MATURITY PRINCIPAL INTEREST
(JUNE 1) AMOUNT RATE YIELD CUSIP** (JUNE 1) AMOUNT RATE YIELD CUSIP**

Payment Payment
Date Principal Interest Reoffering Date Principal Interest Reoffering

(September 1) Amount Rate Yield (September 1} Amount Rate Yield


#single
Year Amount Rate Yield CUSIP! Year Amount Rate Yield CUSIP!
Year Amount Rate Or Price CUSIP #
Year Principal Amount Interest Rate Yield CUSIP No. #--> TWO TABLES 
Year Amount Rate Price No. 246213 Year Amount Rate Price No. 246213
Maturity Amount Rate Price (183036) Maturity Amount Rate Price (183036)
Bond Due Amount Rate Yield Cusip Num* BondsDue Amount Rate Yield Cusip Num*
Maturity Principal Rate Yield CUSIP Maturity Principal Rate Yield CUSIP
(March 15) Principal Amount Interest Rate Yield Price
(JUNE 1) AMOUNT RATE YIELD CUSIP** (JUNE 1) AMOUNT RATE YIELD CUSIP**
(September 1) Amount Rate Yield (September 1} Amount Rate Yield
"""
#three line header
DATE OF DATE OF
MATURITY PRINCIPAL INTEREST MATURITY PRINCIPAL INTEREST
Payment Payment
Date Principal Interest Reoffering Date Principal Interest Reoffering

#first header
Maturity Date Principal Interest CUSIP 
Due Principal Interest
Stated Maturity Principal Interest CUSIP
Maturity Date Principal
due principaL interest due principaL interest
Interest CUSIP(a) Interest CUSIP(a)
Maturity Interest Yield CUSIP = Maturity Interest Yield CUSIP
Maturity Date Principal Initial
Maturity Principal Interest Price or CUSIP
Maturity Date Principal Interest CUSIP†

#double
(May 1) Amount Rate Yield Number!
February 15 Amount Rate Yield #without cusip number
September 1 Amount Rate Yield Number
(August 1) Amount Coupon Yield Price CUSIP†
oct. 1 aMount rate yieLd cusip oct. 1 aMount rate yieLd cusip
Year Amount Rate Price No. 246213 Year Amount Rate Price No. 246213
(February 1) Amount Rate orPrice 288002 (February 1) Amount Rate orPrice 288002
(July 1) Amount Interest Rate Yield Price CUSIP No.(1)
Guly Amount Rate Yield Numbers(1
(December 1) Amount Rate Yield Price (Base 447447)
(August 15) Amount Rate Price Yield CUSIP No.(1)
(JUNE 1) AMOUNT RATE YIELD CUSIP** (JUNE 1) AMOUNT RATE YIELD CUSIP**
(September 1) Amount Rate Yield (September 1} Amount Rate Yield


#single
Year Amount Rate Yield CUSIP! Year Amount Rate Yield CUSIP!
Year Amount Rate Or Price CUSIP #
Year Principal Amount Interest Rate Yield CUSIP No. #--> TWO TABLES 
Year Amount Rate Price No. 246213 Year Amount Rate Price No. 246213
Maturity Amount Rate Price (183036) Maturity Amount Rate Price (183036)
Bond Due Amount Rate Yield Cusip Num* BondsDue Amount Rate Yield Cusip Num*
Maturity Principal Rate Yield CUSIP Maturity Principal Rate Yield CUSIP
(March 15) Principal Amount Interest Rate Yield Price
(JUNE 1) AMOUNT RATE YIELD CUSIP** (JUNE 1) AMOUNT RATE YIELD CUSIP**
(September 1) Amount Rate Yield (September 1} Amount Rate Yield


maturity_header = r"Maturity Date|Maturity|Year|Bond Due|((?:January|Januaryy|Janaury|Janauryy|February|Februaryy|March|Mareh|April|Appril|Mayy|June|July|Julyy|August|Auggust|September|Sepptember|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Guly|Aug|Sep|Oct|Nov|Dec|jan|feb|mar|apr|may|mayy|jun|jul|aug|sep|oct|nov|dec)[\s.,-]*\d{1,2})"

principal_amount_header = r"Principal Amount|Amount|"

interest_rate = r"Interest Rate|Coupon Rate|Rate"

yield_header = r"Yield|Or Price|Price|OrPrice"

cusip_header = r"CUSIP No.|Cusip Num*|CUSIP|CUSIP**|Number|(\(\d{6}\))|(\d{6})|\d{6}"



maturity_val_pattern = r""

principal_val_pattern = r""

interest_val_pattern = r""

yield_val_pattern = r""

cusip_val_pattern = r""

