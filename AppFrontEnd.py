import pyspark
from pyspark.sql.functions import *
from pyspark.sql import SparkSession 
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
conf = SparkConf().setAppName("AppFrontEnd2").setMaster("local")
sc = SparkSession.builder.config("spark.driver.host", "localhost") \
.config ("spark.sql.execution.arrow.enabled", "true").getOrCreate()
#.config ("spark.sql.shuffle.partitions", "50") \
#.config("spark.driver.maxResultSize","5g") \


# Read Credit Card Info from DB to a Data Frame
df_credit=sc.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user="root",\
                                     password="lakshmi",\
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                     dbtable="creditcard_capstone.CDW_SAPP_CREDIT_CARD").load()
df_credit.show()

# Read Branch Info from DB to a Data Frame
df_branch=sc.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user="root",\
                                     password="lakshmi",\
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                     dbtable="creditcard_capstone.CDW_SAPP_BRANCH").load()
df_branch.printSchema()

# Read Customer Info from DB to a Data Frame
df_cust=sc.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user="root",\
                                     password="lakshmi",\
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                     dbtable="creditcard_capstone.CDW_SAPP_CUSTOMER").load()
df_cust.show()

#Function to collect transaction information by user input zip, month and year.
#1. Join cust and credit table by ssn
#2. Filter using cust zip and (month and year) by truncating TimeID column until month.
#3. Compare user input with filtered values(zip, month and date)
#4. Change dtype from string to date to sorting in desc order

def get_credit_card_transaction_byzip_monthyear(zipcode, month, year):
    dateid = year + "-" + month + "-01"
    df_credit.join(df_cust, df_credit.CUST_SSN == df_cust.SSN, 'inner'). \
        select(df_credit.TRANSACTION_ID, df_credit.CUST_SSN, df_cust.CUST_ZIP, df_credit.TIMEID, df_credit.TRANSACTION_TYPE, df_credit.TRANSACTION_VALUE). \
        filter((df_cust.CUST_ZIP==zipcode) & (trunc(col('TIMEID'),"Month") == dateid)). \
        sort(to_date(df_credit.TIMEID, "yyyy-MM-dd").desc()).show()

#Function to display the number and total values of transactions for a user input transaction type
#1. Group by Transaction type and aggregate by Transaction ID with sum of transaction value
#2. Compare the filtered value with the user input value 

def get_transaction_summary_by_transaction_type(transactiontype):
    df_credit.groupBy('TRANSACTION_TYPE') \
        .agg(count('TRANSACTION_ID').alias('Transaction Count'), \
            (sum('TRANSACTION_VALUE')).alias('Total Transaction Value') ) \
        .filter(col('TRANSACTION_TYPE') == transactiontype) \
        .show()

#Function to display the number and total values of transactions for branches in a given state.
#1. Join the table branch with the table credit to get the transaction from credit table for all branches 
#   in a given state
#2. Group by branch code for the given state and display the transaction count and total value

def get_transaction_summary_by_branchstate(branchstate):
    df_credit.join(df_branch, df_credit.BRANCH_CODE == df_branch.BRANCH_CODE, 'inner') \
        .filter(df_branch.BRANCH_STATE == branchstate) \
        .groupBy(df_credit.BRANCH_CODE) \
        .agg(count('TRANSACTION_ID').alias('Transaction Count'), \
            (sum('TRANSACTION_VALUE')).alias('Total Transaction Value') ) \
        .show()

# Passing user input cust_SSN to check the existing account details of a customer from the table customer.

def get_customer_info(custSSN):
    df_cust.select(df_cust.SSN, \
    df_cust.FIRST_NAME, \
    df_cust.MIDDLE_NAME, \
    df_cust.LAST_NAME, \
    df_cust.Credit_card_no, \
    df_cust.FULL_STREET_ADDRESS, \
    df_cust.CUST_CITY, \
    df_cust.CUST_STATE, \
    df_cust.CUST_COUNTRY, \
    df_cust.CUST_ZIP, \
    df_cust.CUST_PHONE, \
    df_cust.CUST_EMAIL, \
    df_cust.LAST_UPDATED \
    ).filter(df_cust.SSN == custSSN).show()

# modify the existing account details of a customer by using SSN and cust address.

def update_customer_info(custSSN, custaddress):
    df_cust.withColumn('FULL_STREET_ADDRESS', lit(custaddress)
    ).filter(df_cust.SSN == custSSN).show()

# Function to generate a monthly bill for a credit card number for a given month and year.

def get_monthly_bill_by_creditcard_forsinglemonth(ccnumber, month, year):
    dateid = year + "-" + month + "-01"
    df_credit.join(df_cust, df_credit.CUST_SSN == df_cust.SSN, 'inner'). \
        select(df_credit.TRANSACTION_ID, df_credit.CUST_SSN, df_cust.Credit_card_no, df_cust.CUST_ZIP, df_credit.TIMEID, df_credit.TRANSACTION_TYPE, df_credit.TRANSACTION_VALUE). \
        filter((df_cust.Credit_card_no==ccnumber) & (trunc(col('TIMEID'),"Month") == dateid)). \
        sort(to_date(df_credit.TIMEID, "yyyy-MM-dd").desc()).show() 

# Function to display the transactions made by a customer between two dates. Order by year, month, and day 
# in descending order.
# 1. Convert input 

def get_transaction_details_between_period_for_customer(custSSN, fromDate, toDate):
    df_credit.join(df_cust, df_credit.CUST_SSN == df_cust.SSN, 'inner'). \
        select(df_credit.TRANSACTION_ID, df_credit.CUST_SSN, df_cust.Credit_card_no, df_cust.CUST_ZIP, df_credit.TIMEID, df_credit.TRANSACTION_TYPE, df_credit.TRANSACTION_VALUE). \
        filter((df_cust.SSN==custSSN) & \
        (to_date(df_credit.TIMEID, "yyyy-MM-dd") >= fromDate) & \
        (to_date(df_credit.TIMEID, "yyyy-MM-dd") <= toDate) ). \
        sort(to_date(df_credit.TIMEID, "yyyy-MM-dd").desc()).show() 

def displayPrompt():
    print(""" Please select numbers between 1 and 7 for appropriate action

    1 - To display the transactions made by customers living in a given zip code for a given month and year.
    2 - To display the total number and total values of transactions for a given transaction type like Gas, Education, Grocery etc.
    3 - To display the total number and total values of transactions for branches in a given state 
    4 - to check the existing account details of a customer.
    5 - to edit the existing account details of a customer.
    6 - Generate a monthly bill for a credit card number for a given month and year.
    7 - To display the transactions made by a customer between two dates.

    """)

displayPrompt()
optionentered = input("Select Option between 1 and 7 or Enter QUIT:")
while(optionentered!= "QUIT"):
    optionselected = int(optionentered) #to verify the numeric input
    if(optionselected<1 | optionselected>7): # to verify in the range
        print("Incorrect Option Selected")
    elif (optionselected == 1):
        zipcode = int(input("Please enter desired zip code:"))
        month = int(input("Please enter two digit month:"))
        year = int(input("Please enter four digit year:"))
        print("Selected month:", month)
        print("Selected zipcode:", zipcode)
        print("Selected Year:", year)
        if(month<10): #adding 0 to convert as two digit string if its less than 10
            monthstr = '0' + str(month)
        else:
            monthstr = str(month)
        get_credit_card_transaction_byzip_monthyear(zipcode= zipcode,month= monthstr,year= str(year))
    elif (optionselected == 2):
        transactiontype=input("Please enter desired transaction type like Gas, Education, Grocery etc.:")
        get_transaction_summary_by_transaction_type(transactiontype=transactiontype)
    elif (optionselected == 3):
        statecode=input("Please enter desired state code:")
        get_transaction_summary_by_branchstate(branchstate=statecode)
    elif (optionselected == 4):
        custSSN = input("Please enter Customer SSN:")
        get_customer_info(custSSN=custSSN)
    elif (optionselected == 5):
        custSSN = input("Please enter Customer SSN:")
        cust_address = input("Please enter Updated Customer Address :")
        update_customer_info(custSSN=custSSN, custaddress= cust_address)
    elif (optionselected == 6):
        creditnumber = int(input("Please enter desired Credit Card #:"))
        month = int(input("Please enter two digit month:"))
        year = int(input("Please enter four digit year:"))
        print("Selected Credit Card:", creditnumber)
        print("Selected month:", month)
        print("Selected Year:", year)
        if(month<10):
            monthstr = '0' + str(month)
        else:
            monthstr = str(month)
        get_monthly_bill_by_creditcard_forsinglemonth(ccnumber= creditnumber,month= monthstr,year= str(year))
    elif (optionselected==7):
        custSSN = input("Please enter Customer SSN:")
        startdate = input("Enter Start Date as YYYY-MM-DD :")
        enddate = input("Enter end Date as YYYY-MM-DD :")
        get_transaction_details_between_period_for_customer(custSSN=custSSN, fromDate=startdate, toDate=enddate)
    optionentered = input("Select Option between 1 and 7 or Enter QUIT:")
