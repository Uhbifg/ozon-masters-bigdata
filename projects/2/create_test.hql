CREATE TEMPORARY EXTERNAL TABLE hw2_test (
    id  INT,
    if1 INT,
    if2 INT,
    if3 INT,
    if4 INT,
    if5 INT,
    if6 INT,
    if7 INT,
    if8 INT,
    if9 INT,
    if10 INT,
    if11 INT,
    if12 INT,
    if13 INT, 
    cf1 string,
    cf2 string,
    cf3 string,
    cf4 string,
    cf5 string,
    cf6 string,
    cf7 string,
    cf8 string,
    cf9 string,
    cf10 string,
    cf11 string,
    cf12 string,
    cf13 string,
    cf14 string,
    cf15 string,
    cf16 string,
    cf17 string,
    cf18 string,
    cf19 string,
    cf20 string,
    cf21 string,
    cf22 string,
    cf23 string,
    cf24 string,
    cf25 string,
    cf26 string,
    day_number INT)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/datasets/criteo_test_large_features'
    tblproperties ("skip.header.line.count"="1");
    
    