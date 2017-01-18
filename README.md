# Coding Test
The source code for the coding test is located in the `src` directory.

## Dependencies
The following code uses Spark 2.0.1 installed into `/opt/spark-2.0.1-bin-hadoop2.7/`

## Setup
The environment variable `$SPARK_HOME` must be set to the root directory of the Spark distribution. 

Example: Set `$SPARK_HOME`:
```
export $SPARK_HOME=/opt/spark-2.0.1-bin-hadoop2.7/
```

## Questions

### 1. Write down the data quality issues with the datasets provided and the steps performed to clean (if any).
 1. The timestamp column in `Refund` and `Sales` are strings. Created a new derived column `d_timestamp` which stores the result of converting the timestamp column to a TimestampType.
 1. The timestamp column in `Sales` contains rows with year's that do not include the century. Needed to handle the edge case when creating the derived column.
 1. `Product`, `Refund`, and `Sales` store the price with the dollar sign. I stripped off the dollar sign and converted the value to a DecimalType so that the value could accommodate 2 decimal places for precision.
 1. The product_name column in `Product` has trailing whitespace. I trimmed off the trailing whitespace.
 1. TODO: Extract year, month, and day from timestamp fields when loading so that functions don't need to constantly dig it out of the timestamp columns.

### 2. Display the distribution of sales by product name and product type.
I chose to write 2 separate queries to get the distribution by product type and by product.

#### Running
```
export PYTHONPATH=./src
$SPARK_HOME/bin/spark-submit src/question_2.py 
```

#### Explanation: By Product Type
The function `distribution_of_sales_by_product_type` in `question_2.py` creates the following dataset which groups sales by product type, sums the amount, and calculates the percent of distribution against the total amounts for all sales.

##### Results
```
+------------+------------------+----------------+--------------------+
|product_type|total_quantity_sum|total_amount_sum|percent_distribution|
+------------+------------------+----------------+--------------------+
|        P105|              2617|         2165983|38.80363825481148...|
|        P104|              2777|         1105273|19.80099274316107...|
|        P103|              4810|         1012018|18.13032714446872...|
|        P106|               655|          949095|17.00306006531459...|
|        P102|              3473|          336637|6.030860062699002...|
|        P101|               679|           12901|0.231121729545117...|
+------------+------------------+----------------+--------------------+
```

#### Explanation: By Product
The function `distribution_of_sales_by_product` in `question_2.py` creates the following dataset which groups sales by product type and name, sums the amount, and calculates the percent of distribution against the total amounts for all sales.

##### Results
```
+------------+-------------------+------------------+----------------+--------------------+
|product_type|     d_product_name|total_quantity_sum|total_amount_sum|percent_distribution|
+------------+-------------------+------------------+----------------+--------------------+
|        P106|            Desktop|               655|          949095|17.00306006531459...|
|        P105|             Laptop|               678|          677322|12.13424014409412...|
|        P105|Home Automation Kit|               633|          632367|11.32887022302593...|
|        P105|             Tablet|               740|          517260|9.266725511550084...|
|        P105|              Phone|               566|          339034|6.073802376141343...|
|        P104|             Camera|               666|          332334|5.953771712785612...|
|        P104|              Watch|               724|          288876|5.175220583216452...|
|        P104|            Printer|               748|          261052|4.676752944826920...|
|        P104|       Baby Monitor|               639|          223011|3.995247502332088...|
|        P103|          Camcorder|               654|          195546|3.503211357695497...|
|        P103|      Car Connector|               776|          168392|3.016746785641537...|
|        P103|      GamingConsole|               672|          167328|2.997685199699672...|
|        P103|    VR Play Station|               701|          139499|2.499127986188232...|
|        P103|    Doorbell Carema|               603|          132057|2.365804374741463...|
|        P103|            Monitor|               726|          108174|1.937939847439235...|
|        P103|            Speaker|               678|          101022|1.809811593063087...|
|        P102|           Harddisk|               757|           74943|1.342605672219189...|
|        P102|        Magic Mouse|               712|           70488|1.262794238599819...|
|        P102|           Keyboard|               688|           68112|1.220228140669488...|
|        P102|              Drone|               719|           63991|1.146400325193522...|
+------------+-------------------+------------------+----------------+--------------------+
only showing top 20 rows
```

### 3. Calculate the total amount of all transactions that happened in year 2013 and have not been refunded as of today.

#### Running
```
export PYTHONPATH=./src
$SPARK_HOME/bin/spark-submit src/question_3.py 
```

#### Explanation
The function `not_refunded` in `question_3.py` takes a year as a parameter and sums the d\_total\_amount of transactions which haven't been refunded for the given year.

##### Results
```
+-----------------+
|2013_total_amount|
+-----------------+
|          1489232|
+-----------------+
``` 

### 4. Display the customer name who made the second most purchases in the month of May 2013. Refunds should be excluded.
#### Running
```
export PYTHONPATH=./src
$SPARK_HOME/bin/spark-submit src/question_4.py 
```

#### Explanation
The function `customer_with_second_most_purchases` in `question_4.py` takes a year and month as parameters and returns the first and last name of the customer with the second most total amount for all associated purchases for the given year and month.

Assumption: This is not a count of purchases, but a sum of the amount of sales. When I did count of purchases, there were many results of customers who have purchased the same amount of products.
Assumption: Sales that had a refund were excluded from the query.

##### Results
```
+-------------------+------------------+
|customer_first_name|customer_last_name|
+-------------------+------------------+
|              GOMEZ|           LINDSEY|
+-------------------+------------------+
```

### 5. Find a product that has not been sold at least once (if any).

#### Running
```
export PYTHONPATH=./src
$SPARK_HOME/bin/spark-submit src/question_5.py 
```

####  Explanation: Attempt 1
The function `attempt1` in `question_5.py` outer joins the `product` and `sales` tables and find product_ids with no `transaction_id`. The query yielded no results.

##### Results
```
+----------+
|product_id|
+----------+
+----------+
```

#### Explanation: Attempt 2
The function `attempt2` in `question_5.py` inner joins the `product` and `sales` tables and filters rows with a total_quantity < 1. The query yielded no results.

##### Results
```
+----------+------------------+
|product_id|total_quantity_sum|
+----------+------------------+
+----------+------------------+
```

#### Explanation: Attempt 3
The function `attempt3` in `question_5.py` considers refunds of sales as a way to invalidate sales that may have occurred but were refunded. The query yielded no results.

##### Results
```
+----------+----------------+
|product_id|total_amount_sum|
+----------+----------------+
+----------+----------------+
```

### 6. Calculate the total number of users who purchased the same product consecutively at least 2 times on a given day.
#### Running
```
export PYTHONPATH=./src
$SPARK_HOME/bin/spark-submit src/question_6.py 
```

#### Explanation
The function `total_users_who_purchased_same_product` in `question_6.py` groups sales data by year, month, day, customer\_id, and product\_id from the `sales` table. It counts the number of sales transactions for the group and filters out customers who have less than 1 purchase.

I purposefully did not consider the `total_quantity` since the question asked for purchases, not quantities. I also did not consider refunds because I didn't think they were necessarily applicable.


##### Results
Result is the count of users who have sales transactions for the same day for the same product:

```
+---+
| ct|
+---+
|  9|
+---+
```

### Extra Question: Display all the details of a customer who is currently living at 1154 Winters Blvd.
#### Running
```
export PYTHONPATH=./src
$SPARK_HOME/bin/spark-submit src/question_7.py 
```

#### Explanation
The function `customers_living_at` in `question_7.py` takes an address, lowercases it, and looks for equality within the `current_street_address` field of the `customer_extended` table.

I considered:
 1. Running a LIKE comparison instead of straight equality but chose against it since it appeared the dataset didn't include any weird permutations of 1154 Winters Blvd.
 1. Looking for other versions of Blvd, like the full word Boulevard, but there didn't appear to be any instances including it.
 1. Looking in other fields like `permanent_street_address` but query showed that Winters Blvd only existed in the `current_street_address` field. I removed querying more fields for simplicity and performance.

##### Results

```
+-----------------------------+---+
|lower(current_street_address)| ct|
+-----------------------------+---+
|            1154 winters blvd|  1|
+-----------------------------+---+
```