"""
LEGACY CODE - Original implementation from Untitled-1 - JolieEdit.py
This file preserves the original code for reference.
DO NOT MODIFY - Keep as backup of original implementation.
"""

import os
os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk-17"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "\\bin;" + os.environ["PATH"]

os.environ["PYSPARK_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"

import csv
from pyspark.sql import SparkSession
from itertools import combinations
import math
from collections import defaultdict
import ast
import pyodbc
import pyspark
import pandas as pd
from pyspark.sql.functions import collect_list
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import Row

        
global conn 
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost;'
    'DATABASE=TestDB;'
    'Trusted_Connection=yes;'
)
print("Connection Successful")


""" The connection is created once when the program starts """


global transaction_new_table 
cursor = conn.cursor()
cursor.execute("SELECT products FROM tableed order by products")
rows = cursor.fetchall()
transaction_new_table = []
for row in rows:
    try:
        items = ast.literal_eval(row[0])
    except (ValueError, SyntaxError):
        continue
    items = list(set(items))
    transaction_new_table.append(items)



class FPNode:
    def __init__(self, item, count, parent):
        self.item = item
        self.count = count
        self.parent = parent
        self.children = {}
        self.link = None


def build_fp_tree(transactions, min_support):
    item_count = defaultdict(int)

    for transaction in transactions:
        for item in transaction:
            item_count[item] += 1

    # Remove infrequent items
    item_count = {item: c for item, c in item_count.items() if c >= min_support}

    if len(item_count) == 0:
        return None, None

    # Header table
    header = {item: [count, None] for item, count in item_count.items()}

    root = FPNode(None, 1, None)

    for transaction in transactions:
        filtered = [item for item in transaction if item in item_count]
        filtered.sort(key=lambda x: item_count[x], reverse=True)

        current = root
        for item in filtered:
            if item in current.children:
                current.children[item].count += 1
            else:
                node = FPNode(item, 1, current)
                current.children[item] = node

                if header[item][1] is None:
                    header[item][1] = node
                else:
                    temp = header[item][1]
                    while temp.link:
                        temp = temp.link
                    temp.link = node

            current = current.children[item]

    return root, header


def ascend_tree(node):
    path = []
    while node.parent and node.parent.item:
        node = node.parent
        path.append(node.item)
    return path


def find_prefix_paths(base, node):
    patterns = []
    while node:
        path = ascend_tree(node)
        if path:
            patterns.append((path, node.count))
        node = node.link
    return patterns


def fp_growth(transactions, min_support):

    tree, header = build_fp_tree(transactions, min_support)
    frequent_patterns = []

    def mine_tree(header, prefix):

        items = sorted(header.items(), key=lambda x: x[1][0])

        for item, data in items:
            new_pattern = prefix.copy()
            new_pattern.add(item)

            frequent_patterns.append((new_pattern, data[0]))

            cond_patterns = find_prefix_paths(item, data[1])

            cond_transactions = []
            for path, count in cond_patterns:
                for _ in range(count):
                    cond_transactions.append(path)

            cond_tree, cond_header = build_fp_tree(cond_transactions, min_support)

            if cond_header:
                mine_tree(cond_header, new_pattern)

    mine_tree(header, set())

    return frequent_patterns




############################################ distributed idea
#### 

def partitional_load():

    spark = SparkSession.builder \
        .appName("DistributedInsert") \
        .getOrCreate()

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE Row_data")
    conn.commit()

    df = spark.read.csv(
        "C:/Python/OnlineRetail.csv",
        header=True,
        inferSchema=True
    )

    df = df.repartition(12)

    print("Partitions:", df.rdd.getNumPartitions())

    df.foreachPartition(insert_partition)

    print("Data inserted successfully")

    spark.stop()



# Function executed on each partition
def insert_partition(iterator):

    print ('here we are')
    try:
        connection = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=localhost;'
            'DATABASE=TestDB;'
            'Trusted_Connection=yes;'
        )
        print("Connection Successful")


            
        cursor = connection.cursor()
        
        
        reader = pd.read_csv(
            'c:/Python/OnlineRetail.csv',
            dtype={0: str}   # or column name instead of index
)

        for row in iterator:
            cursor.execute(
                            """
                            INSERT INTO Row_data 
                            (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            row['InvoiceNo'],
                            row['StockCode'],
                            row['Description'],
                            row['Quantity'],
                            row['InvoiceDate'],
                            row['UnitPrice'],
                            row['CustomerID'],
                            row['Country']
                        )

        connection.commit()
        print("Data inserted successfully")

    except Exception as e:
        print("Error occurred:", e)

    finally:
        connection.close()




#########################################################################################
#########################################################################################
#########################################################################################



def Cleaning ():
    print ('Clean')
    cursorC = conn.cursor()
    cursorC.execute(
        """
        truncate table Cleaned_data
        insert into Cleaned_data select * from Row_data
        delete from Cleaned_data where InvoiceNo like '%C%'
        delete from Cleaned_data where CustomerID is null
        delete from Cleaned_data where Quantity like '%-%' 
        delete from Cleaned_data where UnitPrice = '0' or UnitPrice like '%-%' 
        delete from Cleaned_data where Description is null or Description like '%?%' 
        delete from Cleaned_data where StockCode = 'POST'
        """
        )
    
    cursorC.execute(
    """insert into tableed
        select InvoiceNo, STRING_AGG(CAST(description AS VARCHAR(MAX)), ', ') AS products
        FROM Cleaned_data
        GROUP BY InvoiceNo;
    """
    )
    conn.commit()
    conn.close()
    

def apriori_algorithm(transactions, min_support):
    
    from itertools import combinations
    item_counts = {}
    num_transactions = len(transactions)

    # Count single items
    for transaction in transactions:
        for item in transaction:
            if (item,) not in item_counts:
                item_counts[(item,)] = 0
            item_counts[(item,)] += 1

    # Filter by minimum support
    frequent_itemsets = {}
    current_frequent = {}

    for item, count in item_counts.items():
        support = count 
        if support >= min_support:
            current_frequent[item] = support

    k = 2

    while current_frequent:
        frequent_itemsets.update(current_frequent)
        items = list(current_frequent.keys())
        candidates = {}

        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                union = tuple(sorted(set(items[i]) | set(items[j])))
                if len(union) == k:
                    candidates[union] = 0

        for transaction in transactions:
            transaction_set = set(transaction)
            for candidate in candidates:
                if set(candidate).issubset(transaction_set):
                    candidates[candidate] += 1

        current_frequent = {}
        for item, count in candidates.items():
            support = count 
            if support >= min_support:
                current_frequent[item] = support

        k += 1

    return frequent_itemsets



def parallel_fpgrowth(transactions):

    rows = [Row(items=t) for t in transactions]
    spark = SparkSession.builder.appName("MarketBasket").getOrCreate()
    df = spark.createDataFrame(rows)

    fp = FPGrowth(itemsCol="items", minSupport=0.003, minConfidence=0.005)

    model = fp.fit(df)

    print("Frequent Itemsets (without singles):")
    filtered_itemsets  = model.freqItemsets.filter("size(items) > 1").collect()
    
    connection = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost;'
    'DATABASE=TestDB;'
    'Trusted_Connection=yes;'
) 
    cursorFB = connection.cursor()

    for row in filtered_itemsets:
        items = ",".join(row["items"])   # convert list to string
        freq = row["freq"]

        cursorFB.execute(
            "INSERT INTO FPResults (Items, Frequency) VALUES (?, ?)",
            items,
            freq
        )

    connection.commit()
    connection.close()

    print("Association Rules:")
    model.associationRules.show()




# -----------------------------
# Local Apriori (runs on each partition)
# -----------------------------
def apriori_partition(partition, support_threshold, total_count):
    partition = list(partition)
    partition_size = len(partition)

    # Adjusted local support
    local_support = math.ceil(support_threshold * partition_size / total_count)

    # Count 1-itemsets
    item_counts = {}
    for txn in partition:
        for item in txn:
            item_counts[item] = item_counts.get(item, 0) + 1

    freq_items = set(
        [frozenset([item]) for item, count in item_counts.items() if count >= local_support]
    )

    current_freq = freq_items
    all_freq = set(freq_items)
    k = 2

    while current_freq:
        candidates = set()
        freq_list = list(current_freq)

        # Generate candidates
        for i in range(len(freq_list)):
            for j in range(i + 1, len(freq_list)):
                union = freq_list[i].union(freq_list[j])
                if len(union) == k:
                    candidates.add(union)

        # Count candidates
        candidate_counts = {}
        for txn in partition:
            txn_set = set(txn)
            for candidate in candidates:
                if candidate.issubset(txn_set):
                    candidate_counts[candidate] = candidate_counts.get(candidate, 0) + 1

        current_freq = set(
            [c for c, count in candidate_counts.items() if count >= local_support]
        )

        all_freq.update(current_freq)
        k += 1

    return iter(all_freq)


# -----------------------------
# SON Algorithm
# -----------------------------
def son_apriori(transactions, min_support):
    spark = SparkSession.builder.appName("SON_Apriori").getOrCreate()
    sc = spark.sparkContext

    rdd = sc.parallelize(transactions)
    total_count = rdd.count()

    # -----------------------------
    # Phase 1: Local frequent itemsets (candidates)
    # -----------------------------
    candidates = (
        rdd.mapPartitions(lambda part: apriori_partition(part, min_support, total_count))
           .distinct()
           .collect()
    )

    candidates = [frozenset(c) for c in candidates]

    # Broadcast candidates
    bc_candidates = sc.broadcast(candidates)

    # -----------------------------
    # Phase 2: Global counting
    # -----------------------------
    candidate_counts = (
        rdd.flatMap(lambda txn: [
            (candidate, 1)
            for candidate in bc_candidates.value
            if candidate.issubset(set(txn))
        ])
        .reduceByKey(lambda a, b: a + b)
    )

    # Filter by global support
    frequent_itemsets = (
        candidate_counts
        .filter(lambda x: x[1] / total_count >= min_support)
        .map(lambda x: x[0])
        .collect()
    )

    return frequent_itemsets




def main():
    # main program logic


    print ('Begin')
    partitional_load()
    Cleaning ()
    
    '''
    print ('apriori_algorithm')
        
    result = apriori_algorithm(transaction_new_table, 3)
    filtered_result = {items:support for items, support in result.items() if len(items) > 1}
    print(filtered_result)
    
    print ('')
    print ('fp_growth')
    #print ( fp_growth(transaction_new_table,3))
    
    patterns = fp_growth(transaction_new_table, 3)

    double_items = []
    for items, support in patterns:
        if len(items) == 2:
            double_items.append((items, support))
    print(double_items)
    '''
    
    #distributed
    parallel_fpgrowth(transaction_new_table)
    
    # Parallel Apriori 

    min_support = 0.4
    result = son_apriori(transaction_new_table, min_support)
    print("Frequent Itemsets:")
    for itemset in result:
        print(set(itemset))


    print ('The End')


if __name__ == "__main__":
    main()