"""
Current status of this code: executed in pyspark console, not via pyspark command

#TO-DO:
    1. Printing into json file
    2. Proper handling of overflow errors due to large float values
"""
import pyspark
from pyspark.sql import SparkSession

conf = pyspark.SparkConf().setAll([('spark.driver.memory','240g'), ('spark.executor.memory', '24g'), ('spark.dynamicAllocation.enabled', 'true'), ('spark.executor.cores', '4'), ('spark.executor.instances', '32')])


#sc.stop()
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)


# -------------------------------------------------------------------------------
from pyspark.sql.functions import lit
from dateutil.parser import parse
import json
import time

def mapper_identical_vals(x):
    ans = []
    for coli, col in enumerate(x[:-1]):
        dataset_name = str(x[-1])
        try:
            if(col != None):
                col_eval = json.loads(col)
                t = type(col_eval)
                if(t == int):
                    col_eval = int(col_eval)
                    # maintain datatype, sum, count, max, min, sum_of_squares
                    ans.append(((dataset_name, coli, 'I', col_eval), ('I', col_eval, 1, col_eval, col_eval, col_eval**2)))
                elif(t == float):
                    col_eval = float(col_eval)
                    # maintain datatype, sum, count, max, min, sum_of_squares
                    try:
                        ans.append(((dataset_name, coli, 'R', col_eval), ('R', col_eval, 1, col_eval, col_eval, col_eval**2)))
                    except OverflowError as err:
                        print('Overflowed after ', x, err)
            else:
                ans.append(((dataset_name, coli, 'None', 'None'), ('None', 1)))
        except ValueError:
            # check whether it is of type DATE
            try:
                parsed_date = parse(col)
                # treat as a DATE type: datatype, parsed_date, count
                # parsed_date can be used as a key, but we will maintain original value for future use
                ans.append(((dataset_name, coli, 'D', col), ('D', parsed_date, 1)))
            except:
                # treat as a normal text, maintain datatype, sum, count
                ans.append(((dataset_name, coli, 'T', col), ('T', len(col), 1)))
        except SyntaxError:
            print("Error in evaluating data type for", col)
    return ans



def mapper_identical_datatypes(x):
    if(x[1][0] == 'I' or x[1][0] == 'R'):
        top5cnt_list = [(x[1][2], x[0][3]), (0, 0), (0, 0), (0, 0), (0, 0)]
        # key = (col, datatype); value = (data_type, sum, total_count, max,     \
        # min, sum_of_squares, distinct_cnt, top5cnt_list)
        new_list = [x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5]]
        new_list.append(top5cnt_list)
        new_list.append(1)
        new_val = tuple(new_list)
    elif(x[1][0] == 'T'):
        top5cnt_list = [(x[1][2], x[0][3]), (0, 0), (0, 0), (0, 0), (0, 0)]
        top5long_list = []
        top5short_list = []
        for i in range(min(5, x[1][2])):
            top5long_list.append((x[1][1], x[0][3]))
            top5short_list.append((x[1][1], x[0][3]))
        for i in range(len(top5long_list), 5):
            top5long_list.append((0, ""))
            top5short_list.append((float('inf'), ""))
        new_list = [x[1][0], x[1][1], x[1][2]]
        new_list.append(top5cnt_list)
        new_list.append(1)
        new_list.append(top5long_list)
        new_list.append(top5short_list)
        new_val = tuple(new_list)
    elif(x[1][0] == 'D'):
        top5cnt_list = [(x[1][2], x[0][3]), (0, 0), (0, 0), (0, 0), (0, 0)]
        # datatype, count, max, min, top5list, dist_cnt
        new_list = [x[1][0], x[1][2], (x[0][3], x[1][1]), (x[0][3], x[1][1])]
        new_list.append(top5cnt_list)
        new_list.append(1)
        new_val = tuple(new_list)
    elif(x[1][0] == 'None'):
        return x
    return ((x[0][0], x[0][1], x[0][2]), new_val)



def mapper_identical_columns(x):
    if(x[1][0] == 'I' or x[1][0] == 'R'):
        tot_cnt = x[1][2]
        mean = x[1][1]/x[1][2]
        mx = x[1][3]
        mn = x[1][4]
        stdev = (x[1][5]/x[1][2] - mean**2)**0.5
        tcnt_list = x[1][6]
        dist_cnt = x[1][7]
        return ((x[0][0], x[0][1]), {x[1][0]: (tot_cnt, mean, mx, mn, stdev, tcnt_list, dist_cnt)})
    elif(x[1][0] == 'T'):
        mean = x[1][1]/x[1][2]
        # datatype, tot_cnt, mean, t5cnt, dist_cnt, long5, short5
        return ((x[0][0], x[0][1]), {'T': (x[1][2], mean, x[1][3], x[1][4], x[1][5], x[1][6])})
    elif(x[1][0] == 'D'):
        # datatype, count, max, min, top5cnt, dist_cnt
        return ((x[0][0], x[0][1]), {'D': x[1][1:]})
    elif(x[1][0] == 'None'):
        # datatype, count
        return ((x[0][0], x[0][1]), {x[1][0]: (x[1][1])})



def mapper_identical_datasets(x):
    column_obj = {"column_name": x[0][1], "number_non_empty_cells": 0, "number_empty_cells": 0, "number_distinct_values": 0, "frequent_values": [], "data_types": []}
    
    
def reduce_identical_datatypes(x, y):
    if(x[0] == 'I' or x[0] == 'R'):
        x[6].extend(y[6])
        red_top5cnt = sorted(x[6], key = lambda x: x[0], reverse = True)[:5]
        return (x[0], x[1] + y[1], x[2] + y[2], max(x[3], y[3]), min(x[4], y[4]), x[5] + y[5], red_top5cnt, x[7] + y[7])
    elif(x[0] == 'T'):
        x[3].extend(y[3])
        x[5].extend(y[5])
        x[6].extend(y[6])
        # top 5 frequent counts
        red_top5cnt = sorted(x[3], key = lambda x: x[0], reverse = True)[:5]
        # longest 5
        red_long5 = sorted(x[5], key = lambda x: x[0], reverse = True)[:5]
        # shortest 5
        red_short5 = sorted(x[6], key = lambda x: x[0], reverse = False)[:5]
        return ('T', x[1] + y[1], x[2] + y[2], red_top5cnt, x[4] + y[4], red_long5, red_short5)
    elif(x[0] == 'D'):
        x[4].extend(y[4])
        red_top5cnt = sorted(x[4], key = lambda x: x[0], reverse = True)[:5]
        if(x[2][1] > y[2][1]):
            mx = x[2]
        else:
            mx = y[2]
        if(x[3][1] > y[3][1]):
            mn = y[3]
        else:
            mn = x[3]
        return ('D', x[1] + y[1], mx, mn, red_top5cnt, x[5] + y[5])
    elif(x[0] == 'None'):
        return ('None', x[1] + y[1])



def reduce_identical_vals(x, y):
    if(x[0] == 'I' or x[0] == 'R'):
        # type, sum, count, min, max, sum of squares
        return (x[0], x[1] + y[1], x[2] + y[2], x[3], x[4], x[5] + y[5])
    elif(x[0] == 'T'):
        # type, dist value, sum, count
        return ('T', x[1] + y[1], x[2] + y[2], x[3], x[4])
    elif(x[0] == 'D'):
        # type, parsed_date, count
        return ('D', x[1], x[2] + y[2])
    elif(x[0] == 'None'):
        return ('None', x[1] + y[1])



def reduce_identical_columns(x, y):
    x.update(y)
    return x



def process_dataset_rdd(dataset_rdd):
    # maps int/real, text, date, None datatypes with appropriate values to calculate
    # respective statistics in future
    dataset_map1 = dataset_rdd.flatMap(mapper_identical_vals)
    # reduce to unique values
    dataset_red1 = dataset_map1.reduceByKey(reduce_identical_vals)
    # map to group elements by their data types
    dataset_map2 = dataset_red1.map(mapper_identical_datatypes)
    # reduce to calculate sum, count, min, max, sum of squres
    dataset_red2 = dataset_map2.reduceByKey(reduce_identical_datatypes)
    # calculate mean, max, min, stdev, top_5_cnt_list, dist_cnt
    dataset_map3 = dataset_red2.map(mapper_identical_columns)
    # group by columns
    dataset_red3 = dataset_map3.reduceByKey(reduce_identical_columns)
    
    print(dataset_red3.collect())
    #for coli in range(num_col):   
        #dataset_num_map = dataset_map.filter(lambda x)    



def process_datasets():
    start_time = time.time()
    df_datasets = spark.read.csv("/user/hm74/NYCOpenData/datasets.tsv",header=False,sep="\t")
    processed_dataset_cnt = 0
    for dataset_row in df_datasets.collect():
        dataset_fname = dataset_row[0]
        print(dataset_fname)
        dataset = spark.read.csv("/user/hm74/NYCOpenData/" + dataset_fname + ".tsv.gz", header = True, sep = "\t")
        dataset = dataset.withColumn("dataset_name", lit(dataset_fname))
        process_dataset_rdd(dataset.rdd)
        processed_dataset_cnt += 1
        break
        if(processed_dataset_cnt == 50):
            break
    print(time.time() - start_time)



def process_datasets_parallel():
    start_time = time.time()
    df_datasets = spark.read.csv("/user/hm74/NYCOpenData/datasets.tsv",header=False,sep="\t")
    processed_dataset_cnt = 0
    datasets_rdd_list = []
    for dataset_i, dataset_row in enumerate(df_datasets.collect()):
        dataset_fname = dataset_row[0]
        print(dataset_fname)
        dataset = spark.read.csv("/user/hm74/NYCOpenData/" + dataset_fname + ".tsv.gz", header = True, sep = "\t")
        dataset = dataset.withColumn("dataset_name", lit(dataset_fname))
        datasets_rdd_list.append(dataset.rdd)
        processed_dataset_cnt += 1
        if(processed_dataset_cnt == 50):
            break
    datasets_rdd = sc.union(datasets_rdd_list)
    print(time.time() - start_time)
    #print(datasets_rdd.count())
    process_dataset_rdd(datasets_rdd)
    print(time.time() - start_time)


process_datasets()
#process_datasets_parallel()
