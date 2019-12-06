"""
Current status of this code: executed in pyspark console, not via pyspark command

#TO-DO:
    1. Parallelize for datasets, right now datasets are processed serially
    2. only statistics for numeric data type is calculated properly
        for other data types, it is not calculated
    3. Printing into json file
"""

import json

def mapper_identical_vals(x):
    ans = []
    for coli, col in enumerate(x):
        try:
            if(col != None):
                col_eval = json.loads(col)
                t = type(col_eval)
                if(t == int):
                    col_eval = int(col_eval)
                    # maintain datatype, sum, count, max, min, sum_of_squares
                    ans.append(((coli, 'I', col_eval), ('I', col_eval, 1, col_eval, col_eval, col_eval**2)))
                elif(t == float):
                    col_eval = float(col_eval)
                    # maintain datatype, sum, count, max, min, sum_of_squares
                    ans.append(((coli, 'R', col_eval), ('R', col_eval, 1, col_eval, col_eval, col_eval**2)))                    
            else:
                ans.append(((coli, 'None', 'None'), ('None', 1)))
        except ValueError:
            # maintain datatype, sum, count
            ans.append(((coli, 'T', col), ('T', len(col), 1)))
        except SyntaxError:
            print("Error in evaluating data type for", col)
    return ans



def mapper_identical_datatypes(x):
    if(x[1][0] == 'I' or x[1][0] == 'R'):
        top5cnt_list = [(x[1][2], x[0][2]), (0, 0), (0, 0), (0, 0), (0, 0)]
        # key = (col, datatype); value = (data_type, sum, total_count, max,     \
        # min, sum_of_squares, distinct_cnt, top5cnt_list)
        new_list = [x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5]]
        new_list.append(top5cnt_list)
        new_list.append(1)
        new_val = tuple(new_list)
    elif(x[1][0] == 'T'):
        top5cnt_list = [(x[1][2], x[0][2]), (0, 0), (0, 0), (0, 0), (0, 0)]
        len_tuple = (len(x[0][2]), x[0][2])
        if x[1][2] >= 5:
            top5longest = [len_tuple, len_tuple, len_tuple, len_tuple, len_tuple]
            top5shortest = [len_tuple, len_tuple, len_tuple, len_tuple, len_tuple]
        else:
            top5longest = [(len(""), ""), (len(""), ""), (len(""), ""), (len(""), ""), (len(""), "")]
            top5shortest = [(len("wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww"), "wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww"), (len("wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww"), "wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww"), (len("wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww"), "wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww"), (len("wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww"), "wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww"), (len("wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww"), "wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww")]
            for i in range(x[1][2]):
                top5longest[i] = len_tuple
                top5shortest[i] = len_tuple
        new_list = [x[1][0], x[1][1], x[1][2]]
        new_list.append(top5cnt_list)
        new_list.append(1)
        new_list.append(top5longest)
        new_list.append(top5shortest)
        new_val = tuple(new_list)
    elif(x[1][0] == 'None'):
        return x
    return ((x[0][0], x[0][1]), new_val)



def map_mean_stdev(x):
    if(x[0] == 'I' or x[0] == 'R'):
        mean = x[1]/x[2]
        mx = x[3]
        mn = x[4]
        stdev = (x[5]/x[2] - mean**2)**0.5
        tcnt_list = x[6]
        dist_cnt = x[7]
        return (x[0], mean, mx, mn, stdev, tcnt_list, dist_cnt)
    elif(x[0] == 'T'):
        mean = x[1]/x[2]
        return ('T', mean, x[3], x[4], x[5], x[6])
    elif(x[0] == 'None'):
        return x



def reduce_identical_datatypes(x, y):
    if(x[0] == 'I' or x[0] == 'R'):
        x[6].extend(y[6])
        red_top5cnt = sorted(x[6], key = lambda x: x[0], reverse = True)[:5]
        return (x[0], x[1] + y[1], x[2] + y[2], max(x[3], y[3]), min(x[4], y[4]), x[5] + y[5], red_top5cnt, x[7] + y[7])
    elif(x[0] == 'T'):
        x[3].extend(y[3])
        x[5].extend(y[5])
        x[6].extend(y[6])
        red_top5cnt = sorted(x[3], key = lambda x: x[0], reverse = True)[:5]
        red_top5long = sorted(x[5], key = lambda x: x[0], reverse = True)[:5]
        red_top5short = sorted(x[6], key = lambda x: x[0], reverse = False)[:5]
        return ('T', x[1] + y[1], x[2] + y[2], red_top5cnt, x[4] + y[4], red_top5long, red_top5short)
    elif(x[0] == 'None'):
        return ('None', x[1] + y[1])



def reduce_identical_vals(x, y):
    if(x[0] == 'I' or x[0] == 'R'):
        return ('R', x[1] + y[1], x[2] + y[2], max(x[3], y[3]), min(x[4], y[4]), x[5] + y[5])
    elif(x[0] == 'T'):
        return ('T', x[1] + y[1], x[2] + y[2])
    elif(x[0] == 'None'):
        return ('None', x[1] + y[1])



def process_dataset_rdd(dataset_rdd):
    # maps int/real, text, date, None datatypes with appropriate values to calculate
    # statistics in future
    dataset_map1 = dataset_rdd.flatMap(mapper_identical_vals)
    # reduce to unique values
    dataset_red1 = dataset_map1.reduceByKey(reduce_identical_vals)
    # map to group elements by their data types
    dataset_map2 = dataset_red1.map(mapper_identical_datatypes)
    # reduce to calculate sum, count, min, max, sum of squres
    dataset_red2 = dataset_map2.reduceByKey(reduce_identical_datatypes)
    # calculate mean, stdev etc.
    dataset_map3 = dataset_red2.mapValues(map_mean_stdev)
    #num_col = len(dataset.columns)
    print(dataset_map3.collect())
    #for coli in range(num_col):   
        #dataset_num_map = dataset_map.filter(lambda x)    



import time
def process_datasets():
    start_time = time.time()
    df_datasets = spark.read.csv("/user/hm74/NYCOpenData/datasets.tsv",header=False,sep="\t")
    processed_dataset_cnt = 0
    for dataset_row in df_datasets.collect():
        dataset_fname = dataset_row[0]
        print(dataset_fname)
        dataset = spark.read.csv("/user/hm74/NYCOpenData/" + dataset_fname + ".tsv.gz", header = True, sep = "\t")
        process_dataset_rdd(dataset.rdd)
        processed_dataset_cnt += 1
        break
        if(processed_dataset_cnt == 50):
            break
    print(time.time() - start_time)



process_datasets()
