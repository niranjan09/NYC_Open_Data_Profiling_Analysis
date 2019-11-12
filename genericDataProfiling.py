"""
Current status of this code: executed in pyspark console, not via pyspark command

#TO-DO:
    1. Parallelize for datasets, right now datasets are processed serially
    2. only statistics for numeric data type is calculated properly
        for other data types, it is not calculated
    3. Printing into json file
"""

import json

def mapper(x):
    ans = []
    for coli, col in enumerate(x):
        try:
            if(col != None):
                col_eval = json.loads(col)
                t = type(col_eval)
                if(t == int or t == float):
                    col_eval = float(col_eval)
                    # maintain sum, count, max, min, sum_of_squares
                    ans.append(((coli, 'N'), ('N', col_eval, 1, col_eval, col_eval, col_eval**2)))
            else:
                ans.append(((coli, 'None'), ('None', 1)))
        except ValueError:
            # maintain sum, count
            ans.append(((coli, 'T'), ('T', len(col), 1)))
        except SyntaxError:
            print("Error in evaluating data type for", col)
    return ans

def mapper_mean_stdev(x):
    if(x[0] == 'N'):
        mean = x[1]/x[2]
        mx = x[3]
        mn = x[4]
        stdev = (x[5]/x[2] - mean**2)**0.5
        return ('N', mean, mx, mn, stdev)
    elif(x[0] == 'T'):
        mean = x[1]/x[2]
        return ('T', mean)
    elif(x[0] == 'None'):
        return x

def reducer_add(x, y):
    if(x[0] == 'N'):
        return ('N', x[1] + y[1], x[2] + y[2], max(x[3], y[3]), min(x[4], y[4]), x[5] + y[5])
    elif(x[0] == 'T'):
        return ('T', x[1] + y[1], x[2] + y[2])
    elif(x[0] == 'None'):
        return ('None', x[1] + y[1])

def process_dataset_rdd(dataset):
    # maps int/real, text, date, None datatypes with appropriate values to calculate
    # statistics in future
    dataset_map = dataset.rdd.flatMap(mapper)
    # reduce to calculate sum, count, min, max, sum of squres
    dataset_red1 = dataset_map.reduceByKey(reducer_add)
    # calculate mean, stdev
    dataset_red2 = dataset_red1.mapValues(mapper_mean_stdev)
    #num_col = len(dataset.columns)
    print(dataset_red2.collect())
    #for coli in range(num_col):   
        #dataset_num_map = dataset_map.filter(lambda x)    

def process_datasets():
    df_datasets = spark.read.csv("/user/hm74/NYCOpenData/datasets.tsv",header=False,sep="\t")
    processed_dataset_cnt = 0
    for dataset_row in df_datasets.collect():
        dataset_fname = dataset_row[0]
        print(dataset_fname)
        dataset = spark.read.csv("/user/hm74/NYCOpenData/" + dataset_fname + ".tsv.gz", header = True, sep = "\t")
        process_dataset_rdd(dataset)
        processed_dataset_cnt += 1
        break
        #if(processed_dataset_cnt == 2):
        #    break


process_datasets()
