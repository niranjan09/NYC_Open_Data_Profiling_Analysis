from collections import Counter

task2_datasets = open('cluster3.txt')
dataset_file_names = task2_datasets.readline()[1:-2].split(',')
dataset_file_names = [x.replace("'", "").replace(" ", "").split('.')[0] for x in dataset_file_names]


col_list = []
for dataset_file_name in dataset_file_names:
    dataset_file_path = '/user/hm74/NYCOpenData/' + dataset_file_name + '.tsv.gz'
    dataset_df = spark.read.csv(dataset_file_path, header = True, sep = '\t')
    col_list.extend(dataset_df.columns)

col_ctr = Counter(col_list)
print(col_ctr)

