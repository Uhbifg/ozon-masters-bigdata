import os
import sys
from pyspark import SparkContext, SparkConf


conf = SparkConf()
sc = SparkContext(appName="hw_3_Uhbifg", conf=conf)
os.environ["PYTHONHASHSEED"] = "42"
start_node = sys.argv[1]
end_node = sys.argv[2]
path_data = sys.argv[3]
path_ans = sys.argv[4]

raw_graph = sc.textFile(path_data)
graph = raw_graph.map(lambda x: map(int, x.split("\t")[::-1])).cache()
links = graph.groupByKey().mapValues(list).cache()     

w = dict(links.collect())
def search(x1, x2, x3, seen):
    ans = list()
    a = x3[x1]
    for i in a:
        if i not in seen:
            ans.append(tuple([i, x2 + [i]]))
    return ans


links = graph.groupByKey().mapValues(list).cache()     
answers = []
pathes = sc.parallelize([(start_node, [start_node])])
seen = set([start_node])

while True:
    ans = pathes.filter(lambda x : x[0] == end_node)
    if ans.count() > 0:
        break
    else:
        rdd2 = pathes.map(lambda x : x[0], preservesPartitioning=True).distinct().collect()
        neigbours = dict() 
        for i in rdd2:
            neigbours[i] = w.get(i, [])
        pathes = sc.parallelize(pathes.flatMap(lambda x : search(x[0], x[1], neigbours, seen), preservesPartitioning=True).collect())
        seen.update(pathes.map(lambda x : x[0]).distinct().collect())
ans = ans.map(lambda x : x[1])

def toCSVLine(data):
    return ','.join(str(d) for d in data)

lines = ans.map(toCSVLine)
lines.saveAsTextFile(path_ans)

sc.stop()