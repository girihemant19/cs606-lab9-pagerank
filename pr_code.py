from pyspark.context import SparkContext
import re,sys

links = SparkContext().textFile(sys.argv[1]).map(lambda x: tuple(re.split(r'\s+', x)[:2])).distinct()
#print(links.collect())
links = links.groupByKey()
#print(links.collect())

N =links.count()
initial_pr = 1.0/N
d = 0.80
c = (1 - d)/N
iterations = 40


ranks = links.map(lambda x: (x[0], initial_pr))

for _ in range(iterations):
	ranks = links.join(ranks).flatMap(lambda x: ((i, x[1][1]/len(x[1][0])) for i in x[1][0])).reduceByKey(lambda x, y: x + y).mapValues(lambda x: x *d + c)
#print(ranks.map(lambda x: (x[0],x[1])).collect())

val=ranks.map(lambda x: (x[0],x[1]))
val_sorted = val.sortBy(lambda a: a[1]).collect()
outfile = open('outfile.txt', 'w')
for item in val_sorted:
   outfile.write(str(item) + '\n')
outfile.close()

print('\nNode Ids of top 5 nodes with highest scores')
print(val_sorted[-5:])
print('\n')

print('\nBottom 5 nodes with lowest scores')
print(val_sorted[:5])
print('\n\n')
