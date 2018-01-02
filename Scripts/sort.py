import csv
import operator

file1 = open('database1.csv','r')
csv1 = csv.reader(file1)

sort = sorted(csv1,key=operator.itemgetter(0))

for line in sort:
	with open("database3.csv","a") as file2:
		file2_writer = csv.writer(file2)
		file2_writer.writerow(line)

file1.close()
file2.close()

