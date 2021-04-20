
import os
import sys
import heapq
import operator
import time
start = time.time()


class MyObject(object):
    def __init__(self, val, desc=False):
        self.val = val
        self.desc = desc

    def __lt__(self, other):
        if(not self.desc):
            for i in range(0, len(order)):
                if(self.val[0][order[i]] < other.val[0][order[i]]):
                    return True
                elif self.val[0][order[i]] > other.val[0][order[i]]:
                    return False
        else:
            for i in range(0, len(order)):
                if(self.val[0][order[i]] > other.val[0][order[i]]):
                    return True
                elif self.val[0][order[i]] < other.val[0][order[i]]:
                    return False

        return True

# ---------------------------------------------------------------------

def getTupleList(column_size, line):
    row = []
    start_index = 0
    for size in column_size:
        end_index = start_index + size
        row.append(line[start_index:end_index])
        start_index = end_index + 2
    return row
# ------------------------------------------------------------------------

def merge_files(num_of_files, no_of_tuples, order):  # num_of_files = intermediate files
    i = 0
    list1 = []
    closedFiles = set()
    while i < num_of_files:
        f = open("ifile"+str(i)+".txt", "r")
        tup = f.readline()
        tup = getTupleList(column_size, tup)
        heapq.heappush(list1, MyObject([tup, f], order))
        i += 1
    f2 = open(output_file+str(p)+".txt", "w")
    # try:
    while len(list1) > 0:
        obj = heapq.heappop(list1)
        str1 = ""
        for el in obj.val[0]:
            str1 += el+"  "
        str1 = str1[:-2]+"\r\n"
        f2.write(str1)
        f = obj.val[1]
        if(f not in closedFiles):
            tup = f.readline()
            if not tup:
                closedFiles.add(f)
                f.close()
            else:
                tup = getTupleList(column_size, tup)
                # print(tup)
                heapq.heappush(list1, MyObject([tup, f], order))
    f2.close()
   
# ----------------------------------------------------------------------------


# table each containing 9 rows(last table may contain <9 rows) will come here
def sort_subfiles(tuple_list, desc ):

    print("\t\t...")
    global i
    i = 0
    tuple_list.sort(key=operator.itemgetter(*order), reverse=desc)
    global j
    filename = 'ifile'+str(j)+'.txt'
    tempFiles.append(filename)
    with open(filename, 'w') as f:

        for row in tuple_list:
            str1 = ''
            for column in row:
                str1 += column+'  '
            str1 = str1[:-2] + '\r\n'
            f.write(str1)
    f.close()
    j += 1

# ----------------------------------------------------------------------
# input format = ./sort input.txt output.txt 50 asc C1 C2 (for part1)

# reading command line
cmd_input = sys.argv[1:]

input_file, output_file = cmd_input[0], cmd_input[1][:-4]
ram = int(cmd_input[2]) * 1024 * 1024
order_code = cmd_input[3]
order1 = cmd_input[4:]
desc = True if order_code.lower() == "desc" else False
order = []  # order of columns in which we have to sort our input.txt file
column_size = []
column_names = []
tempFiles = []
count = 0
tuple_list = []
temp_list = []
i = 0
j = 0
p = 0
num_of_files = 0

with open("metadata.txt", "r") as F:
    for line in F.readlines():
        line_list = line.split(",")
        column_names.append(line_list[0].strip())
        column_size.append(int(line_list[1].strip()))

if(not any(x in order1 for x in column_names)):
    print("columns does not exist in metadata")
    sys.exit()

for o in order1:
    for i in column_names:
        if o == i:
            order.append(column_names.index(i))
            

tuple_size = sum(column_size)+5
no_of_tuples = ram // tuple_size  # number of tuples or size of input runs
print("--- starting phase 1 ---")
file = open(input_file, 'r')  # 100 rows in input.txt
# print("Reading input file chunk wise with chunk size: ", no_of_tuples)
while True:
    line = file.readline()
    if not line:  # last chunk which can have no of tuples < 9 and
        if len(tuple_list) > 0:
            sort_subfiles(tuple_list, desc)
            break  # if tuple_list lenght is > 0 the call sort otherwise breakout

    count += 1
    tuple_list.append(getTupleList(column_size, line))
    # print(tuple_list)

    if count % (no_of_tuples) == 0:  # for chunk size = 9
        # print(len(tuple_list))
        sort_subfiles(tuple_list, desc)
        tuple_list = []

print("--- phase 1 completed ---")
print("--- start phase 2 ---")
merge_files(j, no_of_tuples, desc)

print("--- sorting completed ---")
# deleting temporary intermediate files
for tempFileNames in tempFiles:
    if os.path.exists(tempFileNames):
        os.remove(tempFileNames)
end = time.time()
print("--- Time taken ", (end-start))

