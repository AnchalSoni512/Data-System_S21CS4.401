#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 28 01:07:03 2021

@author: anchal_soni
"""
import os
import sys
import heapq
import operator
import time
import threading
start = time.time()
threads = []
threadStatus = []
tempFiles = []
fileList = []
threadCount = 1
nthreads = 1
fileread = threading.Lock()
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
    while i in fileList:
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
    # -----------------------------------------------------------------------
    # if order_code == 'desc':
    #     with open(output_file+str(p)+".txt", "r") as myfile:
    #         data = myfile.readlines()
    #         myfile.close()

    #     data_2 = data[::-1]

    #     with open(output_file+str(p)+".txt", "w") as myfile:
    #         myfile.writelines(data_2)
    #         myfile.close()

# ----------------------------------------------------------------------------

def ThreadSortAndStore(filePointer, nrows, desc, threadNumber):
    global j
    fileread.acquire()
    completed = False
    tuple_list = []
    for i in range(nrows):
        line = filePointer.readline()
        if(line == ''):
            completed = True
            break
        tup = getTupleList(column_size, line)
        tuple_list.append(tup)
    fileread.release()
    if(len(tuple_list) > 0):
        fileread.acquire()
        fileList.append(threadNumber)
        fileread.release()
        j += 1
        print("Thread {} is sorting file number {}".format(threadNumber, threadNumber))
        sort_subfiles(tuple_list, threadNumber, desc)
    threadStatus.append(completed)

# table each containing 9 rows(last table may contain <9 rows) will come here
def sort_subfiles(tuple_list, threadNumber, desc = False):

    print("\t\t...")
    global i
    i = 0
    tuple_list.sort(key=operator.itemgetter(*order), reverse=desc)
    print("Sort Completed")
    print("Writing to file {}".format(threadNumber))
    #global j
    filename = 'ifile'+str(threadNumber)+'.txt'
    tempFiles.append(filename)
    with open(filename, 'w') as f:

        for row in tuple_list:
            str1 = ''
            for column in row:
                str1 += column+'  '
            str1 = str1[:-2] + '\r\n'
            f.write(str1)
    f.close()
    #j += 1
    print("Done writing to file {} ".format(threadNumber))

# ----------------------------------------------------------------------
# input format = ./sort input.txt output.txt 50 asc C1 C2 (for part1)


# reading command line
cmd_input = sys.argv[1:]

input_file, output_file = cmd_input[0], cmd_input[1][:-4]
ram = int(cmd_input[2]) * 1000 * 1000
order_code = cmd_input[3]
orderIndex = 4
for index, commandLineIp in enumerate(cmd_input):
    if("asc" in commandLineIp or "desc" in commandLineIp):
        orderIndex = index
        break
order1 = cmd_input[orderIndex:]
nthreads = 1 if orderIndex == 3 else int(cmd_input[orderIndex - 1])
desc = True if order1[0].lower() == "desc" else False
order = []  # order of columns in which we have to sort our input.txt file
column_size = []
column_names = []

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


if(all(x in order1 for x in column_names)):
    print("columns does not exist in metadata")
    sys.exit()

for o in order1:
    for i in column_names:
        if o == i:
            order.append(column_names.index(i))

tuple_size = sum(column_size)+5
no_of_tuples = ram // (tuple_size * nthreads)  # number of tuples or size of input runs
print("--- starting phase 1 ---")
file = open(input_file, 'r')  # 100 rows in input.txt
# print("Reading input file chunk wise with chunk size: ", no_of_tuples)

while True:
    # line = file.readline()
    # if not line:  # last chunk which can have no of tuples < 9 and
    #     if len(tuple_list) > 0:
    #         sort_subfiles(tuple_list, desc)
    #         break  # if tuple_list lenght is > 0 the call sort otherwise breakout
    for i in range(nthreads):
        t = threading.Thread(target=ThreadSortAndStore, args=(file, no_of_tuples, desc , i))
        threadCount += 1
        threads.append(t)
        t.start()
    for thread in threads:
        thread.join()
    threads = []
    for val in threadStatus:
        completed = True
        if(val == False):
            completed = False
    threadStatus = []
    if(completed):
        break
file.close()
    # count += 1
    # tuple_list.append(getTupleList(column_size, line))
    # # print(tuple_list)

    # if count % (no_of_tuples) == 0:  # for chunk size = 9
    #     # print(len(tuple_list))
    #     sort_subfiles(tuple_list, desc)
    #     tuple_list = []
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

# ------------------------------------------------

# used " \n" for storing but it was not allowed
# mm size check
