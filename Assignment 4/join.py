# file_R = 50000 tuples
# file_S = 10000 tuples
# val of M = 20
# block size = 100
import os
import sys
import time
import operator
from heapq import heapify, heappush, heappop

class MyObject(object):
    def __init__(self, val):
        self.val = val

    def __lt__(self, other):
        cmpr = self.val[3]
        if(self.val[0][cmpr] < other.val[0][cmpr]):
            return True
        elif self.val[0][cmpr] > other.val[0][cmpr]:
            return False
        return True


class SortMerge_Join:
    def __init__(self,inputR, inputS, M = 50):
        self.M = int(M)
        self.inputR = inputR
        self.inputS = inputS
        self.tempfiles = []
        self.orderR = [1]
        self.orderS = [0]
        self.block_size = 100
        self.max_tup = self.block_size * self.M
        self.output_fname = self.inputR+"_"+self.inputS+"_join.txt"

    def write_intermediate_file(self,count,tuple_list,input_file):
        filename = input_file +"_"+str(count)
        # print("Writing to file {}".format(filename))
        with open(filename, 'w') as f:
            for row in tuple_list:
                str1 = row[0]+" "+row[1].replace("\n"," ")+"\n"
                # str1 = ''
                # for column in row:
                #     str1 += column+' '
                #     # print(len(str1))
                # str1 = str1[:-2] + '\n'
                f.write(str1)
        return filename

    def open(self,input_file,input_path, code):
        count = 1
        fp1 = open(input_path, 'r')
        tuple_list = []
        fnn = []
        if(code == 1):
            order = self.orderR
        else:
            order = self.orderS
        while True:
            line = fp1.readline()
            if(line == ''):
                if(len(tuple_list) != 0):
                    tuple_list.sort(key = operator.itemgetter(*order))
                    # print("last chunk Sort Completed", input_file[-1]+str(count))
                    fn = self.write_intermediate_file(count,tuple_list,input_file)
                    fnn.append(fn)
                    count += 1
                break
            tuple_list.append(line.split(" "))
            if(len(tuple_list) == self.max_tup):
                tuple_list.sort(key = operator.itemgetter(*order))
                # print("Sort Completed ", input_file[-1]+str(count))
                fn = self.write_intermediate_file(count,tuple_list,input_file)
                fnn.append(fn)
                count += 1
                tuple_list = []
        self.tempfiles.append(fnn)
            
    def heap_push(self, temp_list, heap_list, f , code, length):
        #code = 1 for inputR code = 0 is for inputS
        for idx,tup in enumerate(temp_list):
            isleaf = True if idx + 1 == len(temp_list) else False
            heappush(heap_list, MyObject([tup,f,idx,code,length, isleaf]))
        return heap_list

    def read_block(self,fptr,heap_list,mode):
        i = 0
        temp_list = []
        # heap_list = []
        while i < self.block_size:
            tup = fptr.readline()
            if tup != '':
                temp_list.append(tup.replace("\n","").split(" "))
            if(tup == '' and len(temp_list) == 0):
                break
            if(tup == '' and len(temp_list) != 0) or (len(temp_list) == self.block_size):
                heap_list = self.heap_push(temp_list,heap_list,fptr,mode,len(temp_list))
                break
            i += 1
        return heap_list

    def getNext(self):
        B_R = (len(self.tempfiles[0]) * self.max_tup)/self.block_size
        B_S = (len(self.tempfiles[1]) * self.max_tup)/self.block_size
        if (B_R + B_S) > (self.M * self.M):
            print("memory constraint violated!")
            exit()
        
        of = open(self.output_fname, "w")
        fptr_list = []

        R_heap_list = []
        R_tempfiles = self.tempfiles[0]
        for tempfile in R_tempfiles:
            fptr_name = tempfile+"_fp"
            fptr_list.append(fptr_name)
            fptr_name = open(tempfile, "r")
            R_heap_list = self.read_block(fptr_name,R_heap_list,1)
        S_heap_list = []
        S_tempfiles = self.tempfiles[1]
        for tempfile in S_tempfiles:
            fptr_name = tempfile+"_fp"
            fptr_list.append(fptr_name)
            fptr_name = open(tempfile, "r")
            S_heap_list = self.read_block(fptr_name,S_heap_list,0)

        while(len(S_heap_list)>0 and len(R_heap_list)>0):
            rGS = False
            rLS = False
            objr = heappop(R_heap_list) 
            objs = heappop(S_heap_list)
            
            while(len(R_heap_list)>0 and objr.val[0][1] < objs.val[0][0]):
                objr = heappop(R_heap_list)
                if(objr.val[5] == True):
                    self.read_block(objr.val[1],R_heap_list,1)
                rLS = True
            
            while(len(S_heap_list)>0 and objr.val[0][1] > objs.val[0][0]):
                objs = heappop(S_heap_list)
                if(objs.val[5] == True):
                    self.read_block(objs.val[1],S_heap_list,0)
                rGS = True
            
            temp_list_s = []
            match_var = objr.val[0][1]
            while(len(S_heap_list)>0 and objr.val[0][1] == objs.val[0][0]):
                rGS = False
                rLS = False
                temp_list_s.append(objs.val[0])
                objs = heappop(S_heap_list)
                if(objs.val[5] == True):
                    self.read_block(objs.val[1],S_heap_list,0)

            if len(S_heap_list)>0:
                heappush(S_heap_list, MyObject(objs.val))
            
            temp_list_r = []
            while(len(R_heap_list)>0 and objr.val[0][1] == match_var):
                temp_list_r.append(objr.val[0])
                objr = heappop(R_heap_list)
                if(objr.val[5] == True):
                    self.read_block(objr.val[1],R_heap_list,1)

            if len(R_heap_list)>0:
                heappush(R_heap_list, MyObject(objr.val))
            elif(len(R_heap_list) == 0 and len(S_heap_list) == 0 and objr.val[0][1] == objs.val[0][0]):
                temp_list_r.append(objr.val[0])
                temp_list_s.append(objs.val[0])
            
            of = self.crossproduct(temp_list_r, temp_list_s, of) 
            if(rGS):
                heappush(R_heap_list, MyObject(objr.val))
            if(rLS):
                heappush(S_heap_list, MyObject(objs.val))


        of.close()
        
    def crossproduct(self, temp_list_r,temp_list_s, fptr):
        for i in range(len(temp_list_r)):
            for j in range(len(temp_list_s)):
                line = temp_list_r[i][0]+" "+temp_list_r[i][1]+" "+temp_list_s[j][1]+"\n"               
                fptr.write(line)
        return fptr

class hash_join:
    def __init__(self, inputR, inputS, M = 50):
        self.M = int(M)
        self.block_size = 100
        self.inputR = inputR
        self.inputS = inputS
        self.S_tempfiles = []
        self.R_tempfiles = []
        self.r_files = []
        self.s_files = []
        self.output_fname = self.inputR+"_"+self.inputS+"_join.txt"
        self.out = open(self.output_fname,'w')

    def _roll(self,str):
        p = 31
        m = self.M
        power_of_p = 1
        hash_val = 0
        for i in range(len(str)):
            hash_val = ((hash_val + (ord(str[i]) - ord('a') + 1) * power_of_p) % m)
            power_of_p = (power_of_p * p) % m
        return int(hash_val)

    def open(self,input_file,input_path,idx):
        fp = open(input_path, 'r')
        while True:
            line = fp.readline()
            if(line == ''):
                break
            hashed_val = self._roll(line.replace("\n","").split(" ")[idx])            
            f_name = input_file + "_"+str(hashed_val) 
            f_ptr = open(f_name, 'a')
            f_ptr.write(line)
            if idx == 0:
                self.s_files.append(f_name)
                self.S_tempfiles.append(f_ptr)
            else:
                self.r_files.append(f_name)
                self.R_tempfiles.append(f_ptr)
            f_ptr.close()

    
    def cmp_write(self):
        
        match = []
        for r in self.r_files:
            for s in self.s_files:
                if r[1:] == s[1:]:
                    match.append([r,s])
        newmatch = []
        myset = set()
        for m in match:
            if m[0] not in myset:
                myset.add(m[0])
                newmatch.append(m)

        match = newmatch 
        for m in match:
            f1 = open(m[0],'r')
            li = f1.readlines()
            li = [x.replace("\n", "").split(" ") for x in li]
            f2 = open(m[1],'r')
            while True:
                tup = f2.readline()
                if tup == '':
                    break
                tup = tup.replace("\n", "").split(" ")
                for each_row in li:
                    if each_row[1] == tup[0]:
                        self.out.write(each_row[0]+" "+tup[0]+" "+tup[1]+"\n")
        
        self.out.close()
        

if __name__ == "__main__":
    start = time.time()
    #input format 
    #<RollNumber.sh> <path of R file> <path of S file> <sort/hash> <M>
    cmd_input = sys.argv
    if len(cmd_input) != 5:
        print("\ninvalid input format")
        print("<RollNumber.sh> <path of R file> <path of S file> <sort/hash> <M>")
        exit()

    inputR_path, inputS_path = cmd_input[1], cmd_input[2]
    inputR = inputR_path.split('/')[-1]
    inputS = inputS_path.split('/')[-1]
    join_type = cmd_input[3]
    M = int(cmd_input[4])
    if M < 1:
        print("number of memory blocks can not be less than one")
        exit()
    
    if join_type == 'hash':
        hobj = hash_join(inputR, inputS, M)
        hobj.open(inputR,inputR_path,1)
        hobj.open(inputS,inputS_path,0)
        hobj.cmp_write()
        # deleting temporary intermediate files
        for tempFileNames in hobj.r_files:
            if os.path.exists(tempFileNames):
                os.remove(tempFileNames)
        for tempFileNames in hobj.s_files:
            if os.path.exists(tempFileNames):
                os.remove(tempFileNames)

    elif join_type == 'sort':
        sobj = SortMerge_Join(inputR, inputS, M)
        sobj.open(inputR,inputR_path, 1)
        sobj.open(inputS,inputS_path, 0)
        sobj.getNext()

    else:
        print("invalid join type")
        exit()

    end = time.time()
    print("--- Time taken ", (end-start))