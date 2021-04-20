from __future__ import annotations
from math import floor
from random import randint
import sys


class Node:
    parent = None
    keys = []
    values = []

    def __init__(self, order):
        self.order = order

    def split(self) -> Node:  # Split a full Node to two new ones.
        left = Node(self.order)
        right = Node(self.order)
        mid = self.order // 2
        mid = int(mid)

        left.parent = self
        right.parent = self
        
        l1 = []
        for i in range(0,mid):
            l1.append(self.keys[i])
        left.keys = l1
        
        l2 = []
        for i in range(0,mid+1):
            l2.append(self.values[i])
        left.values = l2
        
        l3 = []
        for i in range(mid+1,len(self.keys)):
            l3.append(self.keys[i])
        right.keys = l3
        
        l4 = []
        for i in range(mid+1,len(self.values)):
            l4.append(self.values[i])
        right.values = l4


        self.values = [left, right]  # Setup the pointers to child nodes.
        self.keys = [self.keys[mid]]  # Hold the first element from the right subtree.

        # Setup correct parent for each child node.
        for i in range(len(left.values)):
            if isinstance(left.values[i], Node):
                left.values[i].parent = left

        for i in range(len(right.values)):
            if isinstance(right.values[i], Node):
                right.values[i].parent = right
                
        return self


class LeafNode(Node):
    def __init__(self, order):
        super().__init__(order)
        
        self.prevLeaf = None
        self.nextLeaf = None

    def add(self, key, value):
        if len(self.keys) == 0:  # Insert key if it doesn't exist
            self.keys.append(key)
            self.values.append([value])
            return

        for i in range(len(self.keys)):  # Otherwise, search key and append value.
            item = self.keys[i]
            if key == item:  # Key found => Append Value
                # self.values[i][0] = int(value)+1
                self.values[i][0] += 1
                break

            elif key < item:  # Key not found && key < item => Add key before item.
                self.keys.insert(i , key)
                self.values.insert(i , [1])
                break

            elif i + 1 == len(self.keys):  # Key not found here. Append it after.
                self.keys.append(key)
                self.values.append([value])
                break
            else:
                pass

    def split(self) -> Node:  # Split a full leaf node. (Different method used than before!)
        top = Node(self.order)
        right = LeafNode(self.order)
        mid = self.order // 2
        mid = int(mid)

        self.parent = top
        right.parent = top
        
        l1 = []
        for i in range(mid,len(self.keys)):
            l1.append(self.keys[i])
        right.keys = l1
        
        l2 = []
        for i in range(mid,len(self.values)):
            l2.append(self.values[i])
        right.values = l2

        # right.keys = self.keys[mid:]
        # right.values = self.values[mid:]
        right.prevLeaf = self
        right.nextLeaf = self.nextLeaf

        top.keys = [right.keys[0]]
        top.values = [self, right]  # Setup the pointers to child nodes.
        
        l3 = []
        for i in range(0,mid):
            l3.append(self.keys[i])
        self.keys = l3
        
        l4 = []
        for i in range(0,mid):
            l4.append(self.values[i])
        self.values = l4

        # self.keys = self.keys[:mid]
        # self.values = self.values[:mid]
        self.nextLeaf = right  # Setup pointer to next leaf

        return top  # Return the 'top node'


class BPlusTree(object):
    def __init__(self, order=3):
        self.root = LeafNode(order)  # First node must be leaf (to store data).
        self.order = int(order)
        
    def find_keys2(self, key):
        node = self.root
        while not isinstance(node, LeafNode):  # While we are in internal nodes... search for leafs.
            node, _ = self._find(node, key)
        if key in node.keys:
            return 'YES'
        else:
            return 'NO'
    
    def get_count2(self,key):
        node = self.root
        # while not isinstance(node, LeafNode):  # While we are in internal nodes... search for leafs.
        while not type(node) ==  LeafNode:
            node, _ = self._find(node, key)
        # print(node.keys)
        if key in node.keys:
            return node.values[node.keys.index(key)][0]
        else:
            return '0'
        
    
    def find_range2(self,x,y):

        if x > y:
            x, y = y, x
        node = self.root
        temp_list = []
        count_key = 0
        
        # while not isinstance(node, LeafNode):
        while not type(node) ==  LeafNode:
            node = node.values[0]
            
        while node:
            for i in range(len(node.keys)):
                if x <= node.keys[i] and node.keys[i] <= y:
                    temp_list.append(node.keys[i])
                    count_key += node.values[i][0]
                # print(node.keys[i], end= ' ')
            node = node.nextLeaf

        return count_key


    def _find(self,node, key):
        # i = 0
        # for i, item in enumerate(node.keys):
        for i in range(len(node.keys)):
            item = node.keys[i]
            if key < item:
                return node.values[i], i
            elif i + 1 == len(node.keys):
                return node.values[i + 1], i + 1  # return right-most node/pointer.
            else:
                pass
            # i += 1

    def _mergeUp(self, parent, child, index):
        parent.values.pop(index)
        pivot = child.keys[0]

        for i in range(len(child.values)):
            if isinstance(child.values[i], Node):
                child.values[i].parent = parent

        for i in range(len(parent.keys)):
            item = parent.keys[i]
            if pivot < item:
                templ1 = parent.keys[:i] + [pivot] + parent.keys[i:]
                parent.keys = templ1
                templ2 = parent.values[:i] + child.values + parent.values[i:]
                parent.values = templ2
                break
            
            elif len(parent.keys) == i + 1:
                parent.keys += [pivot]
                parent.values += child.values
                break
            else:
                pass


    def insert(self, key, value):
        node = self.root

        while not isinstance(node, LeafNode):  # While we are in internal nodes... search for leafs.
            node, index = self._find(node, key)

       
        if isinstance(node, LeafNode):
            node.add(key, value)

        if len(node.keys) == node.order:
            while True:  
                if not node.parent is None:
                    parent = node.parent
                    node = node.split()  
                    _ , index = self._find(parent, node.keys[0])
                    self._mergeUp(parent, node, index)
                    node = parent
                else:
                    node = node.split()  
                    self.root = node  
                if len(node.keys) != node.order:
                    break

if __name__ == '__main__':
    bpt = BPlusTree(order=3)
    filename = sys.argv[1]
    o = open('testout.txt', 'w')
    with open(filename, 'r') as file:
        while True:
            line = file.readline()
            if not line:  
                break  
            command = line.split(" ")
            # print(command)
            if command[0] == 'INSERT':
                bpt.insert(int(command[1]),1)
            elif command[0] == 'FIND':
                ans = (bpt.find_keys2(int(command[1])))
                print(ans)
                o.write(str(ans)+"\n")
            elif command[0] == 'COUNT':
                ans = bpt.get_count2(int(command[1]))
                print(ans)
                o.write(str(ans)+"\n")
            elif command[0] == 'RANGE':
                ans = (bpt.find_range2(int(command[1]), int(command[2])))
                print(ans)
                o.write(str(ans)+"\n")
    o.close()
    # bpt.printTree()
    # bpt.showAllData()
    print()
#b+tree implementation in python
