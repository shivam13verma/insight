
import sys
from collections import defaultdict, deque

args = []
for arg in sys.argv:
    args.append(arg)

batch_path = str(args[1])
stream_path = str(args[2])
out1_path = str(args[3])
out2_path = str(args[4])
out3_path = str(args[5])
#out4_path = str(args[5])

#batch_path = './paymo_input/batch_payment.txt'
#stream_path = './paymo_input/stream_payment.txt'
#out1_path = './paymo_output/output1.txt'
#out2_path = './paymo_output/output2.txt'
#out3_path = './paymo_output/output3.txt'
#out4_path = './paymo_output/output4.txt'

stream = []
batch = []

with open(batch_path,'r') as f:
    for line in f:
        batch.append(line)

with open(stream_path,'r') as f:
    for line in f:
        stream.append(line)



batch_list = map(lambda x: x.rstrip("\n").split(",")[1:3], batch)
stream_list = map(lambda x: x.rstrip("\n").split(",")[1:3], stream)

del batch_list[0]
del stream_list[0]

def list2Int(x):
    return tuple(map(lambda x: int(x), x))


batch_tup = map(lambda x: list2Int(x), batch_list)

batch_dict = defaultdict(set)

for s in batch_tup:
    batch_dict[s[0]].add(s[1])
    batch_dict[s[1]].add(s[0])


##Feature 1: Prints verification status if id1 -> id2 not in transaction history
with open(out1_path,'wb') as f:
    for x in stream_list[:100]:
        tup = tuple(map(int,x)) #tuple of ids as ints
        try:
            if tup[1] in batch_dict[tup[0]]: #if (id1,id2) have transacted
                f.write("trusted\n")
                continue
            else: #if not transacted
                f.write("unverified\n")
        except KeyError: #if id1 has not been seen before
            f.write("unverified\n")
            batch_dict[tup[0]].add(tup[1])
            pass


##Feature 2: Prints verification status if (id1,id2) have transacted or have a common friend
with open(out2_path,'wb') as f:
    for x in stream_list[:100]:
        tup = tuple(map(int,x))
        try:
            if tup[1] in batch_dict[tup[0]]:
                f.write("trusted\n")
                continue
            else:
                
                has_common = False
                
                for friend in batch_dict[tup[0]]:
                    if tup[1] in batch_dict[friend]: #common friend
                        has_common=True
                        f.write("trusted\n")
                        break
                        
                if not has_common:
                    f.write("unverified\n")
    
        except KeyError:
            batch_dict[tup[0]].add(tup[1])
            pass


def is_kth_friend(batch_dict, x, y, k=4):    
    """
    Returns True if y is in x's network within 'k' connections using Breadth-First Search on transaction graph.
    Args:
        batch_dict: Dictionary of form {id1: [set of ids transacted with]}
        x: id1
        y: id2
        k: degree
    Returns: True if y is within x's span within degree 'k'
    """
    
    def get_friends(x): #returns neighboring nodes or friends transacted.
        return list(batch_dict[x])
    
    friends = get_friends(x)
    visited = set([x])
    count = 0
    friend_list = deque([(x,get_friends(x))])
    is_friend = False

    while count<k:
        try:
            node, curr_friends = friend_list[0]
            for friend in curr_friends:
                if friend not in visited:
                    visited.add(friend)
                    friend_list.append((friend,get_friends(friend)))
                    if y in batch_dict[friend]:
                        is_friend = True
                        return True
        except IndexError:
            break
        friend_list.popleft()
        count+=1
    return is_friend


##Feature 3: Prints verification status if (id1,id2) have transacted or are within network of range 4
with open(out3_path,'wb') as f:
    
    for x in stream_list[:100]:
        tup = tuple(map(int,x))
        try:
            if tup[1] in batch_dict[tup[0]]:
                f.write("trusted\n")
            else:
                
                if is_kth_friend(batch_dict,tup[0],tup[1],4):
                    f.write("trusted\n")
                else:
                    f.write("unverified\n")
    
        except KeyError:
            batch_dict[tup[0]].append(tup[1])
            pass

##Feature 4: Prints verification status if (id1,id2) have transacted or are within network of range K. Uncomment below block to run, and also uncomment out4_path above.
#K = 10 #set as 10, say.
#with open(out4_path,'wb') as f:
#    
#    for x in stream_list[:100]:
#        tup = tuple(map(int,x))
#        try:
#            if tup[1] in batch_dict[tup[0]]:
#                f.write("trusted\n")
#            else:
#                
#                if is_kth_friend(batch_dict,tup[0],tup[1],K):
#                    f.write("trusted\n")
#                else:
#                    f.write("unverified\n")   
#        except KeyError:
#            batch_dict[tup[0]].append(tup[1])
#            pass

