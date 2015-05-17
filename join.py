import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    table = record[0]
    order_id = record[1]    
    data = [table]
    if table == "line_item":
        for i in range(17):
            if(i>1):
                data.append(record[i])
    elif table =="order":
        for i in range(10):
            if(i>1):
                data.append(record[i])     
       
    mr.emit_intermediate(order_id, data)

def reducer(key, list_of_values):
    data = []
    for l in list_of_values :
              
        list_length =len(data)
        print l[0]
        if l[0]=='order':
            data[0:9]=l[0:]             
            
        elif l[0] =='line_item':
            if list_length == 0:               
                data[0:16]=l[0:]              
            else :               
                data[9:16]=l[0:]
    data.insert(1,key)           
    mr.emit((None, data))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
