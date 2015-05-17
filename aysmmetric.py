import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    personA = record[0]
    personB = record[1]    
    mr.emit_intermediate(personA,personB)
    mr.emit_intermediate(personB,personA)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
    s = set(list_of_values)
    for f in s:
        mr.emit((key, f))
        

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
