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
    sequence_id = record[0]
    nucleotides = record[1]
    l = len(nucleotides)
    mr.emit_intermediate(1,nucleotides[0:l-10])
    

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    s = set(list_of_values)
    for f in s:
        mr.emit((key, f))
        

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
