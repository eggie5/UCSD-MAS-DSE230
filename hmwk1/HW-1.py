# Name: Alex Egg
# Email: eggie5@gmail.com
# PID: A53112354
from pyspark import SparkContext
sc = SparkContext()

# In[1]:

# import findspark
# findspark.init()
# import pyspark
# sc = pyspark.SparkContext()


# In[6]:

textRDD = sc.newAPIHadoopFile('/data/Moby-Dick.txt',
                              'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
                              'org.apache.hadoop.io.LongWritable',
                              'org.apache.hadoop.io.Text',
                               conf={'textinputformat.record.delimiter': "\r\n\r\n"}) \
            .map(lambda x: x[1])
            
            
            


sentences=textRDD.flatMap(lambda x: x.split(". ")).map(lambda x: x.encode('utf-8'))


# In[18]:

def find_ngrams(input_list, n):
  return zip(*[input_list[i:] for i in range(n)])


# In[79]:

import string
replace_punctuation = string.maketrans(string.punctuation, ' '*len(string.punctuation))
sentences.map(lambda x: ' '.join(x.split()).lower())    .map(lambda x: x.translate(None, string.punctuation))    .flatMap(lambda x: find_ngrams(x.split(" "), 5))    .map(lambda x: (x,1))    .reduceByKey(lambda x,y: x+y)    .map(lambda x:(x[1],x[0]))     .sortByKey(False)    .take(100)

def printOutput(n,freq_ngramRDD):
    top=freq_ngramRDD.take(5)
    print '\n============ %d most frequent %d-grams'%(5,n)
    print '\nindex\tcount\tngram'
    for i in range(5):
        print '%d.\t%d: \t"%s"'%(i+1,top[i][0],' '.join(top[i][1]))






# In[86]:

for n in range(1,6):
    # Put your logic for generating the sorted n-gram RDD here and store it in freq_ngramRDD variable
    freq_ngramRDD = sentences.map(lambda x: x.lower())    .map(lambda x: x.translate(replace_punctuation))    .flatMap(lambda x: find_ngrams(' '.join(x.split()).split(" "), n))    .map(lambda x: (x,1))    .reduceByKey(lambda x,y: x+y)    .map(lambda x:(x[1],x[0]))     .sortByKey(False)
    printOutput(n,freq_ngramRDD)

