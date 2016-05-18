
# coding: utf-8

# Name: Alex Egg  
# Email: eggie5@gmail.com
# PID: A53112354

from pyspark import SparkContext
sc = SparkContext()


from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

from string import split,strip

from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel, RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils


# Read the file into an RDD
path='/covtype/covtype.data'
inputRDD=sc.textFile(path)
# In[46]:

# Transform the text RDD into an RDD of LabeledPoints
Data=inputRDD.map(lambda line: [float(strip(x)) for x in line.split(',')]).map(lambda a: LabeledPoint(a[-1], a[0:-1]))

# ### Making the problem binary

Label=2.0
mapping = {Label: 1.0}
Data=inputRDD.map(lambda line: [float(x) for x in line.split(',')]).map(lambda a: LabeledPoint(mapping.setdefault(a[-1], 0.0), a[0:-1]))


# ### test/train split

# Data1=Data.sample(False,0.1).cache()
# (trainingData,testData)=Data1.randomSplit([0.7,0.3])
(trainingData,testData)=Data.randomSplit([0.7,0.3],seed=255)


# In[74]:

# from time import time
# errors={}
# for depth in [10]:
#     start=time()
#     model=GradientBoostedTrees.trainClassifier(trainingData, categoricalFeaturesInfo={}, numIterations=10, maxDepth=depth)
#     #print model.toDebugString()
#     errors[depth]={}
#     dataSets={'train':trainingData,'test':testData}
#     for name in dataSets.keys():  # Calculate errors on train and test sets
#         data=dataSets[name]
#         Predicted=model.predict(data.map(lambda x: x.features))
#
#         LabelsAndPredictions = data.map(lambda lp: lp.label).zip(Predicted)
#         Err = LabelsAndPredictions.filter(lambda (v,p): v != p).count()/float(data.count())
#         errors[depth][name]=Err
#     print depth,errors[depth],int(time()-start),'seconds'




# ### Random Forests
from time import time
errors={}
for depth in [20]:
    start=time()
    model = RandomForest.trainClassifier(trainingData, numClasses=2, categoricalFeaturesInfo={},
                                     numTrees=10, featureSubsetStrategy="auto",
                                     impurity='gini', maxDepth=depth)
    #print model.toDebugString()
    errors[depth]={}
    dataSets={'train':trainingData,'test':testData}
    for name in dataSets.keys():  # Calculate errors on train and test sets
        data=dataSets[name]
        Predicted=model.predict(data.map(lambda x: x.features))

        LabelsAndPredictions = data.map(lambda lp: lp.label).zip(Predicted)
        Err = LabelsAndPredictions.filter(lambda (v,p): v != p).count()/float(data.count())
        errors[depth][name]=Err
    print depth,errors[depth],int(time()-start),'seconds'





