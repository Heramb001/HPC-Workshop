

ratings_raw_RDD = sc.textFile('ratings.csv') 

ratings_RDD = ratings_raw_RDD.map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2])))

training_RDD, validation_RDD, test_RDD = ratings_RDD.randomSplit([3, 1, 1], 0) 

predict_validation_RDD = validation_RDD.map(lambda x: (x[0], x[1])) 
predict_test_RDD = test_RDD.map(lambda x: (x[0], x[1]))

#importing Spark
from pyspark.mllib.recommendation import ALS
import math

seed = 5
iterations = 10
regularisation = 0.1
ranks = [4, 8, 12]
errors = [0, 0, 0]
err = 0
min_error = float('inf')
best_rank = -1

