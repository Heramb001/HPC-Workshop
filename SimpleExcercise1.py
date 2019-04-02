# HPC WorkShop
# Spark Simple Excercise Solutions - 1
# Steps to initialise Spark
# If you are starting from scratch on the login node: 
# 1) interact  
# 2) cd BigData/Shakespeare  
# 3) module load spark  
# 4) pyspark 
# read the File
rdd = sc.textFile("Complete_Shakespeare.txt")

# Count the number of lines
lines_rdd = sc.textFile("Complete_Shakespeare.txt") 
lines_rdd.count()

# Count the number of words (hint: Python "split" is a workhorse)
words_rdd = lines_rdd.flatMap(lambda x: x.split()) 
words_rdd.count() 

# Count unique words
words_rdd.distinct().count()

# Count the occurrence of each word


# Show the top 5 most frequent words
