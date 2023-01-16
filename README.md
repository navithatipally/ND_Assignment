# movies_ratings
Steps to run this Spark application
1) Get all the dependencies
```
sbt run
```

2) Package to generate the jar
```
sbt package
```
3) Spark submit in local with dynamic executor allocation
```
spark-submit --master local \
--driver-memory 1G \
--executor-cores 5 \
--executor-memory 1G \
--conf spark.dynamicAllocation.minExecutors=1 \
--conf spark.dynamicAllocation.maxExecutors=5 \
--class uk.co.newday.MoviesRatingsCandidate \
[Absolute Path to jar]/movies_ratings_2.11-1.0.jar
```
