package uk.co.newday.solutions

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Exercise1Candidate {

   case class Movie (movieId:Int, title:String, genre:String)
   case class Rating (userId:Int, movieId:Int, rating:Int, timestamp:Int)

  def execute(): (DataFrame, DataFrame) =
    {
        //Please load movies and ratings csv's in output dataframes.
      /** Not using dataset with case classes because of following reasons:
      1) Dataset to dataframe conversion required, which is again an overhead for performance
      2) Data files don't have headers, so column names need to be mapped again to case class column names
      */

      val spark = SparkSession
          .builder()
          .appName("MovieRatingExercise")
          .master("local")
          .getOrCreate()

      // Creating schema as per case Movie class names and data types
      val movieSchema = StructType(Array(
        StructField("movieId", IntegerType, true),
        StructField("title", StringType, true),
        StructField("genre", StringType, true)))

      // Read movies data file
      val movies = spark.read.option("header", "false")
        .option("delimiter", "::")
        .schema(movieSchema).
        csv("src/main/resources/movies.dat")

      //movies.show

      // Creating schema as per case Rating class names and data types
      val ratingSchema = StructType(Array(
        StructField("userId", IntegerType, true),
        StructField("movieId", IntegerType, true),
        StructField("rating", IntegerType, true),
        StructField("timestamp", IntegerType, true)))

      // Read ratings data file
      val ratings = spark.read.option("header", "false")
        .option("delimiter", "::")
        .schema(ratingSchema)
        .csv("src/main/resources/ratings.dat")

      //ratings.show

      (movies, ratings)
    }
}
