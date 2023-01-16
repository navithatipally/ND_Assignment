package uk.co.newday.solutions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Exercise2Candidate {

  def execute(movies: DataFrame, ratings: DataFrame): (DataFrame) = {
    //With two dataframe apply the join as specified in the exercise.
    /** Used Left join as some movie id's are test entries and can't be found in ratings data */
    movies.join(ratings, movies.col("movieId") === ratings.col("movieId"), "left")
      .drop(ratings.col("movieId"))
      .drop(ratings.col("timestamp"))
      .groupBy("movieId", "title", "genre")
      .agg(
        min("rating").as("min_rating"),
        max("rating").as("max_rating"),
        avg("rating").as("avg_rating")
      )
    //movies.show
    movies
  }
}
