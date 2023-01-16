package uk.co.newday.solutions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Exercise3Candidate {

  def execute(movies: DataFrame, ratings: DataFrame): (DataFrame) = {
    // Complete the exercise and show the top 3 movies per user.

    val userRank = Window.partitionBy(ratings.col("userId")).orderBy(ratings.col("rating").desc)

    /** As there are more than one movie for same user rating for each userId,
     assuming the requirement is to take top 3 movies in the insertion order of user ratings from rating data,
    hence row_number is used for ranking
    **/
    val user = ratings.withColumn("userRank", row_number().over(userRank))

    // Filter top 3 movies
    val top3Users = user.filter(col("userRank") < 4)

    // Join with movie dataframe to get title
    val top3UserMovies = top3Users.join(movies,
      top3Users.col("movieId") === movies.col("movieId"), "left")
      .drop(top3Users.col("movieId"))

    // Collect the top3 movie titles as a list
    val top3UserMoviesCollect = top3UserMovies.groupBy(("userId")).agg(collect_list("title").as("top_3_movies"))
    //top3UserMoviesCollect.show
    top3UserMoviesCollect
  }
}
