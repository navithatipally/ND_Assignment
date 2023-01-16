package uk.co.newday.solutions

import org.apache.spark.sql.DataFrame

object Exercise4Candidate {

  def execute(movies: DataFrame, ratings: DataFrame, movieRatings:DataFrame, ratingWithRankTop3:DataFrame) = {
    /** Write all the output in parquet format, as it is columnar, and compressed */
    movies.write.format("parquet").mode("overwrite").save("output/movies")
    ratings.write.format("parquet").mode("overwrite").save("output/ratings")
    ratingWithRankTop3.write.format("parquet").mode("overwrite").save("output/ratingWithRankTop3")
  }
}
