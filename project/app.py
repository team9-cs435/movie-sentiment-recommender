import pyspark.sql
from pyspark.sql import functions as F


spark = (
    pyspark.sql.SparkSession.builder
    .master("local[*]")
    .appName("Term Project")
    .config("spark.locality.wait", "0")
    .getOrCreate()
)

spark.conf.set("spark.sql.caseSensitive", "true")

# Clean Dataset
raw_df = (spark.read.json("corpus/Movies_and_TV.jsonl")
          .drop("images", "asin", "timestamp", "helpful_vote"))

verified_df = (((raw_df.filter(F.col("verified_purchase") == True)
               .withColumn("full_review_text",
                F.concat_ws(". ", F.col("title"), F.col("text"))))
               .withColumnRenamed("parent_asin", "movie_id"))
               .withColumnRenamed("user_id", "reviewer_id"))

verified_df = (verified_df.drop("verified_purchase", "title", "text"))

# pos_df = verified_df.filter((F.col("rating").isin(4.0, 5.0)))
# neg_df = verified_df.filter(F.col("rating").isin(1.0, 2.0))
# neutral_df = verified_df.filter(F.col("rating") == 3.0)
#
# verified_df.show(5)
# neg_df.show(5)
# neutral_df.show(5)

# Clean Metadata
meta_df = spark.read.json("corpus/meta_Movies_and_TV.jsonl")
meta_clean = ((meta_df.select("parent_asin", "title", "price", "Directors")
              .withColumnRenamed("title", "movie_title"))
              .withColumnRenamed("parent_asin", "movie_id"))

# Combine main dataset with metadata
joined_df = verified_df.join(meta_clean, on="movie_id", how="inner")

joined_df = joined_df.select("movie_title",
                             "movie_id",
                             "rating",
                             "reviewer_id",
                             "full_review_text",
                             "price")

joined_df.show(5)
