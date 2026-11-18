import math

import pyspark.sql
from pyspark.sql import functions as F
from pyspark.sql.functions import rand
from pyspark.sql.functions import lit
from pyspark.ml.feature import StringIndexer
from pyspark.sql.window import Window

spark = (
    pyspark.sql.SparkSession.builder
    .appName("Term Project")
    .getOrCreate()
)

spark.conf.set("spark.sql.caseSensitive", "true")

# Clean Dataset
raw_df = (spark.read.json("project/corpus/Movies_and_TV.jsonl")
          .drop("images", "asin", "timestamp", "helpful_vote"))

verified_df = (((raw_df.filter(F.col("verified_purchase") == True)
               .withColumn("full_review_text",
                F.concat_ws(". ", F.col("title"), F.col("text"))))
               .withColumnRenamed("parent_asin", "movie_id"))
               .withColumnRenamed("user_id", "reviewer_id"))

verified_df = (verified_df.drop("verified_purchase", "title", "text"))

pos_df = verified_df.filter((F.col("rating").isin(4.0, 5.0)))
neg_df = verified_df.filter(F.col("rating").isin(1.0, 2.0))

neutral_df = verified_df.filter(F.col("rating") == 3.0)

sentiment_df = pos_df.unionByName(neg_df, allowMissingColumns=True)

recs_df = sentiment_df.unionByName(neutral_df.withColumn("sentiment", lit(None)), allowMissingColumns=True)

meta_df = spark.read.json("project/corpus/meta_Movies_and_TV.jsonl")

# For getting the movie covers
poster_lookup_df = (
    meta_df.select(
        F.col("parent_asin").alias("movie_id"),
        F.col("title").alias("movie_title"),
        F.col("images")
    )
    .withColumn(
        "poster_url",
        F.coalesce(
            # Try physical media fields first
            F.element_at(F.col("images"), 1).getItem("large"),
            F.element_at(F.col("images"), 1).getItem("hi_res"),
            # Fallback to Prime Video streaming resolution keys
            F.element_at(F.col("images"), 1).getItem("1080w"),
            F.element_at(F.col("images"), 1).getItem("1920w"),
            F.element_at(F.col("images"), 1).getItem("360w")
        )
    )
    # Filter out rows where no valid poster URL could be found
    .filter(F.col("poster_url").isNotNull())
    .select("movie_id", "movie_title", "poster_url")
)

meta_clean = (
    meta_df.select("parent_asin", "title", "price")
    .withColumnRenamed("title", "movie_title")
    .withColumnRenamed("parent_asin", "movie_id")
)

sentiment_joined = sentiment_df.join(meta_clean, on="movie_id", how="inner")

shuffled_sentiment_df = sentiment_joined.repartition(50, rand(seed=42))

train_df, test_df = shuffled_sentiment_df.randomSplit([0.8, 0.2], seed=42)

print("Sentiment Training Set Count:", train_df.count())
train_df.show(5, truncate=False)


# 1. String Indexing for ALS Collaborative Filtering
# ALS requires integer indices for users and items
user_indexer = StringIndexer(inputCol="reviewer_id", outputCol="user_idx", handleInvalid="keep")
item_indexer = StringIndexer(inputCol="movie_id", outputCol="item_idx", handleInvalid="keep")

# Fit indexers on your full recommendation dataset
indexed_recs_df = user_indexer.fit(recs_df).transform(recs_df)
indexed_recs_df = item_indexer.fit(indexed_recs_df).transform(indexed_recs_df)

# Ensure rating is float for ALS matrix factorization
als_training_df = indexed_recs_df.withColumn("rating_float", F.col("rating").cast("float"))
print("ALS Training Matrix Count:", als_training_df.count())

# 2. Cold-Start Fallback Table Generation
# Calculate log-scaled popularity score using rating counts and average ratings
cold_start_df = (
    meta_df
    .withColumn("exploded_category", F.explode_outer(F.col("categories")))
    .withColumn("popularity_score", F.log1p(F.col("rating_number")) * F.col("average_rating"))
    .withColumnRenamed("parent_asin", "movie_id")
)

# Rank top 10 items per category using window functions
window_spec = Window.partitionBy("exploded_category").orderBy(F.col("popularity_score").desc())

top_fallback_df = (
    cold_start_df
    .withColumn("rank", F.row_number().over(window_spec))
    .filter(F.col("rank") <= 10)
    .select("exploded_category", "movie_id", "title", "popularity_score", "rank")
)

print("Cold-Start Fallback Table Sample:")
top_fallback_df.show(5, truncate=False)

from pyspark.ml.recommendation import ALS

# Initialize ALS model
als = ALS(
    maxIter=10,
    regParam=0.1,
    userCol="user_idx",
    itemCol="item_idx",
    ratingCol="rating_float",
    coldStartStrategy="drop",
    nonnegative=True
)

# Train the model on the full recommendation matrix
als_model = als.fit(als_training_df)

# Generate top 10 movie recommendations for all users
user_recs = als_model.recommendForAllUsers(10)
user_recs.show(5, truncate=False)


from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.classification import LogisticRegression

# Define NLP Feature Engineering & Classification Pipeline
tokenizer = Tokenizer(inputCol="full_review_text", outputCol="words")
hashingTF = HashingTF(inputCol="words", outputCol="features", numFeatures=10000)
lr = LogisticRegression(featuresCol="features", labelCol="sentiment", maxIter=10)

sentiment_pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

# Train the sentiment classifier on your 80% training split
sentiment_model = sentiment_pipeline.fit(train_df)

# Evaluate on test set
predictions = sentiment_model.transform(test_df)
predictions.select("full_review_text", "sentiment", "probability", "prediction").show(5, truncate=False)

predictions.show(10)



