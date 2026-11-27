"""Load, clean and join the Amazon Reviews 2023 Movies_and_TV data.

Reads the raw review + metadata JSONL and writes two Parquet tables that every
later stage (temporal analysis, K-Means++, ALS, sentiment model, app) reads:

  <out>/reviews/   one row per review, partitioned by review_year
  <out>/items/     one row per product (parent_asin), incl. n_reviews / first_review_date
                   computed from the cleaned reviews (items with 0 reviews are kept)
  <out>/stats/      JSON row counts at every cleaning step (for the report)

Usage (cluster):  ./run_preprocess.sh [--quick]
Usage (local):    python preprocess.py --data <dir with the two .jsonl> --out <dir> --local
"""
import argparse
import json
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, LongType,
                               MapType, StringType, StructField, StructType)

REVIEW_FILE = "Movies_and_TV.jsonl"
META_FILE = "meta_Movies_and_TV.jsonl"

# Reviews collected up to Sep 2023 (per dataset docs); Amazon reviews start ~1996.
MIN_TS = "1996-01-01"
MAX_TS = "2023-10-01"

# Explicit schemas: skip a full schema-inference pass over the JSON, and avoid
# conflicting inferred types/case-colliding keys in `details`.
REVIEW_SCHEMA = StructType([
    StructField("rating", DoubleType()),
    StructField("title", StringType()),
    StructField("text", StringType()),
    StructField("asin", StringType()),
    StructField("parent_asin", StringType()),
    StructField("user_id", StringType()),
    StructField("timestamp", LongType()),           # epoch milliseconds
    StructField("helpful_vote", LongType()),
    StructField("verified_purchase", BooleanType()),
    StructField("_corrupt_record", StringType()),
])

META_SCHEMA = StructType([
    StructField("main_category", StringType()),
    StructField("title", StringType()),
    StructField("average_rating", DoubleType()),
    StructField("rating_number", LongType()),
    StructField("features", ArrayType(StringType())),
    StructField("price", StringType()),              # mostly float, sometimes text; cast below
    StructField("images", ArrayType(MapType(StringType(), StringType()))),
    StructField("store", StringType()),
    StructField("categories", ArrayType(StringType())),
    # values are a mix of strings and lists; non-strings arrive as raw JSON text
    StructField("details", MapType(StringType(), StringType())),
    StructField("parent_asin", StringType()),
    StructField("_corrupt_record", StringType()),
])


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data", default="hdfs:///user/wilvalen/final_project/raw",
                   help="directory holding Movies_and_TV.jsonl and meta_Movies_and_TV.jsonl")
    p.add_argument("--out", default="hdfs:///user/wilvalen/final_project/clean")
    p.add_argument("--quick", action="store_true",
                   help="smoke test on a 1%% review sample, writes to <out>_quick")
    p.add_argument("--sample", type=float, default=None, help="review sampling fraction (overrides --quick)")
    p.add_argument("--local", action="store_true", help="run with master local[*]")
    return p.parse_args()


def clean_text(col):
    """Strip HTML tags/entities Amazon leaves in review bodies and collapse whitespace."""
    c = F.regexp_replace(col, r"(?i)<br\s*/?>", " ")
    c = F.regexp_replace(c, r"<[^>]+>", " ")
    for ent, ch in [("&nbsp;", " "), ("&quot;", '"'), ("&#39;", "'"), ("&amp;", "&")]:
        c = F.replace(c, F.lit(ent), F.lit(ch))
    return F.trim(F.regexp_replace(c, r"\s+", " "))


def load_reviews(spark, path, stats, sample):
    raw = spark.read.schema(REVIEW_SCHEMA).option("mode", "PERMISSIVE").json(path)
    if sample:
        raw = raw.sample(fraction=sample, seed=42)
    ts = F.timestamp_millis("timestamp")
    df = (raw
          .withColumn("ok_parse", F.col("_corrupt_record").isNull())
          .withColumn("ok_ids", F.col("ok_parse") & (F.length("user_id") > 0) & (F.length("parent_asin") > 0))
          .withColumn("ok_rating", F.col("ok_ids") & F.col("rating").isin(1.0, 2.0, 3.0, 4.0, 5.0))
          .withColumn("ok_ts", F.col("ok_rating") & (ts >= F.lit(MIN_TS).cast("timestamp"))
                      & (ts < F.lit(MAX_TS).cast("timestamp"))))
    # All step counts in a single pass over the raw JSON
    c = df.agg(F.count("*").alias("raw"),
               *[F.sum(F.coalesce(F.col(k), F.lit(False)).cast("long")).alias(k)
                 for k in ["ok_parse", "ok_ids", "ok_rating", "ok_ts"]]).first()
    stats.update({"reviews_raw": c["raw"], "reviews_malformed": c["raw"] - (c["ok_parse"] or 0),
                  "reviews_after_null_ids": c["ok_ids"], "reviews_after_valid_rating": c["ok_rating"],
                  "reviews_after_valid_timestamp": c["ok_ts"]})

    df = df.filter(F.coalesce(F.col("ok_ts"), F.lit(False))).withColumn("review_ts", ts)
    # The same review often appears once per variant (DVD/Blu-ray/...) of a title,
    # all sharing parent_asin. Keep one row per (user, title, timestamp); the
    # number removed is reviews_after_valid_timestamp - reviews_final.
    df = df.dropDuplicates(["user_id", "parent_asin", "timestamp"])

    title = clean_text(F.coalesce(F.col("title"), F.lit("")))
    text = clean_text(F.coalesce(F.col("text"), F.lit("")))
    df = (df
          .withColumn("review_title", title)
          .withColumn("review_text", text)
          .withColumn("has_text", F.length(text) > 0)
          .withColumn("full_review_text",
                      F.when(F.length(title) > 0, F.concat_ws(". ", title, text)).otherwise(text))
          # 1 = positive (4-5 stars), 0 = negative (1-2), null = neutral 3 stars
          .withColumn("sentiment", F.when(F.col("rating") >= 4, 1.0)
                                    .when(F.col("rating") <= 2, 0.0))
          .withColumn("review_date", F.to_date("review_ts"))
          .withColumn("review_year", F.year("review_ts"))
          .withColumn("review_month", F.month("review_ts"))
          .withColumn("review_year_month", F.date_format("review_ts", "yyyy-MM"))
          .withColumnRenamed("user_id", "reviewer_id")
          .withColumnRenamed("parent_asin", "movie_id")
          .withColumn("helpful_vote", F.coalesce("helpful_vote", F.lit(0)))
          .withColumn("verified_purchase", F.coalesce("verified_purchase", F.lit(False))))
    return df.select("reviewer_id", "movie_id", "rating", "sentiment", "review_ts", "review_date",
                     "review_year", "review_month", "review_year_month", "helpful_vote",
                     "verified_purchase", "has_text", "review_title", "review_text", "full_review_text")


def try_int(col):
    """Cast to int, null on failure (ANSI mode on Spark 4 would otherwise raise)."""
    return F.when(col.rlike(r"^\d+$"), col.cast("int"))


def year_from(col):
    """First 4-digit 19xx/20xx year in a string column, as int (null if none)."""
    y = try_int(F.regexp_extract(col, r"\b((?:19|20)\d{2})\b", 1))
    return F.when(y.between(1900, 2023), y)


def load_items(spark, path, stats):
    raw = spark.read.schema(META_SCHEMA).option("mode", "PERMISSIVE").json(path)
    stats["items_raw"] = raw.count()
    df = raw.filter(F.col("_corrupt_record").isNull() & F.col("parent_asin").isNotNull()).drop("_corrupt_record")
    df = df.dropDuplicates(["parent_asin"])
    stats["items_clean"] = df.count()

    d = F.col("details")
    # Prime Video lists the original year as a bare feature, e.g. ["IMDb 6.8", "2013", ...]
    feature_year = F.expr("""try_cast(try_element_at(filter(features, f -> f rlike '^(19|20)[0-9]{2}$'), 1) AS INT)""")
    feature_year = F.when(feature_year.between(1900, 2023), feature_year)
    # Physical media: date of *that edition's* release (can be a re-release of an older film)
    edition_year = F.coalesce(year_from(d["Release date"]), year_from(d["Publication Date"]),
                              year_from(d["Date First Available"]))
    first_img = F.expr("try_element_at(images, 1)")

    df = (df
          .withColumn("release_year", F.coalesce(feature_year, edition_year))
          .withColumn("release_year_source",
                      F.when(feature_year.isNotNull(), "prime_video_year")
                       .when(edition_year.isNotNull(), "edition_release_date"))
          .withColumn("price", F.expr("try_cast(regexp_replace(price, '[$,]', '') AS DOUBLE)"))
          .withColumn("poster_url", F.coalesce(first_img["large"], first_img["hi_res"], first_img["1080w"],
                                               first_img["720w"], first_img["480w"], first_img["360w"],
                                               first_img["thumb"]))
          .withColumn("genre", d["Genre"])
          .withColumn("mpaa_rating", F.coalesce(d["MPAA rating"], d["Rating"]))
          .withColumn("language", d["Language"])
          .withColumn("media_format", F.coalesce(d["Format"], d["Media Format"]))
          .withColumnRenamed("parent_asin", "movie_id")
          .withColumnRenamed("title", "movie_title")
          .withColumn("categories", F.coalesce("categories", F.array().cast("array<string>"))))
    return df.select("movie_id", "movie_title", "main_category", "categories", "genre", "mpaa_rating",
                     "language", "media_format", "store", "price", "average_rating", "rating_number",
                     "release_year", "release_year_source", "poster_url")


def main():
    args = parse_args()
    sample = args.sample if args.sample is not None else (0.01 if args.quick else None)
    out = args.out + ("_quick" if args.quick and not args.out.endswith("_quick") else "")

    builder = SparkSession.builder.appName("CS435 Final - preprocess")
    if args.local:
        builder = builder.master("local[*]").config("spark.driver.memory", "4g")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    t0 = time.time()
    stats = {"sample_fraction": sample}
    data = args.data.rstrip("/")
    items = load_items(spark, f"{data}/{META_FILE}", stats)
    reviews = load_reviews(spark, f"{data}/{REVIEW_FILE}", stats, sample)

    # Product age at review time; negative = pre-order / edition re-dated after review
    reviews = (reviews.join(items.select("movie_id", "release_year", F.lit(True).alias("has_metadata")),
                            "movie_id", "left")
               .withColumn("has_metadata", F.coalesce("has_metadata", F.lit(False)))
               .withColumn("product_age_years", F.col("review_year") - F.col("release_year")))

    # Spread each year over up to 8 files so no single task writes a whole year (~3.6M rows in 2016)
    (reviews.withColumn("_salt", F.pmod(F.hash("movie_id"), F.lit(8)))
     .repartition("review_year", "_salt").drop("_salt")
     .write.mode("overwrite").partitionBy("review_year").parquet(f"{out}/reviews"))
    r = spark.read.parquet(f"{out}/reviews")

    # Release year is missing for ~half the catalogue (mostly Prime Video titles), so also
    # store each item's first review date on Amazon: an age proxy available for every item.
    item_activity = r.groupBy("movie_id").agg(
        F.min("review_date").alias("first_review_date"),
        F.count("*").alias("n_reviews"),
        F.round(F.avg("rating"), 4).alias("mean_rating"))
    (items.join(item_activity, "movie_id", "left")
     .withColumn("first_review_year", F.year("first_review_date"))
     .withColumn("n_reviews", F.coalesce("n_reviews", F.lit(0)))
     .coalesce(8).write.mode("overwrite").parquet(f"{out}/items"))

    # Summary stats read back from Parquet (cheap) for the report
    agg = r.agg(F.count("*").alias("n"), F.countDistinct("reviewer_id").alias("users"),
                F.countDistinct("movie_id").alias("movies"), F.avg("rating").alias("mean_rating"),
                F.avg(F.col("has_text").cast("int")).alias("frac_with_text"),
                F.avg(F.col("verified_purchase").cast("int")).alias("frac_verified"),
                F.avg(F.col("release_year").isNotNull().cast("int")).alias("frac_with_release_year"),
                F.avg(F.col("has_metadata").cast("int")).alias("frac_with_metadata"),
                F.min("review_date").alias("first_review"), F.max("review_date").alias("last_review")).first()
    stats.update({"reviews_final": agg["n"], "distinct_users": agg["users"], "distinct_movies": agg["movies"],
                  "mean_rating": round(agg["mean_rating"], 4), "frac_with_text": round(agg["frac_with_text"], 4),
                  "frac_verified": round(agg["frac_verified"], 4),
                  "frac_reviews_with_release_year": round(agg["frac_with_release_year"], 4),
                  "frac_reviews_with_metadata": round(agg["frac_with_metadata"], 4),
                  "first_review": str(agg["first_review"]), "last_review": str(agg["last_review"])})
    stats["rating_distribution"] = {str(k): v for k, v in
                                    r.groupBy("rating").count().orderBy("rating").collect()}
    stats["reviews_per_year"] = {str(k): v for k, v in
                                 r.groupBy("review_year").count().orderBy("review_year").collect()}
    stats["release_year_source"] = {str(k): v for k, v in
                                    spark.read.parquet(f"{out}/items").groupBy("release_year_source")
                                    .count().collect()}
    stats["seconds"] = round(time.time() - t0, 1)

    text = json.dumps(stats, indent=2)
    print(text)
    spark.createDataFrame([(text,)], "value string").coalesce(1).write.mode("overwrite").text(f"{out}/stats")
    spark.stop()


if __name__ == "__main__":
    main()
