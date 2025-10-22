import sys
import subprocess

print("Checking if packages are available or  not\n")

packages = ["pyspark", "nltk", "wget"]
for pkg in packages:
    subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])

print("\nwget, nltk, pyspark are now available\n")

import nltk
import wget

nltk.download('stopwords')

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# !pip install wget

def download_file(url, outpath):
    wget.download(url, out = outpath)

print("\nDownloading file\n")
download_file("http://www.cs.cmu.edu/~ark/personas/data/MovieSummaries.tar.gz", "MovieSummaries.tar.gz")
print("\nUnzipping the file\n")
subprocess.run(["tar", "-xzvf", "MovieSummaries.tar.gz"])

plot_summaries_path = "MovieSummaries/plot_summaries.txt"

from pyspark.sql.functions import col, split, explode, lower, regexp_replace

"""Using Data Frame since its faster than RDD"""

plots_df = spark.read.option("sep", "\t").csv(plot_summaries_path).toDF("movie_id", "plot")

# plots_df.show()

"""Split into movie id and plot, tokenize and lowercase"""

from nltk.corpus import stopwords
import re
stopwords_set = set(stopwords.words('english'))

tokenized_df = plots_df.withColumn("tokens",split(lower(regexp_replace(col("plot"), '[^a-zA-Z0-9\\s]', '')),' '))
print("\nTokenized DF\n")
tokenized_df.select("movie_id", "tokens").show(5, truncate=True)

"""Clean the data, remove stopwords"""

from pyspark.sql.functions import udf, trim
from pyspark.sql.types import ArrayType, StringType

def remove_stopwords_fn(tokens):
  return [token for token in tokens if token not in stopwords_set]
remove_stopwords_udf = udf(remove_stopwords_fn, ArrayType(StringType()))
clean_df = tokenized_df.withColumn("cleaned_tokens",remove_stopwords_udf(col("tokens")))
cleaned_df = clean_df.withColumn("plot",trim(regexp_replace(col("plot"), "\\{\\{plot\\}\\}", "")))

print("\nRemoved stop words from DF\n")
cleaned_df.select("movie_id", "cleaned_tokens").show(5, truncate=True)

"""seaparate each word, Mapping"""

from pyspark.sql.functions import col, explode
print("\nCleaned tokens DF\n")
words_df = cleaned_df.select("movie_id",explode(col("cleaned_tokens")).alias("word"))

# words_df.show()

"""Reduce by key step using DF"""

word_counts_df = words_df.groupBy("movie_id", "word").count()
# word_counts_df.show()

"""Total words in each movie id"""

from pyspark.sql.functions import size
total_words_df = cleaned_df.withColumn("total_words_in_doc",size(col("cleaned_tokens")))
total_words_df.cache()
# total_words_df.show()

"""From ((doc1, plot_word), freq) to (doc1,(plot_word, freq))"""

tf_joined_df = word_counts_df.join(total_words_df, "movie_id")
tf_joined_df = tf_joined_df.drop("plot","tokens","cleaned_tokens")
tf_joined_df.cache()
# tf_joined_df.show()

tf_joined_df = tf_joined_df.withColumnRenamed("count","term_freq")
# tf_joined_df.show()

"""IDF Calculations"""

total_number_docs = cleaned_df.count()
print(total_number_docs)

from pyspark.sql.functions import array_distinct

unique_words_df = cleaned_df.withColumn("unique_words", array_distinct(col("cleaned_tokens")))
final_df = unique_words_df.select("movie_id", "unique_words")
final_df.cache()
# final_df.show()

"""Mapping using the dataFrames"""

word_appearances_df = unique_words_df.select("movie_id",explode(col("unique_words")).alias("word"))
word_appearances_df.cache()
# word_appearances_df.show()

"""Reduce by key"""

doc_freq_df = word_appearances_df.groupBy("word").count().withColumnRenamed("count", "doc_freq")
doc_freq_df.cache()
# doc_freq_df.show()

from pyspark.sql.functions import log
idf_df = doc_freq_df.withColumn("idf",log(total_number_docs / col("doc_freq")))
idf_df.cache()
# idf_df.show()

from pyspark.sql.functions import col

tf_idf_df = tf_joined_df.join(idf_df, "word").withColumn("tfidf", col("term_freq") * col("idf"))
tf_idf_df.cache()
print("TF-IDF calculations\n")
tf_idf_df.select("movie_id", "word", "tfidf").show()

"""Movie-Names DF"""

metadata_path = "MovieSummaries/movie.metadata.tsv"
movie_names_df = spark.read.option("sep", "\t").csv(metadata_path).select(col("_c0").alias("movie_id"), col("_c2").alias("movie_name"))

movie_names_df.cache()
# movie_names_df.show(5)

def load_queries(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        queries = [line.strip() for line in f if line.strip()]
    single = [q for q in queries if len(q.split()) == 1]
    multi = [q for q in queries if len(q.split()) > 1]

    return single, multi

download_file("https://storage.googleapis.com/bigdata-bucket-assign1/search_queries.txt","search_queries.txt")
single_term_queries, multi_term_queries = load_queries("search_queries.txt")

def print_results(results, val):
    print("-----------------------------------------------------------------")
    print(f"Rank | Movie Title                               | {val}")
    print("-----------------------------------------------------------------")
    for i, (name, score) in enumerate(results, 1):
        print(f"{i:<4} | {name:<40}| {score:.4f}")
    print("\n")

from pyspark.sql.functions import col

def search_single_term_df(term, tf_idf_df, movie_names_df):
    print(f"----------for query: {term}----------")
    top_10_df = tf_idf_df.filter(col("word") == term).join(movie_names_df, "movie_id").orderBy(col("tfidf").desc()).limit(10)
    results = top_10_df.select("movie_name", "tfidf").collect()
    if results:
        results_for_printing = [(row["movie_name"], row["tfidf"]) for row in results]
        print_results(results_for_printing, "TF-IDF score")
    else:
        print("No results found.")

for term in single_term_queries:
    search_single_term_df(term, tf_idf_df, movie_names_df)

import pyspark.sql.functions as F
from pyspark.sql.functions import lit

def prepare_query_vector(query, idf_df):
    query_tokens = [token for token in re.findall('[a-zA-Z0-9]+', query.lower()) if token not in stopwords_set]
    query_tf_list = [(word, query_tokens.count(word)) for word in set(query_tokens)]
    query_tf_df = spark.createDataFrame(query_tf_list, ["word", "qt_freq"])

    # Calculate query TF-IDF
    query_vector_df = query_tf_df.join(idf_df, "word").withColumn("qtfidf", col("qt_freq") * col("idf")).select("word", "qtfidf")

    return query_vector_df


def search_multiple_terms_df(query, final_tfidf_df, movie_names_df, idf_df):
    print(f"----------for query: {query}----------")
    query_vector_df = prepare_query_vector(query, idf_df)

    # Calculating the Dot Products
    dot_product_df = final_tfidf_df.join(query_vector_df, "word") \
        .withColumn("product", col("tfidf") * col("qtfidf")) \
        .groupBy("movie_id") \
        .agg(F.sum("product").alias("dot_product"))

    # ||d||
    doc_mag_df = final_tfidf_df.withColumn("tfidf_sq", col("tfidf") ** 2).groupBy("movie_id").agg(F.sqrt(F.sum("tfidf_sq")).alias("doc_mg"))

    # ||q||
    q_mag = query_vector_df.withColumn("tfidf_sq", col("qtfidf") ** 2).agg(F.sqrt(F.sum("tfidf_sq"))).first()[0]

    if q_mag == 0:
        print("Query terms not found.")
        return

    # Cosine Simmilarity
    results_df = dot_product_df.join(doc_mag_df, "movie_id").withColumn("sim", col("dot_product") / (col("doc_mg") * lit(q_mag)))
    results_df = results_df.join(movie_names_df, "movie_id")
    results_df = results_df.orderBy(col("sim").desc()).limit(10)
    results_df = results_df.select("movie_name", "sim")
        

    results = results_df.collect()
    if results:
        results_for_printing = [(row["movie_name"], row["sim"]) for row in results]
        print_results(results_for_printing, "Cosine Similarity")
    else:
        print("No results found.")

for term in multi_term_queries:
    search_multiple_terms_df(term, tf_idf_df, movie_names_df, idf_df)