import sys
import subprocess

"""Initializing pySpark"""
from pyspark import SparkContext
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")

print("\nChecking if packages are available or  not\n")
packages = ["pyspark", "spacy", "wget","sparknlp"]
for pkg in packages:
    subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])

subprocess.check_call([sys.executable, "-m", "spacy", "download", "en_core_web_sm"])
print("\nwget, spacy, pyspark, sparknlp are now available\n")


import sparknlp
spark = sparknlp.start()


import wget

def download_file(url, outpath):
    wget.download(url, out = outpath)

download_file("https://www.gutenberg.org/ebooks/37106.txt.utf-8","37106.txt.utf-8")



"""Get RDD data"""

textFile = sc.textFile("37106.txt.utf-8")

df = spark.read.text("37106.txt.utf-8").toDF("text")

df.show(5, truncate=False)

"""Convert RDD to list"""

text_lines = textFile.collect()

"""Importing the nltk for extracting named entities, tags etc."""


import spacy
nlp = spacy.load("en_core_web_sm")

def extract_entities_spacy(line):
    valid_labels = {"PERSON", "ORG", "GPE", "LOC"}  # for better results only adding these labels
    entities = []
    doc = nlp(line)
    for ent in doc.ents:
        if ent.label_ in valid_labels:
            entities.append(ent.text)
    return entities



ner_rdd = textFile.map(extract_entities_spacy)

ner_rdd.take(10)

"""Remove empty list"""

new_rdd = ner_rdd.flatMap(lambda x: x)

new_rdd.take(10)

"""Mapping"""

map_rdd = new_rdd.map(lambda x: (x, 1))

map_rdd.take(10)

map_rdd = map_rdd.persist()

"""Reduce by key"""

counts_rdd = map_rdd.reduceByKey(lambda x, y: x + y)

sorted_counts_rdd = counts_rdd.sortBy(lambda item: -item[1]).take(20)

print("\nHighest first 20 occuring Named-Entities\n")
top20_df = spark.createDataFrame(sorted_counts_rdd, ["Entity", "Count"])
top20_df.show(truncate=False)