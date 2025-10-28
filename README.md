# BigData
# 🧠 Named Entity Recognition (NER) with PySpark and spaCy

This project performs **Named Entity Recognition (NER)** on a large text dataset using **PySpark** for distributed processing and **spaCy** for entity extraction.  
It demonstrates how to integrate **Natural Language Processing (NLP)** within a **Big Data** pipeline using **Spark RDDs** and **DataFrames**.

---

## 🚀 Overview

The goal of this project is to identify and count frequently occurring **named entities** such as **people, organizations, and locations** from a large text corpus (downloaded from Project Gutenberg).

It combines the scalability of **PySpark** with the linguistic power of **spaCy**, enabling parallel processing of text data.

---

## 🧰 Tech Stack

- **Python 3**
- **PySpark**
- **spaCy**
- **Spark NLP**
- **wget**
- **RDDs and DataFrames**

---

## 🗂️ Dataset

The dataset used is a public domain text from **[Project Gutenberg](https://www.gutenberg.org/)**.

File used:
https://www.gutenberg.org/ebooks/37106.txt.utf-8


This file is automatically downloaded using the `wget` module in the code.

---

## ⚙️ How It Works

1. **Setup and Environment Initialization**  
   - Initializes SparkContext and SparkSession (via `sparknlp.start()`).
   - Installs required Python packages (`pyspark`, `spacy`, `wget`, `sparknlp`).
   - Downloads the English spaCy model `en_core_web_sm`.

2. **Data Loading**  
   - The Gutenberg text is read as an RDD and converted into a Spark DataFrame for exploration.

3. **Named Entity Extraction**  
   - Each line of text is processed through a `spaCy` pipeline.
   - Extracted entities are filtered to only include **PERSON**, **ORG**, **GPE**, and **LOC** labels.

4. **Aggregation and Counting**  
   - Mapped entities are reduced by key using PySpark’s `reduceByKey` to count occurrences.
   - The top 20 most frequent entities are displayed as a Spark DataFrame.

---

## 📊 Sample Output
Highest first 10 occuring Named-Entities
[('Jo', 1342),
 ('Meg', 674),
 ('Amy', 654),
 ('Laurie', 599),
 ('Beth', 479),
 ('John', 148),
 ('Hannah', 120),
 ('Laurence', 93),
 ('Brooke', 91),
 ('Bhaer', 90)]

 
---

## 🧩 Key Learnings

- Integration of **spaCy** with **PySpark** workflows.  
- Use of **map**, **flatMap**, and **reduceByKey** transformations.  
- Handling NLP workloads in a **distributed computing** setup.  
- Efficient extraction and aggregation of unstructured text data.

---

## 🏁 How to Run

1. Clone the repository:
   ```bash
   git clone https://github.com/<your-username>/big-data.git
   cd big-data/ner-pyspark

 
