import os
from flask import Flask
from flask import request
from pyspark.sql import SparkSession


app = Flask(__name__)

def tokenize(sentence):
    n = 100
    spark = SparkSession.builder.appName("Tokenize").getOrCreate()

    # udf
    def f(s):
        from nltk.tokenize import TweetTokenizer
        tokenizer = TweetTokenizer()
        return tokenizer.tokenize(s)

    # create n sentences
    sentences = [sentence] * n
    # spark.sparkContext.parallelize(sentences).map(f).toDF().show(10)
    return spark.sparkContext.parallelize(sentences).map(f).take(1)[0]


@app.route("/")
def root():
    sentence = request.args.get('text', 'The quick brown fox jumps over the lazy dog')
    tokens = tokenize(sentence)
    return ', '.join(tokens)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)