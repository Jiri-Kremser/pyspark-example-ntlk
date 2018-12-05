from pyspark.sql import SparkSession
import sys


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
    return spark.sparkContext.parallelize(sentences).map(f).take(1)[0]

if __name__ == "__main__":
    sentence = """
    Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Pellentesque arcu. Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Morbi leo mi, nonummy eget tristique non, rhoncus non leo. Fusce aliquam vestibulum ipsum. Cras elementum. Sed ac dolor sit amet purus malesuada congue. Nam libero tempore, cum soluta nobis est eligendi optio cumque nihil impedit quo minus id quod maxime placeat facere possimus, omnis voluptas assumenda est, omnis dolor repellendus. Maecenas aliquet accumsan leo. Phasellus rhoncus. Praesent dapibus. In enim a arcu imperdiet malesuada. Nam libero tempore, cum soluta nobis est eligendi optio cumque nihil impedit quo minus id quod maxime placeat facere possimus, omnis voluptas assumenda est, omnis dolor repellendus. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos hymenaeos. Etiam dictum tincidunt diam. Etiam ligula pede, sagittis quis, interdum ultricies, scelerisque eu. Nulla est. Cras pede libero, dapibus nec, pretium sit amet, tempor quis. Aliquam id dolor.
Etiam ligula pede, sagittis quis, interdum ultricies, scelerisque eu. Nam quis nulla. Proin mattis lacinia justo. Phasellus et lorem id felis nonummy placerat. Vivamus luctus egestas leo. In sem justo, commodo ut, suscipit at, pharetra vitae, orci. Pellentesque sapien. Suspendisse nisl. Nulla pulvinar eleifend sem. Duis ante orci, molestie vitae vehicula venenatis, tincidunt ac pede. Praesent vitae arcu tempor neque lacinia pretium. Duis sapien nunc, commodo et, interdum suscipit, sollicitudin et, dolor. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos hymenaeos. Nam quis nulla. Maecenas lorem. Integer lacinia. Nulla quis diam. Mauris elementum mauris vitae tortor. Pellentesque sapien. Fusce nibh.
Donec vitae arcu. Nam sed tellus id magna elementum tincidunt. Cras elementum. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos hymenaeos. Proin mattis lacinia justo. Nulla quis diam. Nam quis nulla. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos hymenaeos. Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo. Duis risus. Integer vulputate sem a nibh rutrum consequat. Maecenas libero. Fusce nibh. Aenean fermentum risus id tortor. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Nullam feugiat, turpis at pulvinar vulputate, erat libero tristique tellus, nec bibendum odio risus sit amet ante. Duis ante orci, molestie vitae vehicula venenatis, tincidunt ac pede. Fusce tellus.
Etiam ligula pede, sagittis quis, interdum ultricies, scelerisque eu. Quisque tincidunt scelerisque libero. Vivamus luctus egestas leo. Mauris tincidunt sem sed arcu. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Duis pulvinar. Etiam posuere lacus quis dolor. Nullam at arcu a est sollicitudin euismod. Integer imperdiet lectus quis justo. Duis viverra diam non justo. Aliquam in lorem sit amet leo accumsan lacinia. Praesent vitae arcu tempor neque lacinia pretium. Ut enim ad minima veniam, quis nostrum exercitationem ullam corporis suscipit laboriosam, nisi ut aliquid ex ea commodi consequatur? Donec iaculis gravida nulla. Aliquam id dolor.
Nullam justo enim, consectetuer nec, ullamcorper ac, vestibulum in, elit. Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo. Maecenas lorem. In convallis. Praesent vitae arcu tempor neque lacinia pretium. Pellentesque pretium lectus id turpis. Duis ante orci, molestie vitae vehicula venenatis, tincidunt ac pede. Integer tempor. In laoreet, magna id viverra tincidunt, sem odio bibendum justo, vel imperdiet sapien wisi sed libero. Nullam eget nisl. Duis ante orci, molestie vitae vehicula venenatis, tincidunt ac pede. Fusce wisi. Integer vulputate sem a nibh rutrum consequat. Suspendisse nisl. Integer pellentesque quam vel velit. Curabitur ligula sapien, pulvinar a vestibulum quis, facilisis vel sapien. Nullam eget nisl. Mauris dolor felis, sagittis at, luctus sed, aliquam non, tellus. Mauris elementum mauris vitae tortor. Duis risus.
    """
    tokens = tokenize(sentence)
    
    print("\n".join(tokens))
    print("\nPython version:\n" + sys.version + "\n\n")
