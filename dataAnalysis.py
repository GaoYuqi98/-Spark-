from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format,split,rank
from pyspark.sql.window import Window
import pandas as pd

spark = SparkSession.builder.config(conf = SparkConf()).getOrCreate()

# 使用spark读取csv文件，创建dataframe
books_df = spark.read.csv("hdfs://linux01:8020/user/root/input/books_cleaned.csv", header=True, inferSchema=True)
# 确保所有数据在同一个分区以便于后续操作
books_df = books_df.repartition(1)
# 显示数据的前10行
books_df.show(10)

# 创建视图
books_df.createOrReplaceTempView("books")


##---- 1.前10本最受关注的书籍(text_reviews_count)
top_10_text=spark.sql("SELECT bookID,title,SUBSTRING_INDEX(authors, '/', 1) AS first_author,average_rating,language_code,text_reviews_count,publication_date \
                      FROM books \
                      ORDER BY text_reviews_count DESC")
top_10_text.show(n=10,truncate=False)
print("## Top 10 text_reviews_count\n")

top_10_text.write.csv("result/top_10_text.csv",mode='overwrite') # 文件写入hdfs的result中


##---- 2.前10个最长篇幅的书籍（num_pages）
top_10_numpages=spark.sql("SELECT bookID,title,SUBSTRING_INDEX(authors, '/', 1) AS first_author,average_rating,language_code,num_pages,publication_date \
                          FROM books \
                          ORDER BY num_pages DESC")
top_10_numpages.show(n=10,truncate=False)
print("## Top 10 num_pages\n")
top_10_numpages.write.csv("result/top_10_numpages.csv",mode='overwrite')


##---- 3.不同出版社出版的书籍数量，统计前50个
pubulisher_books_num=spark.sql("SELECT publisher,COUNT(*) as books_num \
                               FROM books \
                               GROUP BY publisher \
                               ORDER BY books_num DESC")
pubulisher_books_num.show(truncate=False)
print("## Pubulisher books num\n")
pubulisher_books_num.write.csv("result/pubulisher_books_num.csv",mode='overwrite')


##---- 4.不同语言的书籍数量
language_books_num=spark.sql("SELECT language_code,COUNT(*) as books_num \
                             FROM books \
                             GROUP BY language_code \
                             ORDER BY books_num DESC")
language_books_num.show(truncate=False)
print("## Language books num\n")
language_books_num.write.csv("result/language_books_num.csv",mode='overwrite')


##---- 5.前10本最不受关注的高分书籍(评分在4.5分以上，评分人数超过1万，评论数少于200) —— 冷门高分书籍
top_10_high_score=spark.sql("SELECT bookID,title,SUBSTRING_INDEX(authors, '/', 1) AS first_author,average_rating,language_code,ratings_count,text_reviews_count,publication_date \
                            FROM books \
                            where average_rating>4.5 and ratings_count>=10000 and text_reviews_count<=300 \
                            ORDER BY text_reviews_count ASC")
top_10_high_score.show(n=10,truncate=False)
print("## Top 10 high score\n")
top_10_high_score.write.csv("result/top_10_high_score.csv",mode='overwrite')


##---- 6.出版书籍的数量与时间（年份）的关系
# 从publication_date列中提取年份并创建一个新列year
books_df_with_year = books_df.withColumn("year",  date_format(books_df["publication_date"], "yyyy"))
# 使用带有年份列的DataFrame创建视图
books_df_with_year.createOrReplaceTempView("books_with_year")
books_df_with_year.show(n=10)
# 构造sql
relation_booknum_year=spark.sql("SELECT year,COUNT(*) as books_num FROM books_with_year GROUP BY year ORDER BY year ASC")
relation_booknum_year.show(truncate=False)
print("## Relation_booknum_year\n")
relation_booknum_year.write.csv("result/relation_booknum_year",mode='overwrite')



##---- 7.不同作者的书的平均评分(sum(average_rating*ratings_count)/sum(ratings_count))
# 从authors列中提取年份并创建一个新列first_author
books_df_with_first_author = books_df.withColumn("first_author",split(books_df["authors"], "/").getItem(0))
# 使用带有第一作者列的DataFrame创建视图
books_df_with_first_author.createOrReplaceTempView("books_df_with_first_author")
books_df_with_first_author.show(n=10)
# 构造sql
avg_rate_author=spark.sql("SELECT first_author,SUM(average_rating*ratings_count)/SUM(ratings_count) as avg_rate,COUNT(*) as books_num \
                          FROM books_df_with_first_author \
                          GROUP BY first_author \
                          ORDER BY books_num DESC,avg_rate DESC")
avg_rate_author.show(truncate=False)
print("## avg_attention_author\n")
avg_rate_author.write.csv("result/avg_rate_author.csv",mode='overwrite')



##---- 8.前1000个最受关注的书籍数量与出版社的关系
# 定义一个窗口用于排名
windowSpec = Window.orderBy(books_df_with_first_author["text_reviews_count"].desc())
# 计算每行的排名
books_df_ranked = books_df_with_first_author.withColumn("rank", rank().over(windowSpec))
# 提取排名在前1000的记录
top_1000_books = books_df_ranked.filter(books_df_ranked["rank"] <= 1000).drop("rank")
# 使用筛选后的DataFrame创建视图
top_1000_books.createOrReplaceTempView("top_1000_books_by_reviews")
top_1000_books.show(n=10)
# 构造sql
relation_ratebooknum_publisher=spark.sql("SELECT publisher,COUNT(*) as ratebooks_num FROM top_1000_books_by_reviews GROUP BY publisher ORDER BY ratebooks_num DESC")
relation_ratebooknum_publisher.show(truncate=False)
print("## relation_ratebooknum_publisher\n")
relation_ratebooknum_publisher.write.csv("result/relation_ratebooknum_publisher.csv",mode='overwrite')



##---- 9.前1000个最受关注的书籍数量与语言的关系
# 沿用前面的视图top_1000_books_by_reviews
relation_ratebooknum_language=spark.sql("SELECT language_code,COUNT(*) as ratebooks_num \
                                        FROM top_1000_books_by_reviews \
                                        GROUP BY language_code \
                                        ORDER BY ratebooks_num DESC")
relation_ratebooknum_language.show(truncate=False)
print("## relation_ratebooknum_language\n")
relation_ratebooknum_language.write.csv("result/relation_ratebooknum_language.csv",mode='overwrite')



##---- 10.不同作者的书的平均受关注程度(sum(text_reviews_count)/COUNT(*))
# 沿用前面的视图books_df_with_first_author
avg_attention_author=spark.sql("SELECT first_author,COUNT(*) as books_num,SUM(text_reviews_count)/COUNT(*) as avg_attention \
                               FROM books_df_with_first_author \
                               GROUP BY first_author \
                               ORDER BY avg_attention DESC,books_num DESC")
avg_attention_author.show(truncate=False)
print("## avg_attention_author\n")
avg_attention_author.write.csv("result/avg_attention_author.csv",mode='overwrite')



