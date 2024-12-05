import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.colors as mcolors

##---- 1.前10本最受关注的书籍(text_reviews_count)
csv_filepath=r'result\top_10_text.csv\part-00000-21059674-1ceb-4e1f-8939-c766c2c7c39f-c000.csv'
# 手动指定列名
column_names = ['bookID','title','first_author','average_rating','language_code','text_reviews_count','publication_date']
# 使用pandas读取CSV文件，并在读取时指定列名
df = pd.read_csv(csv_filepath, header=None, names=column_names)
# 筛选前10行数据
data_to_plot = df.head(10)
# 获取数据列
titles=data_to_plot['title']
texts=data_to_plot['text_reviews_count']
# 创建横向柱状图
plt.figure(figsize=(10, 5))
plt.barh(titles, texts, color='skyblue')
# 在每个条形上显示数值
for index, value in enumerate(texts):
    plt.text(value, index, f'{value}', va='center', ha='left')
plt.ylabel('Book title', fontsize=14, fontweight='bold')
plt.xlabel('Text reviews count', fontsize=14, fontweight='bold')
plt.title('Top 10 Most Popular Books', fontsize=14, fontweight='bold')
# 自动调整子图参数, 使之填充整个图像区域
plt.tight_layout()
# 调整左侧边距，以防标签重叠
# plt.subplots_adjust(left=0.3)  
# 保存和显示图表
plt.savefig(r'D:\ecnu\大规模\Figure\Top_10_Most_Popular_Books.png')
plt.show()



##---- 2.前10个最长篇幅的书籍（num_pages）
csv_filepath=r'result\top_10_numpages.csv\part-00000-3e62ac19-0fe2-412e-b2cf-fcc64c981a92-c000.csv'
# 手动指定列名
column_names = ['bookID','title','first_author','average_rating','language_code','num_pages','publication_date']
# 使用pandas读取CSV文件，并在读取时指定列名
df = pd.read_csv(csv_filepath, header=None, names=column_names)
# 筛选前10行数据
data_to_plot = df.head(10)
# 获取数据列
titles=data_to_plot['title']
num_pages=data_to_plot['num_pages']
# 创建柱状图
plt.figure(figsize=(15, 5))
plt.barh(titles, num_pages, color='skyblue')
# 在每个条形上显示数值
for index, value in enumerate(num_pages):
    plt.text(value, index, f'{value}', va='center', ha='left')
# 添加标题和标签
plt.title('The top 10 longest length books', fontsize=14, fontweight='bold')
plt.ylabel('Book Titles', fontsize=14, fontweight='bold')
plt.xlabel('Number of Pages', fontsize=14, fontweight='bold')
# 自动调整子图参数, 使之填充整个图像区域
plt.tight_layout()
# 保存和显示图表
plt.savefig(r'Figure\The_top_10_longest_length_books.png')
plt.show()


##---- 3.不同出版社出版的书籍数量，统计前50个
csv_filepath=r'result\pubulisher_books_num.csv\part-00000-50f283e3-7bac-4d4d-a41f-56ce13dfbdd8-c000.csv'
# 手动指定列名
column_names = ['publisher','books_num']
# 使用pandas读取CSV文件，并在读取时指定列名
df = pd.read_csv(csv_filepath, header=None, names=column_names)
# 筛选前10行数据
# data_to_plot = df.head(10)
data_to_plot=df.head(50)
# 获取数据列
publisher=data_to_plot['publisher']
books_num=data_to_plot['books_num']
# 创建柱状图
plt.figure(figsize=(10, 5))
bars=plt.bar(publisher, books_num, color='skyblue')
# 在每个柱子上显示数值
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2.0, yval, int(yval), ha='center',rotation=45)
# 添加标题和标签
plt.title('The number of books published by different publishers', fontsize=14, fontweight='bold')
plt.ylabel('Number of Books', fontsize=14, fontweight='bold')
plt.xlabel('Publisher', fontsize=14, fontweight='bold')
plt.xticks(rotation=90)
# 自动调整子图参数, 使之填充整个图像区域
plt.tight_layout()
# 保存和显示图表
plt.savefig(r'Figure\The_number_of_books_published_by_different_publishers.png')
plt.show()



##---- 4.不同语言的书籍数量
csv_filepath=r'result\language_books_num.csv\part-00000-f237751d-a91f-4d48-9af0-9127fed67a93-c000.csv'
# 手动指定列名
column_names = ['language_code','books_num']
# 使用pandas读取CSV文件，并在读取时指定列名
df = pd.read_csv(csv_filepath, header=None, names=column_names)
# 筛选前50行数据
data_to_plot=df.head(50)
# 获取数据列
language_code=data_to_plot['language_code']
books_num=data_to_plot['books_num']
# 创建柱状图
plt.figure(figsize=(10, 5))
bars=plt.bar(language_code, books_num, color='skyblue')
# 在每个柱子上显示数值
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2.0, yval, int(yval), ha='center')
# 添加标题和标签
plt.title('Number of books in different languages', fontsize=14, fontweight='bold')
plt.ylabel('Number of Books', fontsize=14, fontweight='bold')
plt.xlabel('Language_code', fontsize=14, fontweight='bold')
# 自动调整子图参数, 使之填充整个图像区域
plt.tight_layout()
# 保存和显示图表
plt.savefig(r'Figure\Number_of_books_in_different_languages.png')
plt.show()



##---- 5.前10本最不受关注的高分书籍(评分在4.5分以上，评分人数超过1万，评论数少于200) —— 冷门高分书籍
csv_filepath=r'result\top_10_high_score.csv\part-00000-bb4987c3-fadd-4844-b90b-8878d699dc5d-c000.csv'
# 手动指定列名
column_names = ['bookID','title','first_author','average_rating','language_code','ratings_count','text_reviews_count','publication_date']
# 使用pandas读取CSV文件，并在读取时指定列名
df = pd.read_csv(csv_filepath, header=None, names=column_names)
# 筛选前10行数据
data_to_plot = df.head(10)
# 获取数据列
titles=data_to_plot['title']
average_rating=data_to_plot['average_rating']
# 创建横向柱状图
plt.figure(figsize=(10, 5))
plt.barh(titles, average_rating, color='skyblue')
# 在每个条形上显示数值
for index, value in enumerate(average_rating):
    plt.text(value, index, f'{value}', va='center', ha='left')
plt.ylabel('Book title', fontsize=14, fontweight='bold')
plt.xlabel('Average rating', fontsize=14, fontweight='bold')
plt.title('Top 10 Unpopular High Scoring Books', fontsize=14, fontweight='bold')
# 自动调整子图参数, 使之填充整个图像区域
plt.tight_layout()
# 调整左侧边距，以防标签重叠
# plt.subplots_adjust(left=0.3)  
# 保存和显示图表
plt.savefig(r'Figure\Top_10_Unpopular_High_Scoring_Books.png')
plt.show()



##---- 6.出版书籍的数量与时间（年份）的关系
csv_filepath=r'result\relation_booknum_year\part-00000-9b124da7-642e-4127-993e-7aec72b48b1b-c000.csv'
# 手动指定列名
column_names = ['year','books_num']
# 使用pandas读取CSV文件，并在读取时指定列名
df = pd.read_csv(csv_filepath, header=None, names=column_names)
# 选取所有数据
data_to_plot = df
# 获取数据列
year=data_to_plot['year']
books_num=data_to_plot['books_num']
# 绘制折线图
plt.figure(figsize=(10, 6))
plt.plot(year, books_num, marker='o', linestyle='-', color='skyblue')
# 添加标题和坐标轴标签
plt.title('Number of Books Published Over Years', fontsize=14, fontweight='bold')
plt.xlabel('Year', fontsize=14, fontweight='bold')
plt.ylabel('Number of Books', fontsize=14, fontweight='bold')
# 优化x轴刻度标签显示，避免重叠
# plt.xticks(rotation=45)
# 显示网格
plt.grid(True, which='both', linestyle='--', linewidth=0.5)
# 自动调整子图参数, 使之填充整个图像区域
plt.tight_layout()  
# 保存和显示图表
plt.savefig(r'Figure\Number_of_Books_Published_Over_Years.png')
plt.show()



##---- 7.不同作者的书的平均评分(sum(average_rating*ratings_count)/sum(ratings_count))
csv_filepath=r'result\avg_rate_author.csv\part-00000-e81935dd-e3b1-4e0c-a2ff-879824a357fa-c000.csv'
# 手动指定列名
column_names = ['first_author','avg_rate','books_num']
# 使用pandas读取CSV文件，并在读取时指定列名
df = pd.read_csv(csv_filepath, header=None, names=column_names)
# 选取前50个数据
data_to_plot = df.head(50)
# 获取数据列
first_author=data_to_plot['first_author']
avg_rate=data_to_plot['avg_rate']
# 绘制柱状图
plt.figure(figsize=(10, 6))
plt.bar(first_author, avg_rate, color='skyblue')
# 添加标题和坐标轴标签
plt.title('Average ratings of books by different authors', fontsize=14, fontweight='bold')
plt.xlabel('First author', fontsize=14, fontweight='bold')
plt.ylabel('Average ratings of books', fontsize=14, fontweight='bold')
# 优化x轴刻度标签显示，避免重叠
plt.xticks(rotation=90)
# 显示网格
# plt.grid(True, which='both', linestyle='--', linewidth=0.5)
# 自动调整子图参数, 使之填充整个图像区域
plt.tight_layout()  
# 保存和显示图表
plt.savefig(r'Figure\Average_ratings_of_books_by_different_authors.png')
plt.show()




##---- 8.前1000个最受关注的书籍数量与出版社的关系
csv_filepath=r'result\relation_ratebooknum_publisher.csv\part-00000-aaa1e8d4-b3fa-4378-8c42-c566f14b8c7c-c000.csv'
# 手动指定列名
column_names = ['publisher','ratebooks_num']
# 使用pandas读取CSV文件，并在读取时指定列名
df = pd.read_csv(csv_filepath, header=None, names=column_names)
# 选取前50个数据
data_to_plot = df.head(10)
# 获取数据列
publisher=data_to_plot['publisher']
ratebooks_num=data_to_plot['ratebooks_num']
# 创建饼图
plt.figure(figsize=(8, 8))
plt.pie(ratebooks_num, labels=publisher, autopct='%1.1f%%', startangle=140)
# 添加图表标题
plt.title('The relationship between books and publisher(Top 10)')
# 自动调整子图参数, 使之填充整个图像区域
plt.tight_layout()  
# 保存和显示图表
plt.savefig(r'Figure\The_relationship_between_books_and_publisher.png')
plt.show()




##---- 9.前1000个最受关注的书籍数量与语言的关系
csv_filepath=r'result\relation_ratebooknum_language.csv\part-00000-c42c1adb-f838-4e1f-9225-98d82d27275c-c000.csv'
# 手动指定列名
column_names = ['language_code','ratebooks_num']
# 使用pandas读取CSV文件，并在读取时指定列名
df = pd.read_csv(csv_filepath, header=None, names=column_names)
# 选取前50个数据
data_to_plot = df.head(10)
# 获取数据列
language_code=data_to_plot['language_code']
ratebooks_num=data_to_plot['ratebooks_num']
# 创建饼图
plt.figure(figsize=(8, 8))
wedges = plt.pie(ratebooks_num, labels=None, autopct='%1.1f%%', startangle=140)[0]  # 只接收楔形对象列表
# 为每个部分创建一个图例项
handles = [plt.Rectangle((0,0),1,1, color=mcolors.to_rgba(wedge.get_facecolor())) for wedge in wedges]
labels = language_code
# 添加图例
plt.legend(handles, labels, title="Publishers", loc='upper right', bbox_to_anchor=(0.9, 0.9))
# 添加图表标题
plt.title('The relationship between books and language_code')
# 自动调整子图参数, 使之填充整个图像区域
plt.tight_layout()  
# 保存和显示图表
plt.savefig(r'Figure\The_relationship_between_books_and_language_code.png')
plt.show()




#---- 10.不同出版社的书的平均受关注程度(sum(text_reviews_count)/COUNT(*))
csv_filepath=r'result\avg_attention_author.csv\part-00000-f58dbdc2-aac8-4554-93c2-722399cc0a1b-c000.csv'
# 手动指定列名
column_names = ['first_author','books_num','avg_attention']
# 使用pandas读取CSV文件，并在读取时指定列名
df = pd.read_csv(csv_filepath, header=None, names=column_names)
# 选取前50个数据
data_to_plot = df.head(50)
# 获取数据列
first_author=data_to_plot['first_author']
avg_attention=data_to_plot['avg_attention']
# 绘制柱状图
plt.figure(figsize=(10, 6))
plt.bar(first_author, avg_attention, color='skyblue')
# 添加标题和坐标轴标签
plt.title('Average attention of books by different authors', fontsize=14, fontweight='bold')
plt.xlabel('First author', fontsize=14, fontweight='bold')
plt.ylabel('Average attention', fontsize=14, fontweight='bold')
# 优化x轴刻度标签显示，避免重叠
plt.xticks(rotation=90)
# 自动调整子图参数, 使之填充整个图像区域
plt.tight_layout()  
# 保存和显示图表
plt.savefig(r'Figure\Average_attention_of_books_by_different_authors.png')
plt.show()


