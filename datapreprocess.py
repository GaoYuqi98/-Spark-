import pandas as pd
import numpy as np

dataFrame = pd.read_csv('books.csv',error_bad_lines=False)
# 显示前10行 
print(dataFrame.head(10))

# 去除列名前后的空格
dataFrame.columns=dataFrame.columns.str.strip()

# 查看数据集信息
print("\nraw dataFrame:")
print(dataFrame.info())

# 删除空值
dataFrame_remove_null=dataFrame.dropna()
print("\ndataFrame_remove_null:")
print(dataFrame_remove_null.info())
dataFrame=dataFrame_remove_null

# 删除重复值
dataFrame_remove_dup=dataFrame.drop_duplicates(keep='first')
print("\ndataFrame_remove_dup:")
print(dataFrame_remove_dup.info())
dataFrame=dataFrame_remove_dup


# 格式化时间列，由9/16/2006变成2006-9-16
# 处理不规范的数据
def convert_date(date_str):
    try:
        # 尝试转换日期格式
        converted_date = pd.to_datetime(date_str, format='%m/%d/%Y')
        return converted_date.strftime('%Y-%m-%d')  # 成功转换后重新格式化
    except ValueError:
        # 转换失败则返回NaN
        return np.nan

dataFrame['publication_date']=dataFrame['publication_date'].apply(convert_date)

# 删除空值
dataFrame_remove_null=dataFrame.dropna()
print("\ndataFrame_remove_null:")
print(dataFrame_remove_null.info())
dataFrame=dataFrame_remove_null


# 查看language_code的数据有没有异常值
print(dataFrame['language_code'].unique())


# 将处理后的数据写入新的csv文件中
dataFrame.to_csv('books_cleaned.csv',encoding='utf-8',index=False)


