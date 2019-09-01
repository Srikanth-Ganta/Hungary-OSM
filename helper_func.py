import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import size
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf 
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql.types import *
import folium
from pyspark.sql.functions import desc


#HELPER FUNCTIONS
#TAGS


def filter_key_value_match(string1,string2):
    def regex_filter(x):
        number_of_tags = len(x)
        flag = 0
        for i in range(0,number_of_tags):
            if ((string1 in x[i][0].decode("utf-8") ) and (string2 in x[i][1].decode("utf-8"))):
                flag +=1
        if flag > 0:
            return True
        else:
            return False

    filter_udf = udf(regex_filter, BooleanType())
    
    return filter_udf

def filter_string_match(string1):
    def regex_filter(x):
        number_of_tags = len(x)
        flag = 0
        for i in range(0,number_of_tags):
            if ((string1 in x[i][0].decode("utf-8") ) or (string1 in x[i][1].decode("utf-8"))):
                flag +=1
        if flag > 0:
            return True
        else:
            return False

    filter_udf = udf(regex_filter, BooleanType())
    
    return filter_udf

def make_column_tag_value_key(string1):
    string = string1
    print('yo')
    def tag_extractor_helper(x):
        number_of_tags = len(x)
        for i in range(0,number_of_tags):
            print(type(i))
            if (string in x[i][0].decode("utf-8")):
                return x[i][1].decode("utf-8")
        return x[0][1]

    tag_extractor_udf = udf(tag_extractor_helper, StringType())
    return tag_extractor_udf

#HELPER FUNCS

#MAP

def add_markers(m,list_of_lat_and_long_and_ID):
    for x in list_of_lat_and_long_and_ID:
        folium.Marker(location=[x[1],x[2]],popup=str(x[0])+','+str(x[1])+','+str(x[2]),size = 0.1).add_to(m)
def get_lat_long(df):
    list1 = []
    for x in df:
        list1.append([x.id,x.latitude , x.longitude]) 
    return list1        
