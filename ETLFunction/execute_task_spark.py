# coding: utf-8

from pyspark.sql import SQLContext, Row, DataFrame, SparkSession
from pyspark import SparkContext
import argparse
import boto3
import botocore
import json
import psycopg2

def main():
	"""
	It receives parameters for the execution of the process defined by DAG.
	arg:
		--bucket(str): Storage (Bucket) of data source and destination
		--schema(str): Storage (bucket) directory
		--sourceTable(str): File source table file
		--destinationTable(str): File destination table file
    """
	parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", action="store")
    parser.add_argument("--schema", action="store")
    parser.add_argument("--sourceTable", action="store")
    parser.add_argument("--destinationTable", action="store")

    args = parser.parse_args()

    ls3Bucket = args.bucket
    lschema = args.schema
    sourceTable = args.sourceTable
    destinationTable = args.destinationTable

    sourceTable = sourceTable.split(',')

    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)

	"""Create views for use in DataFrame"""
    sqlContext.read.parquet("s3://{}/trusted/{}/{}/".format(ls3Bucket,lschema,sourceTable[0])).createOrReplaceTempView('Table1') 
    sqlContext.read.parquet("s3://{}/trusted/{}/{}/".format(ls3Bucket,lschema,sourceTable[1])).createOrReplaceTempView('Table2') 
    
    df = sqlContext.sql("SELECT * FROM Table1 UNION SELECT * FROM Table2")

    columns = ['SK_TABLE','DE_TABLE']
    
	""" Create parquet file whit result of DataFrame"""
    df.coalesce(1)\
        .write\
        .mode("overwrite")\
        .parquet("s3://{}/{}/{}/".format(ls3Bucket,"refined/TABLE/",destinationTable))
    
if __name__ == "__main__":
    main()




