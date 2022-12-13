# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 10:48:30 2022

@author: gquisbertj
"""
#import findspark
#findspark.init()
#from pyspark import SparkContext, SparkConf
#from pyspark.sql import SparkSession, SQLContext
#import pandas as pd

def consulta(name, qry, spark):
    while True:
        try:
            #datos de acceso a BD
            ipServer='10.1.4.141'
            port='1521'
            sid='argos.bdp.com.bo'
            #CREDENCIALES AMBIENTE DE TEST2                
            user='bdpsam'
            password='bdpsam.test2'
            #conexion por jdbc
            url='jdbc:oracle:thin:@//'+ipServer+':'+port+'/'+sid
            driver='oracle.jdbc.driver.OracleDriver'
            
            dfc= spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("driver",driver).option("dbtable", "("+qry+")").load()
            return dfc
        except:
            return "error"
            

