# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 09:52:53 2022

@author: gquisbertj
"""
from pyspark import SparkConf
from pyspark.sql import SparkSession
from conexion import consulta
import os
import datetime as dt

if __name__=='__main__':
    while True:
        try:
            spark=SparkConf()
            spark= SparkSession.builder.appName("consulta1").config('spark.master','local[100]').config('spark.shuffle.sql.partitions',100).getOrCreate()
            
            qry   ="select * from f_car_creditos" 
            #qry="f_car_creditos"  
            print("--ini")
            print(dt.datetime.now())
            d1 = consulta("Prueba1", qry, spark)
            #d1=d1.coalesce(100)
            
            ruta= os.path.abspath('c:\\ssss2.csv')
            d1.write.csv(path= ruta, sep='|')
            
            spark.stop()
            print("--fin")
            print(dt.datetime.now())
            break
        except:
            print("erro")
            break

  
    
    
    
  