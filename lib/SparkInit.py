#!/bin/python
# -*- coding: utf-8 -*-

###################
# SparkInit class #
###################


class SparkInit:
    
    import os
    from pyspark.sql import SparkSession

    spark = None
    sc = None
    
    APPLICATION_HOME = None
    APPLICATION_NAME = None
    APPLICATION_LIB_DIR = None
    DEV_MODE = False
    HIVE_SITE_XML_FILE = None

    def __init__(self, spark=None, sc=None, APPLICATION_HOME=None, DEFAULT_APPLICATION_NAME=None, APPLICATION_LIB_DIR=None):
        self.APPLICATION_HOME = APPLICATION_HOME
        self.APPLICATION_NAME = self.getEnvVar("APPLICATION_NAME", DEFAULT_APPLICATION_NAME)
        self.APPLICATION_LIB_DIR = APPLICATION_LIB_DIR
        # PROD MODE
        if (spark == None or sc == None):
            self.DEV_MODE = False
            self.HIVE_SITE_XML_FILE = self.getEnvVar("HIVE_SITE_XML_FILE")
            if (self.HIVE_SITE_XML_FILE != None and self.os.path.exists(self.HIVE_SITE_XML_FILE)):
                self.spark = self.SparkSession.builder.appName(self.APPLICATION_NAME).enableHiveSupport().getOrCreate()
            else:
                self.spark = self.SparkSession.builder.appName(self.APPLICATION_NAME).getOrCreate()
            self.sc = self.spark.sparkContext
        # DEV MODE
        else:
            self.DEV_MODE = True
            self.spark = spark
            self.sc = sc

    def getEnvVar(self, varName, defaultValue=None):
        value = None
        try:
            value = self.os.environ[varName.strip()]
        except Exception as e:
            value = None
        if (value == None): value = defaultValue
        return value

