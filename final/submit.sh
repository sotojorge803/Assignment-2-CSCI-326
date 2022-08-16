#!/bin/bash
aws emr add-steps --cluster-id j-26ZYPUCERMJBS --steps Type=Spark,Name="JJPROGRAM",ActionOnFailure=CONTINUE,Args=[s3://jacobsj-326/k_means_spark.py,s3://326-data-bucket/Mirai_dataset.csv,k_center_mirai_out1.txt,15]
