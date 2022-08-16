#!/bin/bash
aws emr add-steps --cluster-id j-35J8ITN20J3RK --steps Type=Spark,Name="JJPROGRAM",ActionOnFailure=CONTINUE,Args=[s3://jacobsj-326/comments_emr.py,s3://326-data-bucket/RC_2017-02]
