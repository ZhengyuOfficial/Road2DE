# Project 3: Data Warehouse

To access AWS, you need to do in AWS the following:
- Create IAM user
- Create IMA role with AmazonS3ReadOnlyAccess access rights
- Get ARN
- Create and run Redshift cluster

You can use Redshift IaC script to achieve above goals. 

The script README Link is [https://github.com/san089/Udacity-Data-Engineering-Projects/blob/master/Redshift_IaC_README.md]

Note that there might be something wrong with time zone!

After you successfully run the Redshift Iac script, you should run two .py file:
To create the tables to AWS Redshift

    python3 create_tables.py

To process all the inut data to the database in Redshift

    python3 etl.py

Do not forget to complete the DWH config(dwh.config) before run two .py files!

