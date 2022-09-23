from datetime import timedelta
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
import pandas as pd
import io
import s3fs, fsspec
import pyarrow.parquet
import boto3
import json
import requests
import logging
import time,decimal
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrModifyClusterOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor

#reading the config files
def read_config(**kwargs):
	dag_run=kwargs.get('dag_run')
	app_config_path=dag_run.conf["app_config_file_path"] 
	path_content=app_config_path.replace(":","")
	s3_resource = path_content.split("/")
	s3=boto3.resource(s3_resource[0])
	object = s3.Object(path_content.split("/")[2],"/".join(path_content.split("/")[3:]))
	app_body = object.get()['Body'].read().decode('utf-8')
	jsonData = json.loads(app_body)
	return jsonData
	
#fetching the spark configuration path
def get_spark_config(**kwargs):
    dag_run=kwargs.get('dag_run')
    spark_path = dag_run.conf["spark_config"]
    path_content=spark_path.replace(":","")
    s3_resource = path_content.split("/")
    s3=boto3.resource(s3_resource[0])
    object = s3.Object(path_content.split("/")[2],"/".join(path_content.split("/")[3:]))
    app_body = object.get()['Body'].read().decode('utf-8')
    jsonData = json.loads(app_body)
    return jsonData
	
#copying data from landing zone to raw zone
def S3Landing_COPY_S3_Raw(**kwargs):
    ti=kwargs['ti']
    #boto3 s3 client object
    s3_client = boto3.client('s3')
    s3 = boto3.resource('s3')
    jsonData = ti.xcom_pull(task_ids="read_config_task")
    actives_source= jsonData['ingest-actives']['source']['data-location'].replace(":","")
    actives_destination=jsonData['ingest-actives']['destination']['data-location'].replace(":","")
    viewership_source=jsonData['ingest-viewership']['source']['data-location'].replace(":","")
    viewership_destination=jsonData['ingest-viewership']['destination']['data-location'].replace(":","")
    
	# Bucket and Key Names
    landing_bucket = actives_source.split("/")[2]
    raw_bucket = actives_destination.split("/")[2]
    actives_source_key = '/'.join(actives_source.split("/")[3:])
    actives_destination_key = '/'.join(actives_destination.split("/")[3:])
    viewership_source_key = '/'.join(viewership_source.split("/")[3:])
    viewership_destination_key = '/'.join(viewership_destination.split("/")[3:])
    
    # Actives
    copy_actives_source = {
        'Bucket' : landing_bucket,
        'Key' : actives_source_key + "/actives.parquet"
    }

    copy_actives_destination = s3.Bucket(raw_bucket)
    copy_actives_destination.copy(copy_actives_source, actives_destination_key + "/actives.parquet")
    
    # Viewership
    copy_viewership_source = {
        'Bucket' : landing_bucket,
        'Key' : viewership_source_key + "/viewership.parquet"
    }

    copy_viewership_destination = s3.Bucket(raw_bucket)
    copy_viewership_destination.copy(copy_viewership_source, viewership_destination_key + "/viewership.parquet")

def pre_validation(**kwargs):
    """
    This function validates the following checks with the data in raw-zone:
    1. Data Availability check
    2. Data Count check
    3. Data Type check
    """
    s3_resource = boto3.resource('s3')
    s3_obj = s3_resource
    ti = kwargs['ti']
    path = kwargs['dag_run'].conf['key']
    bucket = "saikumar-raw-zone"
    print(path)
    pre_validation_data = dict()
    print('/'.join(path.split('/')[:-1]))
        
    my_bucket = s3_obj.Bucket(bucket)
    
    for obj in my_bucket.objects.filter(Prefix='/'.join(path.split('/')[:-1])+"/"):
        print(obj.key)
        if obj.key.strip().split("/")[-1]=="":
            continue
        buffer = io.BytesIO()
        object = s3_obj.Object(bucket,obj.key)
        object.download_fileobj(buffer)
        df = pd.read_parquet(buffer, engine = 'pyarrow')
    
        if df.count()[0]==0:
            raise Exception("Data Validation Exception : No Data found in the dataset")
       
        #Adding count of df into the data dictoniary
        pre_validation_data['counts'] = str(int(pre_validation_data.get('counts','0'))+df.count()[0]) 
    
    ti.xcom_push(key="pre_validation_data",value=pre_validation_data)
	
global region_name
region_name = "ap-southeast-1"

# creation of cluster    
def cluster_creation(**kwargs):
    emr= boto3.client('emr',region_name=region_name)
	
    JOB_FLOW_OVERRIDES = emr.run_job_flow(
    Name='SaikumarEmrCluster' + str(datetime.now()),
    ReleaseLabel= 'emr-6.2.0',
    Configurations= get_spark_config(**kwargs),
	Applications=[{"Name": "Hadoop"},{"Name": "Spark"}, {"Name": "Livy"}],
    Instances= {
        'InstanceGroups':[ 
            {
                'Name': 'Master node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1
            },
			{
                'Name': "Slave nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'Ec2KeyName' : 'Saikumar-Keypair',
        'TerminationProtected': False,
        'EmrManagedMasterSecurityGroup': 'sg-064030b59176483f7',
        'EmrManagedSlaveSecurityGroup': 'sg-064030b59176483f7'
    },
    BootstrapActions =[
            {
                "Name": "custom_action",
                "ScriptBootstrapAction": {
                "Path": "s3://saikumar-landing-zone/dependencyfiles/dependencies.sh"
                }
            }
        ],
    JobFlowRole= 'EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole',
    VisibleToAllUsers =True,
    AutoTerminationPolicy = {"IdleTimeout": 3600},
    )
    return JOB_FLOW_OVERRIDES['JobFlowId']

def cluster_Ready_Acknowledge(**kwargs):
    emr = boto3.client('emr',region_name="ap-southeast-1")
    cluster_id = kwargs['ti'].xcom_pull(task_ids = "create_job_flow")
    emr.get_waiter('cluster_running').wait(ClusterId = cluster_id)
 
# fetching cluster DNS  
def get_cluster_dns(**kwargs):
    cluster_id = kwargs['ti'].xcom_pull(task_ids = 'create_job_flow')
    emr = boto3.client('emr', region_name="ap-southeast-1")
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['MasterPublicDnsName']
	
#method to submit livy
def submit_livy(**kwargs):
	# Pulling master from  function create_cluster
    master_DNS = kwargs['ti'].xcom_pull(task_ids='GetClusterDNS')
    
    # Retrieving spark_job_path from dag_run.conf
    spark_job_path = kwargs['dag_run'].conf["spark_job_path"]
    datasets  = kwargs['dag_run'].conf["filename"]
    # livy hostname
    host = "http://" + master_DNS + ":8998/batches"
    
    
    data = {"file":spark_job_path,"className":"com.example.SparkApp", 'args' : [datasets] }
    headers = {'Content-Type':'application/json'}
    try:
        response = requests.post(host, data=json.dumps(data), headers=headers)
    except (response.exceptions.ConnectionError, ValueError):
        return submit_livy()
    else:
        return response.json()

def post_Validation(**kwargs):
    """
    This function validates the following checks with the data in raw-zone:
    1. Data Availability check
    2. Data Count check
    3. Data Type check
    """
    ti = kwargs['ti']    
    # Pulling values from read_config_task function       
    jsonData = ti.xcom_pull(task_ids='read_config_task')
    # Pulling values from prevalidation function
    pre_validation_data = ti.xcom_pull(task_ids='pre_validation')
    
    # getting file details from dag_run conf
    file_name = kwargs['dag_run'].conf['filename']
    masked_data = 'transformed-{dataset}'.format(dataset = file_name)
    
    staging_bucket_data_count = 0
    # stagingzone path
    staging_path = jsonData[masked_data]['destination']['data-location'].replace(":","").split("/")
    
    s3_res = boto3.resource(staging_path[0])
    actives_staging_bucket = s3_res.Bucket(staging_path[2])
    prefix = str(staging_path[3]) + '/' + str(staging_path[4])
    
    count = 0
    for obj in actives_staging_bucket.objects.filter(Prefix=prefix):
        if (obj.key.endswith('parquet')):
            actives_staging_df = pd.read_parquet('s3://' + obj.bucket_name + '/' + obj.key, engine='auto')           
            staging_bucket_data_count += actives_staging_df.count()[0] 
            count += 1  
    # data availability check    
    if actives_staging_df.empty:
        raise Exception('No Data found in the dataset, Data Availability Exception!')
    
    # data count check
    if pre_validation_data['counts'] == staging_bucket_data_count:
        print("count_matches")
    else:
        print("count_mismatch")
        raise AirflowException('Counts Mismatch')
    
    print("Pre_count: ", pre_validation_data['counts'])
    print("Post_count: ", staging_bucket_data_count)

    # datatypes check
    transformation_cols = jsonData[masked_data]['transformation-cols']
    for col, val in transformation_cols.items():
        if 'DecimalType' in val:
            if isinstance(actives_staging_df[col][0], decimal.Decimal):
                continue
            else:
                raise AirflowException("Data mismatch for {}".format(col))
        if 'StringType' in val:
            if pd.api.types.is_string_dtype(actives_staging_df[col]):  
                continue
            else:
                raise AirflowException("Data mismatch for {}".format(col))
        else:
            raise AirflowException("No data in the column")
	

with DAG(
    dag_id='Star-Project-ml6',
    start_date= datetime(2022,7,1),
    schedule_interval='@once',
    dagrun_timeout=timedelta(minutes=60),
    catchup = False
) as dag:

    # reading configuration
    read_config_task = PythonOperator(task_id = 'read_config_task',python_callable = read_config)
    
    #copying data from landing zone to raw zone
    S3Landing_COPY_S3_Raw_task= PythonOperator(task_id = 'S3Landing_COPY_S3_Raw_task' ,python_callable = S3Landing_COPY_S3_Raw)
    
    #prevalidation
    pre_validation = PythonOperator(task_id= 'pre_validation', python_callable= pre_validation)
    
    #cluster creation
    cluster_creator = PythonOperator(task_id ='create_job_flow', python_callable = cluster_creation)
    
    cluster_ready_ackn = PythonOperator(task_id = "cluster_ready_acknowledge",python_callable = cluster_Ready_Acknowledge)
	
    # retriving cluster DNS
    get_cluster_dns_task = PythonOperator(task_id = 'GetClusterDNS', python_callable = get_cluster_dns)

    # submitting job through livy
    submit_livy_task = PythonOperator( task_id = 'submit_livy_task' , python_callable = submit_livy)
    
    #post_Validation
    post_Validation = PythonOperator(task_id= 'post_Validation', python_callable= post_Validation)

#dag flow
read_config_task >> S3Landing_COPY_S3_Raw_task >> pre_validation >> cluster_creator >>cluster_ready_ackn>> get_cluster_dns_task >> submit_livy_task >> post_Validation