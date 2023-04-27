# Data Talks Club Final Porject - Pipeline 
Pipeline to measure aggregate US housing wealth from Zillow AVM and Census data
Measuring Aggregate Housing Wealth: New Insights from
Machine Learning

## Problem Statement

This paper introduces a new method to make use of local property value estimates that are derived from machine learning algorithms applied to detailed data on property sales and characteristics from public records and other sources. The paper combines these property value estimates with housing unit counts from the Census to derive new estimates of aggregate U.S. housing wealth from 2001 to 2018. The methodology generates estimates that should be of considerable value to researchers and policymakers interested in the dynamics of housing wealth and the role that it plays in economic outcomes.
## Data 


Vˆ (p,c,t) = NACS(p,c,t)V¯ Z(p,c,t),

NACS(p,c,t) = NACS(p,c,t | own use)+NACS(p,c,t | vacant)φ(p,c,t).


## Data Pipeline

![images](Pipeline.png)


### Terraform 

Terraform is an open-source infrastructure as code (IaC) tool used to build, manage and version infrastructure resources across multiple cloud providers, including AWS, Azure, and Google Cloud Platform. It allows you to define your infrastructure in code, and then provision and manage it using simple and repeatable workflows. With Terraform, you can automate the deployment and scaling of infrastructure resources, ensuring consistency and reducing the risk of errors.

## Prefect

Prefect is a Python-based open-source workflow automation tool used to build, schedule, and monitor data workflows. It provides a flexible and scalable platform for creating workflows that can run on your local machine or in the cloud. With Prefect, you can define your workflows as code, and then execute them on any infrastructure, making it easy to scale and integrate with your existing systems.

Airflow is also an open-source workflow automation tool, but it focuses on data processing and has a strong emphasis on scheduling and task dependencies. It allows you to define workflows using Python code, and then schedule and monitor their execution using a web-based interface. Airflow has a large community and many plugins available, making it a popular choice for data engineering and data science teams.

The main difference between Prefect and Airflow is their approach to workflow execution. Prefect is designed to be more flexible and can run workflows on any infrastructure, while Airflow has a strong focus on scheduling and task dependencies. Additionally, Prefect has a modern architecture that allows for easier customization and debugging, while Airflow has a more established ecosystem and is known for its stability and reliability. Ultimately, the choice between Prefect and Airflow will depend on your specific use case and requirements.

## Data Cloud Storage and Warehouse

Google Cloud Storage (GCS) is a cloud-based storage service provided by Google Cloud that allows users to store and manage their data objects at scale. GCS is highly available and offers low latency access to data from anywhere in the world. With different storage classes such as Standard, Nearline, and Coldline, GCS provides users with options to manage the cost of storing their data while ensuring high durability.

BigQuery is a fully-managed, cloud-native data warehouse solution provided by Google Cloud that allows users to analyze large datasets using SQL-like queries. It offers high scalability and can handle petabytes of data, making it suitable for businesses of all sizes. BigQuery also provides advanced features such as machine learning integration, real-time streaming, and data encryption to ensure users can efficiently derive insights from their data. Additionally, BigQuery integrates seamlessly with other Google Cloud services, including GCS, allowing users to process and analyze data at scale from one platform.

## DBT

dbt (data build tool) is an open-source data transformation and modeling tool that enables data teams to create a scalable and maintainable data pipeline. With dbt, users can transform and model their data using SQL-based code that can be version controlled and tested like traditional software code.
## Looker

Google Looker is a cloud-based business intelligence and analytics platform that helps businesses make data-driven decisions by providing a comprehensive view of their data. Looker allows users to create and share interactive dashboards, reports, and visualizations using a simple, web-based interface.

 [**Link to the dashboard:**](https://lookerstudio.google.com/reporting/1f5246de-10f4-409b-9dea-b6f6e062d034/page/hT9ND)

![images](Dashboard.png)




## Project setup

Make sure you have the GCC SDK installed: [Google Cloud SDK/CLI](https://cloud.google.com/sdk/docs/install)

### Running Local vs Server

This project could either be setup locally or on a compute engine like [Google Cloud Compute Engine](https://cloud.google.com/compute)

### Requirements 
*  [Terraform](https://www.terraform.io/)
*  [Prefect](https://www.prefect.io/)
*  [DBT Cloud](https://www.getdbt.com/)

### Installing Dependencies

To install the dependencies, run the following code:

`pip install -r requirements.txt`

### API Keys

You will need to source keys from the below APIs to run the pipeline:
* [NASDAQ API](https://data.nasdaq.com/sign-up)
* [US Housing and Urban Development API](https://www.huduser.gov/hudapi/public/register?comingfrom=1)
* [US Census API](https://www.census.gov/data/developers/about/terms-of-service.html)
* [FRED Econ Data API](https://fred.stlouisfed.org/docs/api/api_key.html)

These keys should be stored in the `configuration.conf` file

### Setup Terraform

Run these 3 lines of code to start Terraform: 

`terraform init`

`terraform plan`

`terraform apply`

If you ever need to dismantle the infrastructure, run the below code:

`terraform destroy`

### Setup Prefect

Prefect flows can be setup with the below code

`cd ~/Housing-Wealth-Pipeline`

`prefect deployment build prefect/<Prefect File> -n avm -o prefect/<Prefect File>.yaml`

`prefect deployment apply prefect/<Prefect File>.yaml`

`prefect agent start -p 'default-agent-pool'`

### Setup DBT
* Create BigQuery credential.json file (note, make sure GCC server location aligns with the json file)
* Create new DBT cloud project from repo
* Create new branch
* run `dbt build`

### Connect to Looker
