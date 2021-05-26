# Anaplan source

Description
-----------
Source plugin for loading data from Anaplan model into CDF pipeline.

The data exported from Anaplan model will be firstly materialized in the given GCS bucket, 
and the inbound data stream to the CDF pipeline will be sourced from the file.

There are two connections will be made using the provided property values. The `Anaplan Properties` are used for downloading the target data file from Anaplan server. 
The `GCS Properties` are used for the connection with GCS, which archives the downloaded data file in the given GCS bucket for sourcing the data into the CDF pipeline.

Anaplan Properties
----------
**Workspace Id:** The target Anaplan workspace ID.
 
**Model Id:** The target Anaplan model ID.

**Server File Name:** The name of the predefined placeholder file name in the model for this data import.

**Service Location:** The root service location of the Anaplan API.
 
**Auth Service Location:** The service location for the authentication.

**User Name:** The service account used for the connection.

**Password:** The password for authentication. It is suggested to use a secure macro to manage the password. 
Reference: [Provide secure information to pipelines](https://datafusion.atlassian.net/wiki/spaces/KB/pages/32276556/Provide+secure+information+to+pipelines)

GCP Credentials
-----------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be
provided and can be set to 'auto-detect'.
The GCP credentials will be automatically read from the cluster environment (the GCP project where this CDF instance is hosted).

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided.
The service account key can be found on the Dashboard in the Cloud Platform Console.
Make sure the account key has permission to access BigQuery and Google Cloud Storage.
The service account key file needs to be available on every node in your cluster and
must be readable by all users running the job.

GCS & General Properties
----------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**GCS Bucket as Buffer:** The name of the GCS bucket serving as the buffer for the Anaplan data file export.

**File archive name in GCS bucket:** The desired file name for the data export from Anaplan model.

**Format:** Format of the data that export from the Anaplan model.
The format must be one of 'csv' and 'tsv'.

**Service Account**  - service account key used for authorization

* **File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

* **JSON**: Contents of the service account JSON file.

**Schema:** Output schema. If a Path Field is set, it must be present in the schema as a string.

Data Type Mappings from Anaplan to CDAP
----------
The following table lists out different Anaplan data types, as well as the 
corresponding CDAP data type for each Anaplan type.

| Anaplan type   | CDAP type      |
|--------------- |----------------| 
| text           | string         |
| bool           | boolean        |
| date           | date           |
| numeric        | decimal        |