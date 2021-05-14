# Anaplan sink

 Description
 -----------
 Sink plugin for supporting data import from CDF to Anaplan server.

 The placeholder with the same name as in the `Server file` field must be predefined in the given Anaplan model.
 
 Due to the limitation of Anaplan API 2.0, this plugin does not write data in parallel so may take some time on larger datasets.

 Properties
 ----------
 **Workspace Id:** The target Anaplan workspace ID.
 
 **Model Id:** The target Anaplan model ID.

 **Server File Name:** The name of the predefined placeholder file name in the model for this data import.

 **Service Location:** The root service location of the Anaplan API.
 
 **Auth Service Location:** The service location for the authentication.

 **User Name:** The service account used for the connection.

 **Password:** The password for authentication. It is suggested to use a secure macro to manage the password. 
 Reference: [Provide secure information to pipelines](https://datafusion.atlassian.net/wiki/spaces/KB/pages/32276556/Provide+secure+information+to+pipelines)

Data Type Mappings from CDAP to Anaplan
----------
The following table lists out different CDAP data types, as well as the 
corresponding Anaplan data type for each CDAP type.

| CDAP type      | Anaplan type  |
|----------------|---------------|
| array          | unsupported   |
| boolean        | bool          |
| bytes          | unsupported   |
| date           | date          |
| datetime       | unsupported   |
| decimal        | numeric       |
| double         | numeric       |
| enum           | unsupported   |
| float          | numeric       |
| int            | numeric       |
| long           | numeric       |
| map            | unsupported   |
| record         | unsupported   |
| string         | text          |
| time           | unsupported   |
| timestamp      | unsupported   |
| union          | unsupported   |
