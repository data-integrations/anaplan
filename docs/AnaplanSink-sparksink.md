# Anaplan sink

 Description
 -----------
 Sink plugin for supporting data import from CDF to Anaplan server.

 The placeholder with the same name as in the `Server file` filed must be predefined in the given Anaplan model.

 Properties
 ----------
 **Workspace Id:** The target Anaplan workspace ID.
 
 **Model Id:** The target Anaplan model ID.

 **Server File Name:** The name of the predefined placeholder file name in the model for this data import.

 **Service Location:** The root service location of the Anaplan API.
 
 **Auth Service Location:** The service location for the authentication.

 **User Name:** The service account used for the connection.

 **Password:** The password for authentication. It's suggested to use CDF build-in Cloud KMS integration 
 to manage the password and utilize macro in this filed. 
 Reference: [Provide secure information to pipelines](https://datafusion.atlassian.net/wiki/spaces/KB/pages/32276556/Provide+secure+information+to+pipelines)
