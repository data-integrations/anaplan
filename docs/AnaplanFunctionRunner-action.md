# Anaplan function runner

 Description
 -----------
 Performs an Action/Process that is predefined in the given Anaplan model.

 This is most commonly used to prepare for the data import & export.

 The Action/Process must be predefined in the given Anaplan model.

 Properties
 ----------
 **Workspace Id:** The target Anaplan workspace ID.
 
 **Model Id:** The target Anaplan model ID.

 **Function Type:** The type (Action/Process) of the function to be run. 

 **Function Name:** The name of the predefined function to be run.

 **Service Location:** The root service location of the Anaplan API.
 
 **Auth Service Location:** The service location for the authentication.

 **User Name:** The service account used for the connection.

 **Password:** The password for authentication. It is suggested to use a secure macro to manage the password. 
 Reference: [Provide secure information to pipelines](https://datafusion.atlassian.net/wiki/spaces/KB/pages/32276556/Provide+secure+information+to+pipelines)
