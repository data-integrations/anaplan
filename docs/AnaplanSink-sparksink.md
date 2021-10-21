# Anaplan sink

Description
-----------
Writes data into an Anaplan model.

Due to the limitation of Anaplan API 2.0, this plugin does not write data in parallel so larger datasets might take longer to load into the Anaplan server.

Properties
----------
**Workspace ID:** The target Anaplan Workspace ID.

**Model ID:** The target Anaplan Model ID.

**Server File Name:** The name of the placeholder file in the Anaplan model. You must define the placeholder file in the Anaplan model before using the Anaplan sink in a pipeline.

**Service Location:** The root service location of the Anaplan API.

**Auth Service Location:** The service location for the authentication.

**User Name:** The service account used for the connection.
Recommended: If the service account changes periodically, use a [macro](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/1188036697/Macros+and+macro+functions).

**Password:** The password for authentication. Recommended: Use [secure macros](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/1188036697/Macros+and+macro+functions#Secure-Function) for sensitive values like User passwords.


Data Type Mappings from CDAP to Anaplan
----------
The following table lists different CDAP data types and the
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
