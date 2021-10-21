# Anaplan function runner

Description
-----------
Performs an [Action](https://help.anaplan.com/69250a43-3266-4c04-bda9-bfb50ece9623-Actions) or [Process](https://help.anaplan.com/c01dd9ae-2390-4623-87bd-60b208a84f23-Processes) that is predefined in the given Anaplan model.

This is most commonly used to prepare for the data import & export.

The Action/Process must be predefined in the given Anaplan model.

Properties
----------
**Workspace ID** The target Anaplan Workspace ID.

**Model ID:** The target Anaplan Model ID.

**Function Type:** The type (Action/Process) of the function to be run.

**Process/Action Name:** The name of the predefined function to be run.

**Service Location:** The root service location of the Anaplan API.

**Auth Service Location:** The service location for the authentication.

**User Name:** The service account used for the connection.
Recommended: If the service account changes periodically, use a [macro](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/1188036697/Macros+and+macro+functions).

**Password:** The password for authentication. Recommended: Use [secure macros](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/1188036697/Macros+and+macro+functions#Secure-Function) for sensitive values like User passwords.