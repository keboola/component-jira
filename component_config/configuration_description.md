A sample configuration can be found in the [component's repository](https://bitbucket.org/kds_consulting_team/kds-team.ex-jira/src/master/component_config/sample-config/config.json).

### Parameters

All parameters, except parameter for load type, are required.

- **Username**
    - **type:** required
    - **configuration name:** `username`
    - **description:** A username, which will be used to log in to Atlassian Cloud. Usually, the username is an email address of the user.
- **API Token**
    - **type:** required
    - **configuration name:** `#token`
    - **description:** An API token, which can be obtained in [Manage account](https://id.atlassian.com/manage/api-tokens) section
- **Organization ID**
    - **type:** required
    - **configuration name:** `organization_id`
    - **description:** ID of the organization of which data will be downloaded. The organization ID is located in the first part of the URL of the Atlassian stack; e.g. for *https://cool_org.atlassian.net/* the organization ID is *cool_org*.
- **Date Range**
    - **type:** required
    - **configuration name:** `since`
    - **description:** The date range, since when the data will be downloaded. Can be specified absolutely (e.g. **2020-01-01**) or relatively (e.g. **2 days ago**, **1 month ago**).
    - **default:** 3 days ago
- **Datasets**
    - **type:** required
    - **configuration name:** `datasets`
    - **description:** An array of objects, which will be downloaded.
    - **possible values:** `issues`, `worklogs`
- **Load Type**
    - **type:** optional
    - **configuration name:** `incremental`
    - **description:** Specifies load type back to storage.
    - **default:** `1` - `Incremental Load`