# Jira extractor

Jira is a proprietary issue tracking product developed by Atlassian that allows bug tracking and agile project management. A Jira extractor for Keboola Connection allows to download data about projects, issues and time worked on each issue.

## Configuration

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
    - **note:** If you'd like to download `issues_changelogs` dataset, **`issues` must be selected as well.**
    - **description:** An array of objects, which will be downloaded.
    - **possible values:** `issues`, `issues_changelogs`, `worklogs`, `boards_n_sprints`
- **Load Type**
    - **type:** optional
    - **configuration name:** `incremental`
    - **description:** Specifies load type back to storage.
    - **default:** `1` - `Incremental Load`

### Functionality notes

When fetching issues, take note that an update in the fixVersion does not update the "update" time of the issue. 
Therefore, if an issue's version is released, and the issue is no longer in the specified Date Range the data will not be fetched.
If fetching incrementally, this can lead to out of date data in the fixVersion field. 
Make sure to have a date range set to a long enough period to fetch the issue data.

## Development
 
This example contains runnable container with simple unittest. For local testing it is useful to include `data` folder in the root
and use docker-compose commands to run the container or execute tests. 

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to your custom path:
```yaml
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
```

Clone this repository, init the workspace and run the component with following command:

```
git clone https://bitbucket.org:kds_consulting_team/kds-team.ex-jira.git my-new-component
cd my-new-component
docker-compose build
docker-compose run --rm dev
```

Run the test suite and lint check using this command:

```
docker-compose run --rm test
```