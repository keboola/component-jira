import logging
import sys
from urllib.parse import urljoin
from kbc.client_base import HttpClientBase

BASE_URL = 'https://{0}.atlassian.net/rest/api/3/'
MAX_RESULTS = 100


class JiraClient(HttpClientBase):

    def __init__(self, organization_id, username, api_token):

        self.param_base_url = BASE_URL.format(organization_id)
        self.param_username = username
        self.param_api_token = api_token

        super().__init__(self.param_base_url, auth=(self.param_username, self.param_api_token), max_retries=5,
                         default_http_header={
            'accept': 'application/json',
            'content-type': 'application/json'
        })

        _ = self.get_projects()

    def get_projects(self):

        url_projects = urljoin(self.base_url, 'project')

        rsp_projects = self.get_raw(url=url_projects)

        if rsp_projects.status_code == 200:
            return rsp_projects.json()

        elif rsp_projects.status_code == 403 and \
                'Basic auth with password is not allowed on this instance' in rsp_projects.text:
            logging.exception("Could not authenticate against the API. Please, check the API token.")
            sys.exit(1)

        else:
            logging.exception(f"Unable to authenticate against {self.param_base_url}.")
            logging.exception(f"Received: {rsp_projects.status_code} - {rsp_projects.text}.")
            sys.exit(1)

    def get_all_issues(self, update_date=None):

        url_issues = urljoin(self.param_base_url, 'search')
        param_jql = f'updated >= {update_date}' if update_date is not None else None
        offset = 0
        all_issues = []
        is_complete = False

        while is_complete is False:
            params_issues = {
                'startAt': offset,
                'jql': param_jql,
                'maxResults': MAX_RESULTS
            }

            rsp_issues = self.get_raw(url=url_issues, params=params_issues)

            if rsp_issues.status_code == 200:
                _iss = rsp_issues.json()['issues']
                all_issues += _iss

                if len(_iss) < MAX_RESULTS:
                    is_complete = True

                else:
                    offset += MAX_RESULTS

            else:
                logging.exception("Could not download issues.")
                logging.error(f"Received: {rsp_issues.status_code} - {rsp_issues.text}.")
                sys.exit(1)

        return all_issues

    def get_users(self):

        url_users = urljoin(self.param_base_url, 'users')
        offset = 0
        all_users = []
        is_complete = False

        while is_complete is False:
            params_users = {
                'startAt': offset,
                'maxResults': MAX_RESULTS
            }

            rsp_users = self.get_raw(url=url_users, params=params_users)

            if rsp_users.status_code == 200:
                _usr = rsp_users.json()
                all_users += _usr

                if len(_usr) < MAX_RESULTS:
                    is_complete = True

                else:
                    offset += MAX_RESULTS

            else:
                logging.exception("Could not download users.")
                logging.error(f"Received: {rsp_users.status_code} - {rsp_users.text}.")
                sys.exit(1)

        return all_users

    def get_fields(self):

        url_fields = urljoin(self.param_base_url, 'field')
        params_fields = {
            'expand': 'projects.issuetypes.fields'
        }

        rsp_fields = self.get_raw(url=url_fields, params=params_fields)

        if rsp_fields.status_code == 200:
            return rsp_fields.json()

        else:
            logging.exception("Could not download fields.")
            logging.error(f"Received: {rsp_fields.status_code} - {rsp_fields.text}.")
            sys.exit(1)

    @staticmethod
    def split_list_to_chunks(list_split, chunk_size):

        for i in range(0, len(list_split), chunk_size):
            yield list_split[i:i + chunk_size]

    def get_deleted_worklogs(self, since=None):

        url_deleted = urljoin(self.param_base_url, 'worklog/deleted')
        param_since = since
        is_complete = False
        all_worklogs = []

        while is_complete is False:

            params_deleted = {
                'since': param_since
            }

            rsp_deleted = self.get_raw(url=url_deleted, params=params_deleted)

            if rsp_deleted.status_code == 200:
                js_worklogs = rsp_deleted.json()
                all_worklogs += js_worklogs['values']

                if js_worklogs['lastPage'] is True:
                    is_complete = True

                else:
                    param_since = js_worklogs['until']

            else:
                logging.exception("Could not download deleted worklogs.")
                logging.error(f"Received: {rsp_deleted.status_code} - {rsp_deleted.text}.")
                sys.exit(1)

        return all_worklogs

    def get_updated_worklogs(self, since=None):

        url_updated = urljoin(self.param_base_url, 'worklog/updated')
        param_since = since
        is_complete = False
        all_worklogs = []

        while is_complete is False:

            params_updated = {
                'since': param_since
            }

            rsp_updated = self.get_raw(url=url_updated, params=params_updated)

            if rsp_updated.status_code == 200:
                js_worklogs = rsp_updated.json()
                all_worklogs += js_worklogs['values']

                if js_worklogs['lastPage'] is True:
                    is_complete = True

                else:
                    param_since = js_worklogs['until']

            else:
                logging.exception("Could not download updated worklogs.")
                logging.error(f"Received: {rsp_updated.status_code} - {rsp_updated.text}.")
                sys.exit(1)

        return all_worklogs

    def get_worklogs(self, worklog_ids):

        url_worklogs = urljoin(self.base_url, 'worklog/list')
        list_gen = self.split_list_to_chunks(worklog_ids, 1000)
        all_worklogs = []

        for w_list in list_gen:

            rsp_worklogs = self.post_raw(url=url_worklogs, json={'ids': w_list})

            if rsp_worklogs.status_code == 200:
                all_worklogs += rsp_worklogs.json()

            else:
                logging.exception("Could not download changed worklogs.")
                logging.error(f"Received: {rsp_worklogs.status_code} - {rsp_worklogs.json()}.")
                sys.exit(1)

        return all_worklogs
