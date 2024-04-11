import logging
from keboola.component import UserException
from urllib.parse import urljoin
from keboola.http_client.async_client import AsyncHttpClient


BASE_URL = 'https://{0}.atlassian.net/rest/api/3/'
AGILE_URL = 'https://{0}.atlassian.net/rest/agile/1.0/'
MAX_RESULTS = 100
MAX_RESULTS_AGILE = 50


class JiraClient(AsyncHttpClient):

    def __init__(self, organization_id, username, api_token):

        self.param_base_url = BASE_URL.format(organization_id)
        self.param_agile_url = AGILE_URL.format(organization_id)
        self.param_username = username
        self.param_api_token = api_token

        super().__init__(self.param_base_url, auth=(self.param_username, self.param_api_token), retries=5,
                         default_headers={
                             'accept': 'application/json',
                             'content-type': 'application/json'
                         })

    async def get_projects(self):

        url_projects = urljoin(self.base_url, 'project')
        par_projects = {'expand': 'description'}
        rsp_projects = await self.get_raw(endpoint=url_projects, params=par_projects)

        if rsp_projects.status_code == 200:
            return rsp_projects.json()

        elif rsp_projects.status_code == 403 and \
                'Basic auth with password is not allowed on this instance' in rsp_projects.text:
            raise UserException("Could not authenticate against the API. Please, check the API token.")

        else:
            raise UserException(f"Unable to authenticate against {self.param_base_url}."
                                f"Received: {rsp_projects.status_code} - {rsp_projects.text}.")

    async def get_comments(self, issue_id: str):
        url_comments = urljoin(self.base_url, f'issue/{issue_id}/comment')

        r = await self.get_raw(endpoint=url_comments)
        sc, js = r.status_code, r.json()

        if sc == 200:
            comments = js['comments']

        else:
            logging.error(f"Could not download comments for issue {issue_id}.")
            logging.error(f"Received: {sc} - {js}.")
            return {}

        return comments

    async def get_changelogs(self, issue_key):

        url_changelogs = urljoin(self.base_url, f'issue/{issue_key}/changelog')
        offset = 0
        all_changelogs = []
        is_complete = False

        while is_complete is False:
            params_changelogs = {
                'startAt': offset,
                'maxResults': MAX_RESULTS
            }

            rsp_changelogs = await self.get_raw(endpoint=url_changelogs, params=params_changelogs)
            sc_changelogs, js_changelogs = rsp_changelogs.status_code, rsp_changelogs.json()

            if sc_changelogs == 200:
                all_changelogs += js_changelogs['values']
                offset += MAX_RESULTS
                is_complete = js_changelogs['isLast']

            else:
                raise UserException(f"Could not download changelogs for issue {issue_key}."
                                    f"Received: {sc_changelogs} - {js_changelogs}.")

        return all_changelogs

    async def get_issues(self, update_date=None, offset=0):

        url_issues = urljoin(self.param_base_url, 'search')
        param_jql = f'updated >= {update_date}' if update_date is not None else None
        is_complete = False

        params_issues = {
            'startAt': offset,
            'jql': param_jql,
            'maxResults': MAX_RESULTS,
            'expand': 'changelog'
        }

        rsp_issues = await self.get_raw(endpoint=url_issues, params=params_issues)

        if rsp_issues.status_code == 200:
            issues = rsp_issues.json()['issues']

            if len(issues) < MAX_RESULTS:
                is_complete = True

            else:
                offset += MAX_RESULTS

            return issues, is_complete, offset

        else:
            raise UserException(f"Could not download issues."
                                f"Received: {rsp_issues.status_code} - {rsp_issues.text}.")

    def get_issues_gen(self, update_date=None):
        url_issues = urljoin(self.param_base_url, 'search')
        param_jql = f'updated >= {update_date}' if update_date is not None else None
        offset = 0

        while True:
            params_issues = {
                'startAt': offset,
                'jql': param_jql,
                'maxResults': MAX_RESULTS,
                'expand': 'changelog'
            }

            rsp_issues = self.get_raw(url=url_issues, params=params_issues)

            if rsp_issues.status_code == 200:
                issues = rsp_issues.json()['issues']

                if not issues:
                    break  # No more issues to fetch

                for issue in issues:
                    yield issue

                if len(issues) < MAX_RESULTS:
                    break  # We've reached the end of the issues

                offset += MAX_RESULTS  # Prepare for the next batch of issues

            else:
                raise UserException(f"Could not download issues."
                                    f"Received: {rsp_issues.status_code} - {rsp_issues.text}.")

    async def get_users(self):

        url_users = urljoin(self.param_base_url, 'users')
        offset = 0
        all_users = []
        is_complete = False

        while is_complete is False:
            params_users = {
                'startAt': offset,
                'maxResults': MAX_RESULTS
            }

            rsp_users = await self.get_raw(endpoint=url_users, params=params_users)

            if rsp_users.status_code == 200:
                _usr = rsp_users.json()
                all_users += _usr

                if len(_usr) < MAX_RESULTS:
                    is_complete = True

                else:
                    offset += MAX_RESULTS

            else:
                raise UserException(f"Could not download users."
                                    f"Received: {rsp_users.status_code} - {rsp_users.text}.")

        return all_users

    async def get_fields(self):

        url_fields = urljoin(self.param_base_url, 'field')
        params_fields = {
            'expand': 'projects.issuetypes.fields'
        }

        rsp_fields = await self.get_raw(endpoint=url_fields, params=params_fields)

        if rsp_fields.status_code == 200:
            return rsp_fields.json()

        else:
            raise UserException(f"Could not download fields."
                                f"Received: {rsp_fields.status_code} - {rsp_fields.text}.")

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
                raise UserException(f"Could not download deleted worklogs."
                                    f"Received: {rsp_deleted.status_code} - {rsp_deleted.text}.")

        return all_worklogs

    async def get_updated_worklogs(self, since=None):

        url_updated = urljoin(self.param_base_url, 'worklog/updated')
        param_since = since
        is_complete = False
        all_worklogs = []

        while is_complete is False:

            params_updated = {
                'since': param_since
            }

            rsp_updated = await self.get_raw(endpoint=url_updated, params=params_updated)

            if rsp_updated.status_code == 200:
                js_worklogs = rsp_updated.json()
                all_worklogs += js_worklogs['values']

                if js_worklogs['lastPage'] is True:
                    is_complete = True

                else:
                    param_since = js_worklogs['until']

            else:
                raise UserException(f"Could not download updated worklogs."
                                    f"Received: {rsp_updated.status_code} - {rsp_updated.text}.")

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
                raise UserException(f"Could not download changed worklogs."
                                    f"Received: {rsp_worklogs.status_code} - {rsp_worklogs.text}.")

        return all_worklogs

    def get_all_boards(self):

        url_boards = urljoin(self.param_agile_url, 'board')
        offset = 0
        is_complete = False
        all_boards = []

        while is_complete is False:
            params_boards = {
                'startAt': offset,
                'maxResults': MAX_RESULTS_AGILE
            }

            rsp_boards = self.get_raw(url=url_boards, params=params_boards)

            if rsp_boards.status_code == 200:
                _brd = rsp_boards.json()
                all_boards += _brd['values']
                is_complete = _brd['isLast']
                offset += MAX_RESULTS_AGILE

            else:
                raise UserException(f"Could not download boards."
                                    f"Received: {rsp_boards.status_code} - {rsp_boards.text}.")

        return all_boards

    def get_all_customers(self):

        url_boards = urljoin(self.param_base_url, 'board')
        offset = 0
        is_complete = False
        all_boards = []

        while is_complete is False:
            params_boards = {
                'startAt': offset,
                'maxResults': MAX_RESULTS_AGILE
            }

            rsp_boards = self.get_raw(url=url_boards, params=params_boards)

            if rsp_boards.status_code == 200:
                _brd = rsp_boards.json()
                all_boards += _brd['values']
                is_complete = _brd['isLast']
                offset += MAX_RESULTS_AGILE

            else:
                raise UserException(f"Could not download boards."
                                    f"Received: {rsp_boards.status_code} - {rsp_boards.text}.")

        return all_boards

    async def get_custom_jql(self, jql, offset=0):
        url_issues = urljoin(self.param_base_url, 'search')
        is_complete = False

        params_issues = {
            'startAt': offset,
            'jql': jql,
            'maxResults': MAX_RESULTS,
            'expand': 'changelog'
        }

        rsp_issues = await self.get_raw(endpoint=url_issues, params=params_issues)

        if rsp_issues.status_code == 200:
            issues = rsp_issues.json()['issues']

            if len(issues) < MAX_RESULTS:
                is_complete = True

            else:
                offset += MAX_RESULTS

            return issues, is_complete, offset

        else:
            raise UserException(f"Could not download custom JQL."
                                f"Received: {rsp_issues.status_code} - {rsp_issues.text}.")

    async def get_board_sprints(self, board_id):

        url_sprints = urljoin(self.param_agile_url, f'board/{board_id}/sprint')
        offset = 0
        is_complete = False
        all_sprints = []

        while is_complete is False:
            params_sprints = {
                'startAt': offset,
                'maxResults': MAX_RESULTS_AGILE
            }

            rsp_sprints = await self.get_raw(url_sprints, params=params_sprints)

            if rsp_sprints.status_code == 200:
                _sprt = rsp_sprints.json()
                all_sprints += _sprt['values']
                is_complete = _sprt['isLast']
                offset += MAX_RESULTS_AGILE

            elif rsp_sprints.status_code == 400 and \
                    'The board does not support sprints' in rsp_sprints.json()['errorMessages']:
                break
            elif rsp_sprints.status_code == 400 and \
                    'Tabule nepodporuje sprinty' in rsp_sprints.json()['errorMessages']:
                break

            else:
                raise UserException(f"Could not download sprints for board {board_id}."
                                    f"Received: {rsp_sprints.status_code} - {rsp_sprints.text}.")

        return all_sprints

    def get_sprint_issues(self, sprint_id, update_date=None):

        url_issues = urljoin(self.param_agile_url, f'sprint/{sprint_id}/issue')
        param_jql = f'updated >= {update_date}' if update_date is not None else None
        is_complete = False
        offset = 0
        all_issues = []

        while is_complete is False:
            params_issues = {
                'startAt': offset,
                'maxResults': MAX_RESULTS,
                'jql': param_jql,
                'fields': 'id,key'
            }

            rsp_issues = self.get_raw(url_issues, params=params_issues)

            if rsp_issues.status_code == 200:
                _iss = rsp_issues.json()['issues']
                all_issues += _iss

                if len(_iss) < MAX_RESULTS:
                    is_complete = True

                else:
                    offset += MAX_RESULTS

            else:
                raise UserException(f"Could not download issues for sprint {sprint_id}."
                                    f"Received: {rsp_issues.status_code} - {rsp_issues.text}.")

        return all_issues
