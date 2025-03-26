import logging
from keboola.component import UserException
from urllib.parse import urljoin
from keboola.http_client.async_client import AsyncHttpClient
import httpx
import json
import psutil
import os

BASE_URL = 'https://{0}.atlassian.net/rest/api/3/'
AGILE_URL = 'https://{0}.atlassian.net/rest/agile/1.0/'
SERVICEDESK_URL = 'https://{0}.atlassian.net/rest/servicedeskapi/'
MAX_RESULTS = 100
MAX_RESULTS_AGILE = 50
MAX_RESULTS_SERVICEDESK = 50

logger = logging.getLogger(__name__)


def get_memory_usage():
    """Get current memory usage of the process"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024  # Convert to MB


def log_request_details(method, url, params=None, headers=None, json_data=None):
    """Helper function to log request details"""
    logger.info(f"Making {method} request to: {url}")
    if params:
        logger.info(f"Request parameters: {json.dumps(params, indent=2)}")
    if headers:
        logger.info(f"Request headers: {json.dumps(headers, indent=2)}")
    if json_data:
        logger.info(f"Request body: {json.dumps(json_data, indent=2)}")
    logger.info(f"Current memory usage: {get_memory_usage():.2f} MB")


def log_response_details(response):
    """Helper function to log response details"""
    response_size = len(response.content) / 1024 / 1024  # Convert to MB
    logger.info(f"Response size: {response_size:.2f} MB")
    logger.info(f"Memory usage after response: {get_memory_usage():.2f} MB")
    return response_size


class JiraClient(AsyncHttpClient):

    def __init__(self, organization_id, username, api_token):
        logger.info(f"Initializing JiraClient for organization: {organization_id}")
        self.param_base_url = BASE_URL.format(organization_id)
        self.param_agile_url = AGILE_URL.format(organization_id)
        self.param_servicedesk_url = SERVICEDESK_URL.format(organization_id)
        self.param_username = username
        self.param_api_token = api_token

        headers = {
            'accept': 'application/json',
            'content-type': 'application/json'
        }
        logger.info(f"Initializing with headers: {json.dumps(headers, indent=2)}")

        super().__init__(self.param_base_url, auth=(self.param_username, self.param_api_token), retries=5,
                         default_headers=headers)
        logger.info("JiraClient initialized successfully")

    async def get_projects(self):
        logger.info("Starting to fetch projects")
        url_projects = urljoin(self.base_url, 'project')
        par_projects = {'expand': 'description'}

        log_request_details('GET', url_projects, params=par_projects)

        try:
            rsp_projects = await self.get_raw(endpoint=url_projects, params=par_projects)
            log_response_details(rsp_projects)

            if rsp_projects.status_code == 200:
                logger.info("Successfully fetched projects")
                return rsp_projects.json()
            else:
                logger.error(
                    f"Failed to get projects. Status: {rsp_projects.status_code}, Response: {rsp_projects.text}"
                )
                raise UserException(f"Unable to get projects from {self.param_base_url}. "
                                    f"Received: {rsp_projects.status_code} - {rsp_projects.text}.")

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403 and 'Basic auth with password is not allowed' in e.response.text:
                logger.error("Authentication failed - API token issue")
                raise UserException("Could not authenticate against the API. Please, check the API token.")
            else:
                logger.error(f"HTTP error while fetching projects: {e.response.status_code} - {e.response.text}")
                raise UserException(f"Unable to get projects from {self.param_base_url}. "
                                    f"Received: {e.response.status_code} - {e.response.text}.")

    async def get_comments(self, issue_id: str):
        logger.info(f"Starting to fetch comments for issue: {issue_id}")
        url_comments = urljoin(self.base_url, f'issue/{issue_id}/comment')

        params = {
            'expand': 'properties'
        }

        log_request_details('GET', url_comments, params=params)

        try:
            r = await self.get_raw(endpoint=url_comments, params=params)
            log_response_details(r)
            sc, js = r.status_code, r.json()

            if sc == 200:
                logger.info(f"Successfully fetched comments for issue {issue_id}")
                comments = js['comments']
            else:
                logger.error(f"Failed to fetch comments for issue {issue_id}. Status: {sc}, Response: {js}")
                comments = {}

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error while fetching comments for issue {issue_id}: {e.response.text}")
            comments = {}

        return comments

    async def get_changelogs(self, issue_key):
        logger.info(f"Starting to fetch changelogs for issue: {issue_key}")
        url_changelogs = urljoin(self.base_url, f'issue/{issue_key}/changelog')
        offset = 0
        all_changelogs = []
        is_complete = False

        while is_complete is False:
            params_changelogs = {
                'startAt': offset,
                'maxResults': MAX_RESULTS
            }

            log_request_details('GET', url_changelogs, params=params_changelogs)

            try:
                rsp_changelogs = await self.get_raw(endpoint=url_changelogs, params=params_changelogs)
                log_response_details(rsp_changelogs)
                sc_changelogs, js_changelogs = rsp_changelogs.status_code, rsp_changelogs.json()

                if sc_changelogs == 200:
                    all_changelogs += js_changelogs['values']
                    logger.info(
                        f"Fetched {len(js_changelogs['values'])} changelog entries. Total: {len(all_changelogs)}"
                    )
                    offset += MAX_RESULTS
                    is_complete = js_changelogs['isLast']
                else:
                    logger.error(f"Failed to fetch changelogs. Status: {sc_changelogs}, Response: {js_changelogs}")
                    raise UserException(f"Could not download changelogs for issue {issue_key}."
                                        f"Received: {sc_changelogs} - {js_changelogs}.")

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error while fetching changelogs: {e.response.status_code} - {e.response.text}")
                raise UserException(f"Could not download changelogs for issue {issue_key}."
                                    f"Received: {e.response.status_code} - {e.response.text}.")

        logger.info(f"Successfully fetched all changelogs for issue {issue_key}. Total count: {len(all_changelogs)}")
        return all_changelogs

    async def get_issues(self, update_date=None, offset=0, issue_jql_filter=None):
        logger.info(f"Starting to fetch issues with JQL filter: {issue_jql_filter or f'updated >= {update_date}'}")
        url_issues = urljoin(self.param_base_url, 'search')
        if issue_jql_filter:
            param_jql = issue_jql_filter
        else:
            param_jql = f'updated >= {update_date}' if update_date else None

        is_complete = False

        params_issues = {
            'startAt': offset,
            'jql': param_jql,
            'maxResults': MAX_RESULTS,
            'expand': 'changelog',
            'fields': 'key,summary,status,created,updated,issuetype,project,priority'  # Omezíme pole pro menší odpovědi
        }

        log_request_details('GET', url_issues, params=params_issues)

        try:
            rsp_issues = await self.get_raw(endpoint=url_issues, params=params_issues)
            log_response_details(rsp_issues)
            response_size = log_response_details(rsp_issues)

            if rsp_issues.status_code == 200:
                issues = rsp_issues.json()['issues']
                logger.info(f"Successfully fetched {len(issues)} issues")
                logger.info(f"Average issue size: {response_size / len(issues):.2f} MB per issue")

                if len(issues) < MAX_RESULTS:
                    is_complete = True
                    logger.info("All issues fetched")
                else:
                    offset += MAX_RESULTS
                    logger.info(f"More issues available, next offset: {offset}")

                return issues, is_complete, offset
            else:
                logger.error(f"Failed to fetch issues. Status: {rsp_issues.status_code}, Response: {rsp_issues.text}")
                raise UserException(f"Could not download issues."
                                    f"Received: {rsp_issues.status_code} - {rsp_issues.text}.")

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error while fetching issues: {e.response.status_code} - {e.response.text}")
            raise UserException(f"Could not download issues."
                                f"Received: {e.response.status_code} - {e.response.text}.")

    async def get_users(self):
        logger.info("Starting to fetch users")
        url_users = urljoin(self.param_base_url, 'users')
        offset = 0
        all_users = []
        is_complete = False

        while is_complete is False:
            params_users = {
                'startAt': offset,
                'maxResults': MAX_RESULTS
            }

            log_request_details('GET', url_users, params=params_users)

            try:
                rsp_users = await self.get_raw(endpoint=url_users, params=params_users)
                log_response_details(rsp_users)

                if rsp_users.status_code == 200:
                    _usr = rsp_users.json()
                    all_users += _usr
                    logger.info(f"Fetched {len(_usr)} users. Total: {len(all_users)}")

                    if len(_usr) < MAX_RESULTS:
                        is_complete = True
                        logger.info("All users fetched")
                    else:
                        offset += MAX_RESULTS
                        logger.info(f"More users available, next offset: {offset}")
                else:
                    logger.error(f"Failed to fetch users. Status: {rsp_users.status_code}, Response: {rsp_users.text}")
                    raise UserException(f"Could not download users."
                                        f"Received: {rsp_users.status_code} - {rsp_users.text}.")

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error while fetching users: {e.response.status_code} - {e.response.text}")
                raise UserException(f"Could not download users."
                                    f"Received: {e.response.status_code} - {e.response.text}.")

        logger.info(f"Successfully fetched all users. Total count: {len(all_users)}")
        return all_users

    async def get_organizations(self):
        logger.info("Starting to fetch organizations")
        url_organizations = urljoin(self.param_servicedesk_url, 'organization')
        offset = 0
        all_organizations = []
        is_complete = False

        while is_complete is False:
            params_organizations = {
                'start': offset,
                'limit': MAX_RESULTS_SERVICEDESK
            }

            log_request_details('GET', url_organizations, params=params_organizations)

            try:
                rsp_organizations = await self.get_raw(endpoint=url_organizations, params=params_organizations)
                log_response_details(rsp_organizations)

                if rsp_organizations.status_code == 200:
                    _usr = rsp_organizations.json()['values']
                    all_organizations += _usr
                    logger.info(f"Fetched {len(_usr)} organizations. Total: {len(all_organizations)}")

                    if len(_usr) < MAX_RESULTS_SERVICEDESK:
                        is_complete = True
                        logger.info("All organizations fetched")
                    else:
                        offset += MAX_RESULTS_SERVICEDESK
                        logger.info(f"More organizations available, next offset: {offset}")
                else:
                    logger.error(
                        f"Failed to fetch organizations. Status: {rsp_organizations.status_code}, "
                        f"Response: {rsp_organizations.text}"
                    )
                    raise UserException(f"Could not download organizations."
                                        f"Received: {rsp_organizations.status_code} - {rsp_organizations.text}.")

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error while fetching organizations: {e.response.status_code} - {e.response.text}")
                raise UserException(f"Could not download organizations."
                                    f"Received: {e.response.status_code} - {e.response.text}.")

        logger.info(f"Successfully fetched all organizations. Total count: {len(all_organizations)}")
        return all_organizations

    async def get_servicedesks(self):
        logger.info("Starting to fetch servicedesks")
        url_organizations = urljoin(self.param_servicedesk_url, 'servicedesk')
        offset = 0
        all_servicedesks = []
        is_complete = False

        while is_complete is False:
            params_servicedesks = {
                'start': offset,
                'limit': MAX_RESULTS_SERVICEDESK
            }

            log_request_details('GET', url_organizations, params=params_servicedesks)

            try:
                rsp_servicedesks = await self.get_raw(endpoint=url_organizations, params=params_servicedesks)
                log_response_details(rsp_servicedesks)

                if rsp_servicedesks.status_code == 200:
                    _usr = rsp_servicedesks.json()['values']
                    all_servicedesks += _usr
                    logger.info(f"Fetched {len(_usr)} servicedesks. Total: {len(all_servicedesks)}")

                    if len(_usr) < MAX_RESULTS_SERVICEDESK:
                        is_complete = True
                        logger.info("All servicedesks fetched")
                    else:
                        offset += MAX_RESULTS_SERVICEDESK
                        logger.info(f"More servicedesks available, next offset: {offset}")
                else:
                    logger.error(
                        f"Failed to fetch servicedesks. Status: {rsp_servicedesks.status_code}, "
                        f"Response: {rsp_servicedesks.text}"
                    )
                    raise UserException(f"Could not download servicedesks."
                                        f"Received: {rsp_servicedesks.status_code} - {rsp_servicedesks.text}.")

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error while fetching servicedesks: {e.response.status_code} - {e.response.text}")
                raise UserException(f"Could not download servicedesks."
                                    f"Received: {e.response.status_code} - {e.response.text}.")

        logger.info(f"Successfully fetched all servicedesks. Total count: {len(all_servicedesks)}")
        return all_servicedesks

    async def get_servicedesk_customers(self, servicedesk_id: str):
        logger.info(f"Starting to fetch customers for servicedesk: {servicedesk_id}")
        url_organization_users = urljoin(self.param_servicedesk_url, f'servicedesk/{servicedesk_id}/customer')
        offset = 0
        all_users = []
        is_complete = False

        while is_complete is False:
            params_organization_users = {
                'start': offset,
                'limit': MAX_RESULTS_SERVICEDESK
            }

            headers = {"X-ExperimentalApi": "opt-in"}
            log_request_details('GET', url_organization_users, params=params_organization_users, headers=headers)

            try:
                rsp_users = await self.get_raw(endpoint=url_organization_users, params=params_organization_users,
                                               headers=headers)
                log_response_details(rsp_users)

                if rsp_users.status_code == 200:
                    _usr = rsp_users.json()['values']
                    all_users += _usr
                    logger.info(f"Fetched {len(_usr)} customers. Total: {len(all_users)}")

                    if len(_usr) < MAX_RESULTS_SERVICEDESK:
                        is_complete = True
                        logger.info("All customers fetched")
                    else:
                        offset += MAX_RESULTS_SERVICEDESK
                        logger.info(f"More customers available, next offset: {offset}")
                else:
                    logger.error(
                        f"Failed to fetch customers. Status: {rsp_users.status_code}, Response: {rsp_users.text}"
                    )
                    raise UserException(f"Could not download users."
                                        f"Received: {rsp_users.status_code} - {rsp_users.text}.")

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error while fetching customers: {e.response.status_code} - {e.response.text}")
                raise UserException(f"Could not download users."
                                    f"Received: {e.response.status_code} - {e.response.text}.")

        logger.info(f"Successfully fetched all customers. Total count: {len(all_users)}")
        return all_users

    async def get_fields(self):
        logger.info("Starting to fetch fields")
        url_fields = urljoin(self.param_base_url, 'field')
        params_fields = {
            'expand': 'projects.issuetypes.fields'
        }

        log_request_details('GET', url_fields, params=params_fields)

        try:
            rsp_fields = await self.get_raw(endpoint=url_fields, params=params_fields)
            log_response_details(rsp_fields)

            if rsp_fields.status_code == 200:
                logger.info("Successfully fetched fields")
                return rsp_fields.json()
            else:
                logger.error(f"Failed to fetch fields. Status: {rsp_fields.status_code}, Response: {rsp_fields.text}")
                raise UserException(f"Could not download fields."
                                    f"Received: {rsp_fields.status_code} - {rsp_fields.text}.")

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error while fetching fields: {e.response.status_code} - {e.response.text}")
            raise UserException(f"Could not download fields."
                                f"Received: {e.response.status_code} - {e.response.text}.")

    @staticmethod
    def split_list_to_chunks(list_split, chunk_size):
        logger.info(f"Splitting list into chunks of size {chunk_size}")
        for i in range(0, len(list_split), chunk_size):
            yield list_split[i:i + chunk_size]

    async def get_deleted_worklogs(self, since=None):
        logger.info(f"Starting to fetch deleted worklogs since: {since}")
        url_deleted = urljoin(self.param_base_url, 'worklog/deleted')
        param_since = since
        is_complete = False
        all_worklogs = []

        while is_complete is False:
            params_deleted = {
                'since': param_since
            }

            log_request_details('GET', url_deleted, params=params_deleted)

            try:
                rsp_deleted = await self.get_raw(endpoint=url_deleted, params=params_deleted)
                log_response_details(rsp_deleted)

                if rsp_deleted.status_code == 200:
                    js_worklogs = rsp_deleted.json()
                    all_worklogs += js_worklogs['values']
                    logger.info(f"Fetched {len(js_worklogs['values'])} deleted worklogs. Total: {len(all_worklogs)}")

                    if js_worklogs['lastPage'] is True:
                        is_complete = True
                        logger.info("All deleted worklogs fetched")
                    else:
                        param_since = js_worklogs['until']
                        logger.info(f"More deleted worklogs available, next since: {param_since}")
                else:
                    logger.error(
                        f"Failed to fetch deleted worklogs. Status: {rsp_deleted.status_code}, "
                        f"Response: {rsp_deleted.text}"
                    )
                    raise UserException(f"Could not download deleted worklogs."
                                        f"Received: {rsp_deleted.status_code} - {rsp_deleted.text}.")

            except httpx.HTTPStatusError as e:
                logger.error(
                    f"HTTP error while fetching deleted worklogs: {e.response.status_code} - {e.response.text}"
                )
                raise UserException(f"Could not download deleted worklogs."
                                    f"Received: {e.response.status_code} - {e.response.text}.")

        logger.info(f"Successfully fetched all deleted worklogs. Total count: {len(all_worklogs)}")
        return all_worklogs

    async def get_updated_worklogs(self, since=None):
        logger.info(f"Starting to fetch updated worklogs since: {since}")
        url_updated = urljoin(self.param_base_url, 'worklog/updated')
        param_since = since
        is_complete = False
        all_worklogs = []

        while is_complete is False:
            params_updated = {
                'since': param_since
            }

            log_request_details('GET', url_updated, params=params_updated)

            try:
                rsp_updated = await self.get_raw(endpoint=url_updated, params=params_updated)
                log_response_details(rsp_updated)

                if rsp_updated.status_code == 200:
                    js_worklogs = rsp_updated.json()
                    all_worklogs += js_worklogs['values']
                    logger.info(f"Fetched {len(js_worklogs['values'])} updated worklogs. Total: {len(all_worklogs)}")

                    if js_worklogs['lastPage'] is True:
                        is_complete = True
                        logger.info("All updated worklogs fetched")
                    else:
                        param_since = js_worklogs['until']
                        logger.info(f"More updated worklogs available, next since: {param_since}")
                else:
                    logger.error(
                        f"Failed to fetch updated worklogs. Status: {rsp_updated.status_code}, "
                        f"Response: {rsp_updated.text}"
                    )
                    raise UserException(f"Could not download updated worklogs."
                                        f"Received: {rsp_updated.status_code} - {rsp_updated.text}.")

            except httpx.HTTPStatusError as e:
                logger.error(
                    f"HTTP error while fetching updated worklogs: {e.response.status_code} - {e.response.text}"
                )
                raise UserException(f"Could not download updated worklogs."
                                    f"Received: {e.response.status_code} - {e.response.text}.")

        logger.info(f"Successfully fetched all updated worklogs. Total count: {len(all_worklogs)}")
        return all_worklogs

    async def get_worklogs(self, worklog_ids):
        logger.info(f"Starting to fetch worklogs for {len(worklog_ids)} IDs")
        url_worklogs = urljoin(self.base_url, 'worklog/list')
        list_gen = self.split_list_to_chunks(worklog_ids, 1000)
        all_worklogs = []

        for w_list in list_gen:
            try:
                json_data = {'ids': w_list}
                log_request_details('POST', url_worklogs, json_data=json_data)

                rsp_worklogs = await self.post_raw(endpoint=url_worklogs, json=json_data)
                log_response_details(rsp_worklogs)

                if rsp_worklogs.status_code == 200:
                    all_worklogs += rsp_worklogs.json()
                    logger.info(
                        f"Successfully fetched {len(rsp_worklogs.json())} worklogs. Total: {len(all_worklogs)}"
                    )
                else:
                    logger.error(
                        f"Failed to fetch worklogs. Status: {rsp_worklogs.status_code}, Response: {rsp_worklogs.text}"
                    )
                    raise UserException(f"Could not download changed worklogs."
                                        f"Received: {rsp_worklogs.status_code} - {rsp_worklogs.text}.")

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error while fetching worklogs: {e.response.status_code} - {e.response.text}")
                raise UserException(f"Could not download changed worklogs."
                                    f"Received: {e.response.status_code} - {e.response.text}.")

        logger.info(f"Successfully fetched all worklogs. Total count: {len(all_worklogs)}")
        return all_worklogs

    async def get_all_boards(self):
        logger.info("Starting to fetch all boards")
        url_boards = urljoin(self.param_agile_url, 'board')
        offset = 0
        is_complete = False
        all_boards = []

        while is_complete is False:
            params_boards = {
                'startAt': offset,
                'maxResults': MAX_RESULTS_AGILE
            }

            log_request_details('GET', url_boards, params=params_boards)

            try:
                rsp_boards = await self.get_raw(endpoint=url_boards, params=params_boards)
                log_response_details(rsp_boards)

                if rsp_boards.status_code == 200:
                    _brd = rsp_boards.json()
                    all_boards += _brd['values']
                    logger.info(f"Fetched {len(_brd['values'])} boards. Total: {len(all_boards)}")
                    is_complete = _brd['isLast']
                    offset += MAX_RESULTS_AGILE
                    logger.info(f"More boards available: {not is_complete}")
                else:
                    logger.error(
                        f"Failed to fetch boards. Status: {rsp_boards.status_code}, Response: {rsp_boards.text}"
                    )
                    raise UserException(f"Could not download boards."
                                        f"Received: {rsp_boards.status_code} - {rsp_boards.text}.")

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error while fetching boards: {e.response.status_code} - {e.response.text}")
                raise UserException(f"Could not download boards. "
                                    f"Received: {e.response.status_code} - {e.response.text}.")

        logger.info(f"Successfully fetched all boards. Total count: {len(all_boards)}")
        return all_boards

    def get_all_customers(self):
        logger.info("Starting to fetch all customers")
        url_boards = urljoin(self.param_base_url, 'board')
        offset = 0
        is_complete = False
        all_boards = []

        while is_complete is False:
            params_boards = {
                'startAt': offset,
                'maxResults': MAX_RESULTS_AGILE
            }

            log_request_details('GET', url_boards, params=params_boards)

            try:
                rsp_boards = self.get_raw(url=url_boards, params=params_boards)
                log_response_details(rsp_boards)

                if rsp_boards.status_code == 200:
                    _brd = rsp_boards.json()
                    all_boards += _brd['values']
                    logger.info(f"Fetched {len(_brd['values'])} customers. Total: {len(all_boards)}")
                    is_complete = _brd['isLast']
                    offset += MAX_RESULTS_AGILE
                    logger.info(f"More customers available: {not is_complete}")

                else:
                    logger.error(
                        f"Failed to fetch customers. Status: {rsp_boards.status_code}, Response: {rsp_boards.text}"
                    )
                    raise UserException(f"Could not download boards."
                                        f"Received: {rsp_boards.status_code} - {rsp_boards.text}.")

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error while fetching customers: {e.response.status_code} - {e.response.text}")
                raise UserException(f"Could not download boards. "
                                    f"Received: {e.response.status_code} - {e.response.text}.")

        logger.info(f"Successfully fetched all customers. Total count: {len(all_boards)}")
        return all_boards

    async def get_custom_jql(self, jql, offset=0):
        logger.info(f"Starting to fetch issues with custom JQL: {jql}")
        url_issues = urljoin(self.param_base_url, 'search')
        is_complete = False

        params_issues = {
            'startAt': offset,
            'jql': jql,
            'maxResults': MAX_RESULTS,
            'expand': 'changelog'
        }

        log_request_details('GET', url_issues, params=params_issues)

        try:
            rsp_issues = await self.get_raw(endpoint=url_issues, params=params_issues)
            log_response_details(rsp_issues)

            if rsp_issues.status_code == 200:
                issues = rsp_issues.json()['issues']
                logger.info(f"Successfully fetched {len(issues)} issues")

                if len(issues) < MAX_RESULTS:
                    is_complete = True
                    logger.info("All issues fetched")
                else:
                    offset += MAX_RESULTS
                    logger.info(f"More issues available, next offset: {offset}")

                return issues, is_complete, offset
            else:
                logger.error(
                    f"Failed to fetch issues with custom JQL. Status: {rsp_issues.status_code}, "
                    f"Response: {rsp_issues.text}"
                )
                raise UserException(f"Could not download custom JQL."
                                    f"Received: {rsp_issues.status_code} - {rsp_issues.text}.")

        except httpx.HTTPStatusError as e:
            logger.error(
                f"HTTP error while fetching issues with custom JQL: {e.response.status_code} - {e.response.text}"
            )
            raise UserException(f"Could not download custom JQL."
                                f"Received: {e.response.status_code} - {e.response.text}.")

    async def get_board_sprints(self, board_id):
        logger.info(f"Starting to fetch sprints for board: {board_id}")
        url_sprints = urljoin(self.param_agile_url, f'board/{board_id}/sprint')
        offset = 0
        is_complete = False
        all_sprints = []

        while is_complete is False:
            params_sprints = {
                'startAt': offset,
                'maxResults': MAX_RESULTS_AGILE
            }

            log_request_details('GET', url_sprints, params=params_sprints)

            try:
                rsp_sprints = await self.get_raw(url_sprints, params=params_sprints)
                log_response_details(rsp_sprints)

                if rsp_sprints.status_code == 200:
                    _sprt = rsp_sprints.json()
                    all_sprints += _sprt['values']
                    logger.info(f"Fetched {len(_sprt['values'])} sprints. Total: {len(all_sprints)}")
                    is_complete = _sprt['isLast']
                    offset += MAX_RESULTS_AGILE
                    logger.info(f"More sprints available: {not is_complete}")

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400 and \
                        ('The board does not support sprints' in e.response.text or
                         'Tabule nepodporuje sprinty' in e.response.text):
                    logger.info("Board does not support sprints")
                    break
                else:
                    logger.error(f"HTTP error while fetching sprints: {e.response.status_code} - {e.response.text}")
                    raise UserException(f"Could not download sprints for board {board_id}."
                                        f"Received: {e.response.status_code} - {e.response.text}.")

        logger.info(f"Successfully fetched all sprints. Total count: {len(all_sprints)}")
        return all_sprints

    async def get_sprint_issues(self, sprint_id, update_date=None):
        logger.info(f"Starting to fetch issues for sprint: {sprint_id}")
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

            log_request_details('GET', url_issues, params=params_issues)

            try:
                rsp_issues = await self.get_raw(url_issues, params=params_issues)
                log_response_details(rsp_issues)

                if rsp_issues.status_code == 200:
                    _iss = rsp_issues.json()['issues']
                    all_issues += _iss
                    logger.info(f"Fetched {len(_iss)} issues. Total: {len(all_issues)}")

                    if len(_iss) < MAX_RESULTS:
                        is_complete = True
                        logger.info("All issues fetched")
                    else:
                        offset += MAX_RESULTS
                        logger.info(f"More issues available, next offset: {offset}")
                else:
                    logger.error(
                        f"Failed to fetch sprint issues. Status: {rsp_issues.status_code}, "
                        f"Response: {rsp_issues.text}"
                    )
                    raise UserException(f"Could not download issues for sprint {sprint_id}."
                                        f"Received: {rsp_issues.status_code} - {rsp_issues.text}.")

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error while fetching sprint issues: {e.response.status_code} - {e.response.text}")
                raise UserException(f"Could not download issues for sprint {sprint_id}."
                                    f"Received: {e.response.status_code} - {e.response.text}.")

        logger.info(f"Successfully fetched all sprint issues. Total count: {len(all_issues)}")
        return all_issues
