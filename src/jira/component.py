import copy
import dateparser
import logging
import sys
from kbc.env_handler import KBCEnvHandler
from jira.client import JiraClient
from jira.result import JiraWriter


KEY_USERNAME = 'username'
KEY_TOKEN = '#token'
KEY_ORGANIZATION = 'organization_id'
KEY_SINCE = 'since'
KEY_INCREMENTAL = 'incremental'
KEY_DATASETS = 'datasets'

MANDATORY_PARAMS = [KEY_USERNAME, KEY_TOKEN, KEY_ORGANIZATION, KEY_SINCE, KEY_DATASETS]


class JiraComponent(KBCEnvHandler):

    def __init__(self):

        super().__init__(mandatory_params=MANDATORY_PARAMS, log_level='INFO')

        if self.cfg_params.get('debug', False) is True:
            logger = logging.getLogger()
            logger.setLevel(level='DEBUG')

        try:
            self.validate_config(mandatory_params=MANDATORY_PARAMS)

        except ValueError as e:
            logging.exception(e)
            sys.exit(1)

        self.param_username = self.cfg_params[KEY_USERNAME]
        self.param_token = self.cfg_params[KEY_TOKEN]
        self.param_organization = self.cfg_params[KEY_ORGANIZATION]
        self.param_since_raw = self.cfg_params[KEY_SINCE]
        self.param_incremental = bool(self.cfg_params.get(KEY_INCREMENTAL, 1))
        self.param_datasets = self.cfg_params[KEY_DATASETS]

        _parsed_date = dateparser.parse(self.param_since_raw)

        if _parsed_date is None:
            logging.exception(f"Could not recognize date \"{self.param_since_raw}\".")
            sys.exit(1)

        else:
            self.param_since_date = _parsed_date.strftime('%Y-%m-%d')
            self.param_since_unix = int(_parsed_date.timestamp() * 1000)

        self.client = JiraClient(organization_id=self.param_organization,
                                 username=self.param_username,
                                 api_token=self.param_token)

    def get_and_write_projects(self):

        projects = self.client.get_projects()
        JiraWriter(self.tables_out_path, 'projects', self.param_incremental).writerows(projects)

    def get_and_write_users(self):
        users = self.client.get_users()
        JiraWriter(self.tables_out_path, 'users', self.param_incremental).writerows(users)

    def get_and_write_fields(self):
        fields = self.client.get_fields()
        JiraWriter(self.tables_out_path, 'fields', self.param_incremental).writerows(fields)

    def get_and_write_worklogs(self):

        _worklogs_u = [w['worklogId'] for w in self.client.get_updated_worklogs(self.param_since_unix)]
        worklogs = self.client.get_worklogs(_worklogs_u)
        JiraWriter(self.tables_out_path, 'worklogs', self.param_incremental).writerows(worklogs)

        worklogs_deleted = self.client.get_deleted_worklogs(self.param_since_unix)
        JiraWriter(self.tables_out_path, 'worklogs-deleted', self.param_incremental).writerows(worklogs_deleted)

    def get_and_write_issues(self):
        issues = self.client.get_all_issues(self.param_since_date)
        issues_f = []

        writer_issues = JiraWriter(self.tables_out_path, 'issues', self.param_incremental)

        if 'issues_changelogs' in self.param_datasets:
            writer_changelogs = JiraWriter(self.tables_out_path, 'issues-changelogs', self.param_incremental)
            _changelogs = []
            download_further_changelogs = []

        for issue in issues:

            _out = {
                'id': issue['id'],
                'key': issue['key']
            }

            _custom = {}

            for key, value in issue['fields'].items():
                if 'customfield_' in key:
                    _custom[key] = value
                else:
                    _out[key] = value

            _out['custom_fields'] = _custom
            issues_f += [copy.deepcopy(_out)]

            if 'issues_changelogs' in self.param_datasets:
                _changelog = issue['changelog']

                if _changelog['maxResults'] < _changelog['total']:
                    download_further_changelogs += [issue['key']]

                else:
                    _changelogs += [{**x, **{'issue_key': issue['key']}} for x in _changelog['histories']]

        writer_issues.writerows(issues_f)

        if 'issues_changelogs' in self.param_datasets:
            all_changelogs = []
            for issue_key in download_further_changelogs:
                _changelogs_issue = self.client.get_changelogs(issue_key)
                _changelogs += [{**c, **{'issue_key': issue_key}} for c in _changelogs_issue]

            for changelog in _changelogs:
                _out = dict()
                _out['total_changed_items'] = len(changelog['items'])
                _out['id'] = changelog['id']
                _out['issue_key'] = changelog['issue_key']
                _out['author_accountId'] = changelog['author']['accountId']
                _out['author_emailAddress'] = changelog['author'].get('emailAddress', '')
                _out['created'] = changelog['created']

                for idx, item in enumerate(changelog['items'], start=1):
                    item['changed_item_order'] = idx
                    all_changelogs += [{**_out, **item}]

            writer_changelogs.writerows(all_changelogs)

    def run(self):

        logging.info("Downloading projects.")
        self.get_and_write_projects()

        logging.info("Downloading a list of fields.")
        self.get_and_write_fields()

        logging.info("Downloading users.")
        self.get_and_write_users()

        if 'issues' in self.param_datasets:
            logging.info("Downloading issues.")
            self.get_and_write_issues()

        if 'worklogs' in self.param_datasets:
            logging.info("Downloading worklogs.")
            self.get_and_write_worklogs()
