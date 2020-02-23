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

        super().__init__(mandatory_params=MANDATORY_PARAMS, log_level='DEBUG')

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

    def run(self):

        top = self.tables_out_path
        inc = self.param_incremental

        logging.info("Downloading projects.")
        projects = self.client.get_projects()
        JiraWriter(top, 'projects', inc).writerows(projects)

        logging.info("Downloading a list of fields.")
        fields = self.client.get_fields()
        JiraWriter(top, 'fields', inc).writerows(fields)

        logging.info("Downloading users.")
        users = self.client.get_users()
        JiraWriter(top, 'users', inc).writerows(users)

        if 'issues' in self.param_datasets:

            logging.info("Downloading issues.")
            issues = self.client.get_all_issues(self.param_since_date)
            issues_f = []

            for i in issues:
                _out = {
                    'id': i['id'],
                    'key': i['key']
                }

                _custom = {}

                for key, value in i['fields'].items():
                    if 'customfield_' in key:
                        _custom[key] = value
                    else:
                        _out[key] = value

                _out['custom_fields'] = _custom
                issues_f += [copy.deepcopy(_out)]

            JiraWriter(top, 'issues', inc).writerows(issues_f)

        if 'worklogs' in self.param_datasets:

            logging.info("Downloading worklogs.")
            _worklogs_u = [w['worklogId'] for w in self.client.get_updated_worklogs(self.param_since_unix)]
            worklogs = self.client.get_worklogs(_worklogs_u)
            JiraWriter(top, 'worklogs', inc).writerows(worklogs)

            worklogs_deleted = self.client.get_deleted_worklogs(self.param_since_unix)
            JiraWriter(top, 'worklogs-deleted', inc).writerows(worklogs_deleted)
