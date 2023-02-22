import copy
import logging
import sys
import os
import csv

import dateparser
from keboola.component import ComponentBase, UserException

from client import JiraClient
from result import JiraWriter, FIELDS_R_ISSUES, FIELDS_R_ISSUES_CHANGELOGS

KEY_USERNAME = 'username'
KEY_TOKEN = '#token'
KEY_ORGANIZATION = 'organization_id'
KEY_SINCE = 'since'
KEY_INCREMENTAL = 'incremental'
KEY_DATASETS = 'datasets'
KEY_CUSTOM_JQL = "custom_jql"
KEY_JQL = "jql"
KEY_TABLE_NAME = "table_name"

MANDATORY_PARAMS = [KEY_USERNAME, KEY_TOKEN, KEY_ORGANIZATION, KEY_SINCE, KEY_DATASETS]


class JiraComponent(ComponentBase):

    def __init__(self):

        super().__init__()

        try:
            self.validate_configuration_parameters(mandatory_params=MANDATORY_PARAMS)

        except ValueError as e:
            logging.exception(e)
            sys.exit(1)

        self.param_username = self.configuration.parameters[KEY_USERNAME]
        self.param_token = self.configuration.parameters[KEY_TOKEN]
        self.param_organization = self.configuration.parameters[KEY_ORGANIZATION]
        self.param_since_raw = self.configuration.parameters[KEY_SINCE]
        self.param_incremental = bool(self.configuration.parameters.get(KEY_INCREMENTAL, 1))
        self.param_datasets = self.configuration.parameters[KEY_DATASETS]
        self.custom_jqls = self.configuration.parameters.get(KEY_CUSTOM_JQL)

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

        logging.info("Downloading projects.")
        self.get_and_write_projects()

        logging.info("Downloading a list of fields.")
        self.get_and_write_fields()

        logging.info("Downloading users.")
        self.get_and_write_users()

        if 'issues' not in self.param_datasets and 'issues_changelogs' in self.param_datasets:
            logging.warning("Issues need to be enabled in order to download issues changelogs.")

        if 'issues' in self.param_datasets:
            logging.info("Downloading issues.")
            self.get_and_write_issues()

            if 'comments' in self.param_datasets:
                logging.info("Downloading comments")
                self.get_and_write_comments()

        if 'boards_n_sprints' in self.param_datasets:
            logging.info("Downloading boards and sprints.")
            self.get_and_write_boards_and_sprints()

        if 'worklogs' in self.param_datasets:
            logging.info("Downloading worklogs.")
            self.get_and_write_worklogs()

        if self.custom_jqls:
            for custom_jql in self.custom_jqls:
                if not custom_jql.get(KEY_JQL):
                    logging.exception("Custom JQL error: JQL is empty, must be filled in")
                    sys.exit(1)
                if not custom_jql.get(KEY_TABLE_NAME):
                    logging.exception("Custom JQL error: table name is empty, must be filled in")
                    sys.exit(1)
                logging.info(f"Downloading custom JQL : {custom_jql.get(KEY_JQL)}")
                self.get_and_write_custom_jql(custom_jql.get(KEY_JQL), custom_jql.get(KEY_TABLE_NAME))

    def get_and_write_comments(self):

        if 'issues_changelogs' in self.param_datasets:
            load_table_name = os.path.join(self.tables_out_path, 'issues-changelogs.csv')
            load_table_cols = FIELDS_R_ISSUES_CHANGELOGS
        else:
            load_table_name = os.path.join(self.tables_out_path, 'issues.csv')
            load_table_cols = FIELDS_R_ISSUES

        issue_ids = set()

        with open(load_table_name, 'r') as file:
            r = csv.DictReader(file, fieldnames=load_table_cols)
            for row in r:
                issue_ids.add(row['issue_id'])

        comments = self.client.get_comments(issue_ids=issue_ids)
        d = dict()
        for issue_comments in comments:
            for comment in issue_comments:

                body_content = comment["body"]["content"]
                text_list = []
                for paragraph in body_content:
                    if paragraph["type"] == "paragraph":
                        for text_dict in paragraph["content"]:
                            if text_dict["type"] == "text":
                                text_list.append(text_dict["text"])
                body_text = "".join(text_list)

                d[comment["id"]] = {
                    "account_id": comment["author"]["accountId"],
                    "email_address": comment["author"]["emailAddress"],
                    "display_name": comment["author"]["displayName"],
                    "active": comment["author"]["active"],
                    "account_type": comment["author"]["accountType"],
                    "text": body_text,
                    "update_author_account_id": comment["updateAuthor"]["AccountId"],
                    "update_author_display_name": comment["updateAuthor"]["displayName"],
                    "update_author_active": comment["updateAuthor"]["active"],
                    "update_author_account_type": comment["updateAuthor"]["accountType"],
                    "created": comment["created"],
                    "updated": comment["updated"]
                }
        print(d)

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

        worklogs_out = []

        for w in worklogs:
            worklogs_out += [{**w, **{'comment': self.parse_description(w.get('comment', '')).strip('\n')}}]

        JiraWriter(self.tables_out_path, 'worklogs', self.param_incremental).writerows(worklogs_out)

        worklogs_deleted = self.client.get_deleted_worklogs(self.param_since_unix)
        JiraWriter(self.tables_out_path, 'worklogs-deleted', self.param_incremental).writerows(worklogs_deleted)

    def parse_description(self, description) -> str:
        if description is None:
            return ''
        text = ''

        if 'content' in description:

            text += self.parse_description(description['content'])

            if description['type'] == 'paragraph':
                text += '\n'

        elif isinstance(description, dict):

            if description['type'] == 'inlineCard' or description['type'] == 'blockCard':
                text += description.get('attrs', {}).get('url', '')
            elif description['type'] == 'text':
                text += description.get('text', '')
            elif description['type'] == 'hardBreak':
                text += '\n'
            elif description['type'] == 'mention':
                text += description.get('attrs', {}).get('text', '')
            elif description['type'] == 'status':
                text += description.get('attrs', {}).get('text', '')
            elif description['type'] in ('codeBlock', 'media'):
                pass
            else:
                text += ''

        elif isinstance(description, list):

            for list_item in description:
                text += self.parse_description(list_item)

        else:
            pass

        return text

    def get_and_write_issues(self):
        offset = 0
        is_complete = False
        download_further_changelogs = []

        writer_issues = JiraWriter(self.tables_out_path, 'issues', self.param_incremental)

        if 'issues_changelogs' in self.param_datasets:
            writer_changelogs = JiraWriter(self.tables_out_path, 'issues-changelogs', self.param_incremental)

        while is_complete is False:

            issues, is_complete, offset = self.client.get_issues(self.param_since_date, offset=offset)
            issues_f = []

            for issue in issues:

                _out = {
                    'id': issue['id'],
                    'key': issue['key']
                }

                _custom = {}

                for key, value in issue['fields'].items():
                    if 'customfield_' in key:
                        _custom[key] = value
                    elif key == 'description':
                        _out['description'] = self.parse_description(issue['fields']['description']).strip('\n')
                    else:
                        _out[key] = value

                _out['custom_fields'] = _custom
                issues_f += [copy.deepcopy(_out)]

                if 'issues_changelogs' in self.param_datasets:
                    _changelog = issue['changelog']

                    if _changelog['maxResults'] < _changelog['total']:
                        download_further_changelogs += [(issue['id'], issue['key'])]

                    else:
                        all_changelogs = []
                        _changelogs = [{**x, **{'issue_id': issue['id'], 'issue_key': issue['key']}}
                                       for x in _changelog['histories']]

                        for changelog in _changelogs:
                            _out = dict()
                            _out['total_changed_items'] = len(changelog['items'])
                            _out['id'] = changelog['id']
                            _out['issue_id'] = changelog['issue_id']
                            _out['issue_key'] = changelog['issue_key']
                            _out['author_accountId'] = changelog.get('author', {}).get('accountId', '')
                            _out['author_emailAddress'] = changelog.get('author', {}).get('emailAddress', '')
                            _out['created'] = changelog['created']

                            for idx, item in enumerate(changelog['items'], start=1):
                                item['changed_item_order'] = idx
                                all_changelogs += [{**_out, **item}]

                        writer_changelogs.writerows(all_changelogs)

            writer_issues.writerows(issues_f)

        for issue in download_further_changelogs:
            all_changelogs = []
            _changelogs = [{**c, **{'issue_id': issue[0], 'issue_key': issue[1]}}
                           for c in self.client.get_changelogs(issue[1])]

            for changelog in _changelogs:
                _out = dict()
                _out['total_changed_items'] = len(changelog['items'])
                _out['id'] = changelog['id']
                _out['issue_id'] = changelog['issue_id']
                _out['issue_key'] = changelog['issue_key']
                _out['author_accountId'] = changelog.get('author', {}).get('accountId', '')
                _out['author_emailAddress'] = changelog.get('author', {}).get('emailAddress', '')
                _out['created'] = changelog['created']

                for idx, item in enumerate(changelog['items'], start=1):
                    item['changed_item_order'] = idx
                    all_changelogs += [{**_out, **item}]

            writer_changelogs.writerows(all_changelogs)

    def get_and_write_boards_and_sprints(self):

        boards = self.client.get_all_boards()
        _boards = [b['id'] for b in boards]
        JiraWriter(self.tables_out_path, 'boards', self.param_incremental).writerows(boards)

        sprint_writer = JiraWriter(self.tables_out_path, 'sprints', self.param_incremental)
        all_sprints = []
        for board in _boards:
            sprints = self.client.get_board_sprints(board)
            all_sprints += [s['id'] for s in sprints if
                            s.get('completeDate', self.param_since_date) >= self.param_since_date]
            sprints = [{**s, **{'board_id': board}} for s in sprints]
            sprint_writer.writerows(sprints)

        issues_writer = JiraWriter(self.tables_out_path, 'sprints-issues', self.param_incremental)
        for sprint in set(all_sprints):
            issues = self.client.get_sprint_issues(sprint, update_date=self.param_since_date)
            issues = [{**i, **{'sprint_id': sprint}} for i in issues]
            issues_writer.writerows(issues)

    def get_and_write_custom_jql(self, jql, table_name):
        offset = 0
        is_complete = False
        writer_issues = JiraWriter(self.tables_out_path, 'issues', self.param_incremental, custom_name=table_name)

        while is_complete is False:
            issues, is_complete, offset = self.client.get_custom_jql(jql, offset=offset)
            issues_f = []
            for issue in issues:
                _out = {
                    'id': issue['id'],
                    'key': issue['key']
                }
                _custom = {}
                for key, value in issue['fields'].items():
                    if 'customfield_' in key:
                        _custom[key] = value
                    elif key == 'description':
                        _out['description'] = self.parse_description(issue['fields']['description']).strip('\n')
                    else:
                        _out[key] = value

                _out['custom_fields'] = _custom
                issues_f += [copy.deepcopy(_out)]
            writer_issues.writerows(issues_f)


if __name__ == "__main__":
    try:
        comp = JiraComponent()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
