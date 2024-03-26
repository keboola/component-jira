import copy
import logging
import sys
import os
import csv
import re

import dateparser
from keboola.component import ComponentBase, UserException
from configuration import Configuration

from memory_profiler import profile


from client import JiraClient
from result import JiraWriter, FIELDS_R_ISSUES, FIELDS_COMMENTS, PK_COMMENTS

KEY_JQL = "jql"
KEY_TABLE_NAME = "table_name"


class JiraComponent(ComponentBase):

    def __init__(self):

        super().__init__()

        self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        self.cfg: Configuration = Configuration.load_from_dict(self.configuration.parameters)

        _parsed_date = dateparser.parse(self.cfg.since)

        if _parsed_date is None:
            logging.exception(f"Could not recognize date \"{self.cfg.since}\".")
            sys.exit(1)

        else:
            self.param_since_date = _parsed_date.strftime('%Y-%m-%d')
            self.param_since_unix = int(_parsed_date.timestamp() * 1000)

        self.client = JiraClient(organization_id=self.cfg.organization_id,
                                 username=self.cfg.username,
                                 api_token=self.cfg.pswd_token)

    @profile
    def run(self):

        logging.info("Downloading projects.")
        self.get_and_write_projects()

        logging.info("Downloading a list of fields.")
        self.get_and_write_fields()

        logging.info("Downloading users.")
        self.get_and_write_users()

        self.check_issues_param()

        if 'issues' in self.cfg.datasets:
            logging.info("Downloading issues.")
            self.get_and_write_issues()

            if 'comments' in self.cfg.datasets:
                logging.info("Downloading comments")
                self.get_and_write_comments()

        if 'boards_n_sprints' in self.cfg.datasets:
            logging.info("Downloading boards and sprints.")
            self.get_and_write_boards_and_sprints()

        if 'worklogs' in self.cfg.datasets:
            logging.info("Downloading worklogs.")
            self.get_and_write_worklogs()

        if self.cfg.custom_jql:
            for custom_jql in self.cfg.custom_jql:
                if not custom_jql.get(KEY_JQL):
                    logging.exception("Custom JQL error: JQL is empty, must be filled in")
                    sys.exit(1)
                if not custom_jql.get(KEY_TABLE_NAME):
                    logging.exception("Custom JQL error: table name is empty, must be filled in")
                    sys.exit(1)
                logging.info(f"Downloading custom JQL : {custom_jql.get(KEY_JQL)}")
                self.get_and_write_custom_jql(custom_jql.get(KEY_JQL), custom_jql.get(KEY_TABLE_NAME))

    def check_issues_param(self):
        if 'issues' not in self.cfg.datasets:
            if 'issues_changelogs' in self.cfg.datasets:
                logging.warning("Issues need to be enabled in order to download issues changelogs.")
            if 'comments' in self.cfg.datasets:
                logging.warning("Issues need to be enabled in order to download issues comments.")

    @staticmethod
    def merge_text_and_mentions(data):
        content_list = data["body"]["content"]
        merged_string = ""
        for content in content_list:
            if content["type"] == "paragraph":
                for c in content["content"]:
                    if c["type"] == "text":
                        merged_string += c["text"]
                    elif c["type"] == "mention":
                        merged_string += c["attrs"]["text"]
        return merged_string

    @staticmethod
    def get_issue_id_from_url(url):
        pattern = r"/issue/(\d+)"
        match = re.search(pattern, url)
        if match:
            issue_id = match.group(1)
            return issue_id
        else:
            logging.error("Cannot find issue_id in response during fetching comments.")
            sys.exit(1)

    @staticmethod
    def get_issue_ids(table_name, table_cols, issue_id_col_name):
        with open(table_name, 'r') as file:
            r = csv.DictReader(file, fieldnames=table_cols)
            for row in r:
                yield row[issue_id_col_name]

    def parse_comments(self, comments) -> list:
        result = []
        for comment in comments:
            body_text = self.merge_text_and_mentions(comment)
            update_author = comment.get("updateAuthor", {})
            result.append({
                "comment_id": comment["id"],
                "issue_id": self.get_issue_id_from_url(comment["self"]),
                "account_id": comment["author"].get("accountId"),
                "email_address": comment["author"].get("emailAddress"),
                "display_name": comment["author"].get("displayName"),
                "active": comment["author"].get("active"),
                "account_type": comment["author"].get("accountType"),
                "text": body_text,
                "update_author_account_id": update_author.get("accountId"),
                "update_author_display_name": update_author.get("displayName"),
                "update_author_active": update_author.get("active"),
                "update_author_email_address": update_author.get("emailAddress"),
                "update_author_account_type": update_author.get("accountType"),
                "created": comment["created"],
                "updated": comment["updated"]
            })
        return result

    def get_and_write_comments(self):

        load_table_name = os.path.join(self.tables_out_path, 'issues.csv')
        issue_id_col_name = 'id'

        issue_ids = set()
        for issue_id in self.get_issue_ids(load_table_name, FIELDS_R_ISSUES, issue_id_col_name):
            issue_ids.add(issue_id)

        # This is the only table that is being saved in component.py, other tables use JiraWriter. The reason is
        # that I wanted to save both mentions and comments in a single field as sting and this was the easiest way.
        with open(os.path.join(self.tables_out_path, 'comments.csv'), mode="w", newline="") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=FIELDS_COMMENTS, extrasaction="ignore")
            for issue_id in issue_ids:
                issue_comments = self.client.get_comments(issue_id=issue_id)
                if issue_comments:
                    comments = self.parse_comments(issue_comments)
                    writer.writerows(comments)

        table = self.create_out_table_definition(name="comments.csv", columns=FIELDS_COMMENTS, primary_key=PK_COMMENTS,
                                                 incremental=self.cfg.incremental)
        self.write_manifest(table)

    def get_and_write_projects(self):
        
        projects = self.client.get_projects()
        wr = JiraWriter(self.tables_out_path, 'projects', self.cfg.incremental)
        wr.writerows(projects)
        wr.close()

    def get_and_write_users(self):

        users = self.client.get_users()
        wr = JiraWriter(self.tables_out_path, 'users', self.cfg.incremental)
        wr.writerows(users)
        wr.close()

    def get_and_write_fields(self):

        fields = self.client.get_fields()
        wr = JiraWriter(self.tables_out_path, 'fields', self.cfg.incremental)
        wr.writerows(fields)
        wr.close()

    def get_and_write_worklogs(self, batch_size=1000):
        _worklogs_u = [w['worklogId'] for w in self.client.get_updated_worklogs(self.param_since_unix)]
        total_worklogs = len(_worklogs_u)

        wr = JiraWriter(self.tables_out_path, 'worklogs', self.cfg.incremental)

        for i in range(0, total_worklogs, batch_size):
            batch_worklog_ids = _worklogs_u[i:i + batch_size]
            batch_worklogs = self.client.get_worklogs(batch_worklog_ids)

            worklogs_out = []

            for w in batch_worklogs:
                worklogs_out.append({**w, **{'comment': self.parse_description(w.get('comment', '')).strip('\n')}})

            wr.writerows(worklogs_out)

        wr.close()

        worklogs_deleted = self.client.get_deleted_worklogs(self.param_since_unix)
        wr = JiraWriter(self.tables_out_path, 'worklogs-deleted', self.cfg.incremental)
        wr.writerows(worklogs_deleted)
        wr.close()

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

        writer_issues = JiraWriter(self.tables_out_path, 'issues', self.cfg.incremental)

        writer_changelogs = None
        if 'issues_changelogs' in self.cfg.datasets:
            writer_changelogs = JiraWriter(self.tables_out_path, 'issues-changelogs', self.cfg.incremental)

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

                if 'issues_changelogs' in self.cfg.datasets:
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

        writer_issues.close()

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
        if writer_changelogs:
            writer_changelogs.close()

    def get_and_write_boards_and_sprints(self):

        boards = self.client.get_all_boards()
        _boards = [b['id'] for b in boards]
        JiraWriter(self.tables_out_path, 'boards', self.cfg.incremental).writerows(boards)

        sprint_writer = JiraWriter(self.tables_out_path, 'sprints', self.cfg.incremental)
        all_sprints = []
        for board in _boards:
            sprints = self.client.get_board_sprints(board)
            all_sprints += [s['id'] for s in sprints if
                            s.get('completeDate', self.param_since_date) >= self.param_since_date]
            sprints = [{**s, **{'board_id': board}} for s in sprints]
            sprint_writer.writerows(sprints)
        sprint_writer.close()

        issues_writer = JiraWriter(self.tables_out_path, 'sprints-issues', self.cfg.incremental)
        for sprint in set(all_sprints):
            issues = self.client.get_sprint_issues(sprint, update_date=self.param_since_date)
            issues = [{**i, **{'sprint_id': sprint}} for i in issues]
            issues_writer.writerows(issues)
        issues_writer.close()

    def get_and_write_custom_jql(self, jql, table_name):
        offset = 0
        is_complete = False
        writer_issues = JiraWriter(self.tables_out_path, 'issues', self.cfg.incremental, custom_name=table_name)

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
        writer_issues.close()


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
