import copy
import logging
import os
import csv
import re

import asyncio

import dateparser
from keboola.component import ComponentBase, UserException
from configuration import Configuration

from client import JiraClient
from result import JiraWriter, FIELDS_R_ISSUES, FIELDS_COMMENTS, PK_COMMENTS

KEY_JQL = "jql"
KEY_TABLE_NAME = "table_name"
BUFFER_SIZE = 1_000


class JiraComponent(ComponentBase):
    def __init__(self):
        super().__init__()

        self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        self.cfg: Configuration = Configuration.load_from_dict(self.configuration.parameters)

        self.cfg.incremental = bool(self.cfg.incremental)

        _parsed_date = dateparser.parse(self.cfg.since)

        if _parsed_date is None:
            raise UserException(f'Could not recognize date "{self.cfg.since}".')

        else:
            self.param_since_date = _parsed_date.strftime("%Y-%m-%d")
            self.param_since_unix = int(_parsed_date.timestamp() * 1000)

        self.client = JiraClient(
            organization_id=self.cfg.organization_id, username=self.cfg.username, api_token=self.cfg.pswd_token
        )

    def run(self):
        asyncio.run(self.run_async())

    async def run_async(self):
        tasks = []

        logging.info("Downloading projects.")
        tasks.append(self.get_and_write_projects())

        logging.info("Downloading a list of fields.")
        tasks.append(self.get_and_write_fields())

        logging.info("Downloading users.")
        tasks.append(self.get_and_write_users())

        self.check_issues_param()

        if "issues" in self.cfg.datasets:
            logging.info("Downloading issues.")
            await self.get_and_write_issues()

            if "comments" in self.cfg.datasets:
                logging.info("Downloading comments")
                tasks.append(self.get_and_write_comments())

        if "boards_n_sprints" in self.cfg.datasets:
            logging.info("Downloading boards and sprints.")
            tasks.append(self.get_and_write_boards_and_sprints())

        if "worklogs" in self.cfg.datasets:
            logging.info("Downloading worklogs.")
            tasks.append(self.get_and_write_worklogs())

        if "organizations" in self.cfg.datasets:
            logging.info("Downloading organizations.")
            tasks.append(self.get_and_write_organizations())

        if "servicedesks_and_customers" in self.cfg.datasets:
            logging.info("Downloading servicedesks and customers.")
            tasks.append(self.get_and_write_servicedesks_and_customers())

        if self.cfg.custom_jql:
            for custom_jql in self.cfg.custom_jql:
                if not custom_jql.get(KEY_JQL):
                    raise UserException("Custom JQL error: JQL is empty, must be filled in")
                if not custom_jql.get(KEY_TABLE_NAME):
                    raise UserException("Custom JQL error: table name is empty, must be filled in")
                logging.info(f"Downloading custom JQL : {custom_jql.get(KEY_JQL)}")
                tasks.append(self.get_and_write_custom_jql(custom_jql.get(KEY_JQL), custom_jql.get(KEY_TABLE_NAME)))

        await asyncio.gather(*tasks)

    def check_issues_param(self):
        if "issues" not in self.cfg.datasets:
            if "issues_changelogs" in self.cfg.datasets:
                logging.warning("Issues need to be enabled in order to download issues changelogs.")
            if "comments" in self.cfg.datasets:
                logging.warning("Issues need to be enabled in order to download issues comments.")

    @staticmethod
    def merge_text_and_mentions(data):
        merged_string = ""

        content_list = data.get("body", {}).get("content", [])

        for content in content_list:
            if content.get("type") == "paragraph":
                for c in content.get("content", []):
                    if c.get("type") == "text":
                        merged_string += c.get("text", "")
                    elif c.get("type") == "mention":
                        merged_string += c.get("attrs", {}).get("text", "")

        return merged_string

    @staticmethod
    def get_issue_id_from_url(url):
        pattern = r"/issue/(\d+)"
        match = re.search(pattern, url)
        if match:
            issue_id = match.group(1)
            return issue_id
        else:
            raise UserException("Cannot find issue_id in response during fetching comments.")

    @staticmethod
    def get_issue_ids(table_name, table_cols, issue_id_col_name):
        with open(table_name, "r") as file:
            r = csv.DictReader(file, fieldnames=table_cols)
            for row in r:
                yield row[issue_id_col_name]

    def parse_comments(self, comments) -> list:
        result = []
        for comment in comments:
            body_text = self.merge_text_and_mentions(comment)
            update_author = comment.get("updateAuthor", {})
            # Check if the comment has properties and parse public visibility if present
            public_visibility = None
            if comment.get("properties"):
                for prop in comment["properties"]:
                    if prop.get("key") == "sd.public.comment":
                        public_visibility = prop.get("value", {}).get("internal")
                        break

            result.append(
                {
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
                    "updated": comment["updated"],
                    "public_visibility": public_visibility,
                }
            )
        return result

    async def get_and_write_comments(self):
        load_table_name = os.path.join(self.tables_out_path, "issues.csv")
        issue_id_col_name = "id"

        issue_ids = set()
        for issue_id in self.get_issue_ids(load_table_name, FIELDS_R_ISSUES, issue_id_col_name):
            issue_ids.add(issue_id)

        # This is the only table that is being saved in component.py, other tables use JiraWriter. The reason is
        # that I wanted to save both mentions and comments in a single field as sting and this was the easiest way.
        with open(os.path.join(self.tables_out_path, "comments.csv"), mode="w", newline="") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=FIELDS_COMMENTS, extrasaction="ignore")
            for issue_id in issue_ids:
                issue_comments = await self.client.get_comments(issue_id=issue_id)
                if issue_comments:
                    comments = self.parse_comments(issue_comments)
                    writer.writerows(comments)

        table = self.create_out_table_definition(
            name="comments.csv", columns=FIELDS_COMMENTS, primary_key=PK_COMMENTS, incremental=self.cfg.incremental
        )
        self.write_manifest(table)

    async def get_and_write_projects(self):
        projects = await self.client.get_projects()
        wr = JiraWriter(self.tables_out_path, "projects", self.cfg.incremental)
        wr.writerows(projects)
        wr.close()

    async def get_and_write_users(self):
        wr = JiraWriter(self.tables_out_path, "users", self.cfg.incremental)
        buffer = []

        async for user in self.client.get_users():
            buffer.append(user)
            if len(buffer) >= BUFFER_SIZE:
                wr.writerows(buffer)
                buffer = []

        if buffer:  # Write any remaining items
            wr.writerows(buffer)
        wr.close()

    async def get_and_write_fields(self):
        fields = await self.client.get_fields()
        wr = JiraWriter(self.tables_out_path, "fields", self.cfg.incremental)
        wr.writerows(fields)
        wr.close()

    async def get_and_write_organizations(self):
        wr = JiraWriter(self.tables_out_path, "organizations", self.cfg.incremental)
        buffer = []

        async for organization in self.client.get_organizations():
            buffer.append(organization)
            if len(buffer) >= BUFFER_SIZE:
                wr.writerows(buffer)
                buffer = []

        if buffer:
            wr.writerows(buffer)
        wr.close()

    async def get_and_write_servicedesks_and_customers(self):
        # Create a list to collect servicedesks for later use
        servicedesks = []
        wr = JiraWriter(self.tables_out_path, "servicedesks", self.cfg.incremental)
        buffer = []

        async for desk in self.client.get_servicedesks():
            servicedesks.append(desk)
            buffer.append(desk)
            if len(buffer) >= BUFFER_SIZE:
                wr.writerows(buffer)
                buffer = []

        if buffer:
            wr.writerows(buffer)
        wr.close()

        for organization in servicedesks:
            wr = JiraWriter(self.tables_out_path, "servicedesk-customers", self.cfg.incremental)
            buffer = []

            async for customer in self.client.get_servicedesk_customers(organization["id"]):
                buffer.append(customer)
                if len(buffer) >= BUFFER_SIZE:
                    wr.writerows(buffer)
                    buffer = []

            if buffer:
                wr.writerows(buffer)
            wr.close()

    async def get_and_write_worklogs(self, batch_size=1000):
        worklog_ids = []
        async for worklog in self.client.get_updated_worklogs(self.param_since_unix):
            worklog_ids.append(worklog["worklogId"])
        total_worklogs = len(worklog_ids)

        wr = JiraWriter(self.tables_out_path, "worklogs", self.cfg.incremental)
        buffer = []

        for i in range(0, total_worklogs, batch_size):
            batch_worklog_ids = worklog_ids[i:i + batch_size]
            async for worklog in self.client.get_worklogs(batch_worklog_ids):
                worklog_out = {**worklog, **{"comment": self.parse_description(worklog.get("comment", "")).strip("\n")}}
                buffer.append(worklog_out)
                if len(buffer) >= BUFFER_SIZE:
                    wr.writerows(buffer)
                    buffer = []

        if buffer:
            wr.writerows(buffer)
        wr.close()

        wr = JiraWriter(self.tables_out_path, "worklogs-deleted", self.cfg.incremental)
        buffer = []

        async for worklog in self.client.get_deleted_worklogs(self.param_since_unix):
            buffer.append(worklog)
            if len(buffer) >= BUFFER_SIZE:
                wr.writerows(buffer)
                buffer = []

        if buffer:
            wr.writerows(buffer)
        wr.close()

    def parse_description(self, description) -> str:
        if description is None:
            return ""
        text = ""

        if "content" in description:
            text += self.parse_description(description["content"])

            if description["type"] == "paragraph":
                text += "\n"

        elif isinstance(description, dict):
            if description["type"] == "inlineCard" or description["type"] == "blockCard":
                text += description.get("attrs", {}).get("url", "")
            elif description["type"] == "text":
                text += description.get("text", "")
            elif description["type"] == "hardBreak":
                text += "\n"
            elif description["type"] == "mention":
                text += description.get("attrs", {}).get("text", "")
            elif description["type"] == "status":
                text += description.get("attrs", {}).get("text", "")
            elif description["type"] in ("codeBlock", "media"):
                pass
            else:
                text += ""

        elif isinstance(description, list):
            for list_item in description:
                text += self.parse_description(list_item)

        else:
            pass

        return text

    async def get_and_write_issues(self):
        page_token = None
        is_complete = False
        download_further_changelogs = []

        writer_issues = JiraWriter(self.tables_out_path, "issues", self.cfg.incremental)

        writer_changelogs = None
        if "issues_changelogs" in self.cfg.datasets:
            writer_changelogs = JiraWriter(self.tables_out_path, "issues-changelogs", self.cfg.incremental)

        while is_complete is False:
            issues, is_complete, page_token = await self.client.get_issues(
                self.param_since_date, page_token=page_token, issue_jql_filter=self.cfg.issue_jql_filter
            )
            issues_f = []

            for issue in issues:
                _out = {"id": issue["id"], "key": issue["key"]}

                _custom = {}

                for key, value in issue["fields"].items():
                    if "customfield_" in key:
                        _custom[key] = value
                    elif key == "description":
                        _out["description"] = (
                            self.parse_description(issue["fields"]["description"]).strip("\n").replace("\0", "\\0")
                        )
                    else:
                        _out[key] = value

                _out["custom_fields"] = _custom
                issues_f += [copy.deepcopy(_out)]

                if "issues_changelogs" in self.cfg.datasets:
                    _changelog = issue["changelog"]

                    if _changelog["maxResults"] < _changelog["total"]:
                        download_further_changelogs += [(issue["id"], issue["key"])]

                    else:
                        _changelogs = [
                            {**x, **{"issue_id": issue["id"], "issue_key": issue["key"]}}
                            for x in _changelog["histories"]
                        ]

                        await self._process_changelogs(_changelogs, writer_changelogs)

            writer_issues.writerows(issues_f)

        writer_issues.close()

        await self._get_changelogs(download_further_changelogs, writer_changelogs)

        if writer_changelogs:
            writer_changelogs.close()

    async def _get_changelogs(self, download_further_changelogs, writer_changelogs):
        issue_keys = [issue[1] for issue in download_further_changelogs]
        async for changelog in self.client.get_bulk_changelogs(issue_keys):
            # batch changelogs are returned in a dictionary with the issueId and without the issueKey
            issue_id = changelog["issueId"]
            issue_key = next(issue[1] for issue in download_further_changelogs if issue[0] == issue_id)
            await self._process_changelogs(changelog["changeHistories"], writer_changelogs, issue_id, issue_key)

    @staticmethod
    async def _process_changelogs(_changelogs, writer_changelogs, issue_id=None, issue_key=None):
        all_changelogs = []
        for changelog in _changelogs:
            _out = dict()
            _out["total_changed_items"] = len(changelog["items"])
            _out["id"] = changelog["id"]
            _out["issue_id"] = issue_id or changelog["issue_id"]
            _out["issue_key"] = issue_key or changelog["issue_key"]
            _out["author_accountId"] = changelog.get("author", {}).get("accountId", "")
            _out["author_emailAddress"] = changelog.get("author", {}).get("emailAddress", "")
            _out["created"] = changelog["created"]

            for idx, item in enumerate(changelog["items"], start=1):
                item["changed_item_order"] = idx
                all_changelogs += [{**_out, **item}]

        writer_changelogs.writerows(all_changelogs)

    async def get_and_write_boards_and_sprints(self):
        # Collect boards
        boards = []
        async for board in self.client.get_all_boards():
            boards.append(board)

        _boards = [b["id"] for b in boards]
        JiraWriter(self.tables_out_path, "boards", self.cfg.incremental).writerows(boards)

        sprint_writer = JiraWriter(self.tables_out_path, "sprints", self.cfg.incremental)
        all_sprints = []
        buffer = []

        for board in _boards:
            async for sprint in self.client.get_board_sprints(board):
                if sprint.get("completeDate", self.param_since_date) >= self.param_since_date:
                    all_sprints.append(sprint["id"])
                sprint_with_board = {**sprint, **{"board_id": board}}
                buffer.append(sprint_with_board)
                if len(buffer) >= BUFFER_SIZE:
                    sprint_writer.writerows(buffer)
                    buffer = []

        if buffer:
            sprint_writer.writerows(buffer)
        sprint_writer.close()

        issues_writer = JiraWriter(self.tables_out_path, "sprints-issues", self.cfg.incremental)
        buffer = []

        for sprint in set(all_sprints):
            async for issue in self.client.get_sprint_issues(sprint, update_date=self.param_since_date):
                issue_with_sprint = {**issue, **{"sprint_id": sprint}}
                buffer.append(issue_with_sprint)
                if len(buffer) >= BUFFER_SIZE:
                    issues_writer.writerows(buffer)
                    buffer = []

        if buffer:
            issues_writer.writerows(buffer)
        issues_writer.close()

    async def get_and_write_custom_jql(self, jql, table_name):
        offset = 0
        is_complete = False
        writer_issues = JiraWriter(self.tables_out_path, "issues", self.cfg.incremental, custom_name=table_name)

        while is_complete is False:
            issues, is_complete, offset = await self.client.get_custom_jql(jql, offset=offset)
            issues_f = []
            for issue in issues:
                _out = {"id": issue["id"], "key": issue["key"]}
                _custom = {}
                for key, value in issue["fields"].items():
                    if "customfield_" in key:
                        _custom[key] = value
                    elif key == "description":
                        _out["description"] = self.parse_description(issue["fields"]["description"]).strip("\n")
                    else:
                        _out[key] = value

                _out["custom_fields"] = _custom
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
