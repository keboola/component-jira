import csv
import json
import os

FIELDS_ISSUES = ['id', 'key', 'statuscategorychangedate', 'issuetype_id', 'issuetype_name', 'timespent', 'project_key',
                 'fixVersions', 'aggregatetimespent', 'resolution', 'resolutiondate', 'resolution_id',
                 'resolution_name', 'resolution_description', 'workratio', 'lastViewed',
                 'created', 'priority_id', 'priority_name', 'labels', 'timeestimate', 'aggregatetimeoriginalestimate',
                 'assignee_accountId', 'assignee_displayName', 'updated', 'status_id', 'status_name', 'components',
                 'timeoriginalestimate', 'security', 'aggregatetimeestimate', 'summary', 'description',
                 'creator_accountId', 'creator_emailAddress', 'creator_displayName', 'parent_id', 'subtasks',
                 'reporter_accountId', 'reporter_displayName', 'aggregateprogress_progress', 'aggregateprogress_total',
                 'environment', 'duedate', 'progress_progress', 'progress_total', 'custom_fields', 'issuelinks']
FIELDS_R_ISSUES = ['id', 'key', 'status_category_change_date', 'issue_type_id', 'issue_type_name', 'time_spent',
                   'project_key', 'fix_versions', 'aggregate_time_spent', 'resolution', 'resolution_date',
                   'resolution_id', 'resolution_name', 'resolution_description', 'work_ratio',
                   'last_viewed', 'created', 'priority_id', 'priority_name', 'labels', 'time_estimate',
                   'aggregate_time_original_estimate', 'assignee_account_id', 'assignee_display_name', 'updated',
                   'status_id', 'status_name', 'components', 'time_original_estimate', 'security',
                   'aggregate_time_estimate', 'summary', 'description', 'creator_account_id', 'creator_email_address',
                   'creator_display_name', 'parent_id', 'subtasks', 'reporter_account_id', 'reporter_display_name',
                   'aggregate_progress', 'aggregate_progress_total', 'environment', 'due_date', 'progress',
                   'progress_total', 'custom_fields', 'issuelinks']
PK_ISSUES = ['id']
JSON_ISSUES = ['fixVersions', 'components', 'subtasks', 'custom_fields', 'issuelinks']

FIELDS_USERS = ['accountId', 'displayName', 'active', 'accountType', 'emailAddress', 'locale']
FIELDS_R_USERS = ['account_id', 'display_name', 'active', 'account_type', 'email_address', 'locale']
PK_USERS = ['account_id']
JSON_USERS = []

FIELDS_FIELDS = ['id', 'key', 'name', 'custom']
FIELDS_R_FIELDS = FIELDS_FIELDS
PK_FIELDS = ['id', 'key']
JSON_FIELDS = []

FIELDS_PROJECTS = ['id', 'key', 'name', 'description', 'projectCategory_id', 'projectCategory_name',
                   'projectCategory_description', 'projectTypeKey', 'isPrivate', 'archived', 'archivedBy_accountId',
                   'archivedBy_displayName']
FIELDS_R_PROJECTS = ['id', 'key', 'name', 'description', 'project_category_id', 'project_category_name',
                     'project_category_description', 'project_type_key', 'is_private', 'archived',
                     'archived_by_account_id', 'archived_by_display_name']
PK_PROJECTS = ['id', 'key']
JSON_PROJECTS = []

FIELDS_WORKLOGS_DELETED = ['worklogId', 'updatedTime']
FIELDS_R_WORKLOGS_DELETED = ['worklog_id', 'updated_time']
PK_WORKLOGS_DELETED = ['worklog_id']
JSON_WORKLOGS_DELETED = []

FIELDS_WORKLOGS = ['id', 'issueId', 'author_accountId', 'author_displayName', 'updateAuthor_accountId',
                   'updateAuthor_displayName', 'created', 'updated', 'started', 'timeSpent', 'timeSpentSeconds',
                   'comment']
FIELDS_R_WORKLOGS = ['id', 'issue_id', 'author_account_id', 'author_display_name', 'update_author_account_id',
                     'update_author_display_name', 'created', 'updated', 'started', 'time_spent', 'time_spent_seconds',
                     'comment']
PK_WORKLOGS = ['id']
JSON_WORKLOGS = []

FIELDS_ISSUES_CHANGELOGS = ['id', 'issue_id', 'issue_key', 'author_accountId', 'author_emailAddress', 'created',
                            'total_changed_items', 'changed_item_order', 'field', 'fieldtype', 'from', 'fromString',
                            'to', 'toString']
FIELDS_R_ISSUES_CHANGELOGS = ['id', 'issue_id', 'issue_key', 'author_account_id', 'author_email_address', 'created',
                              'total_changed_items', 'changed_item_order', 'field', 'field_type', 'from', 'from_string',
                              'to', 'to_string']
PK_ISSUES_CHANGELOGS = ['id', 'issue_key', 'field']
JSON_ISSUES_CHANGELOGS = []

FIELDS_BOARDS = ['id', 'self', 'name', 'type', 'location_projectId']
FIELDS_R_BOARDS = ['id', 'url', 'name', 'type', 'project_id']
PK_BOARDS = ['id']
JSON_BOARDS = []

FIELDS_SPRINTS = ['id', 'board_id', 'self', 'state', 'name', 'startDate', 'endDate',
                  'completeDate', 'originBoardId', 'goal']
FIELDS_R_SPRINTS = ['id', 'board_id', 'url', 'state', 'name', 'start_date', 'end_date',
                    'complete_date', 'origin_board_id', 'goal']
PK_SPRINTS = ['id']
JSON_SPRINTS = []

FIELDS_SPRINTS_ISSUES = ['id', 'sprint_id', 'key']
FIELDS_R_SPRINTS_ISSUES = ['issue_id', 'sprint_id', 'issue_key']
PK_SPRINTS_ISSUES = ['issue_id', 'sprint_id']
JSON_SPRINTS_ISSUES = []

FIELDS_COMMENTS = ["comment_id", "issue_id", "account_id", "email_address", "display_name", "active", "account_type",
                   "text", "update_author_account_id", "update_author_display_name", "update_author_active",
                   "update_author_email_address", "update_author_account_type", "created", "updated"]
FIELDS_R_COMMENTS = ["comment_id", "issue_id", "account_id", "email_address", "display_name", "active", "account_type",
                     "text", "update_author_account_id", "update_author_display_name", "update_author_active",
                     "update_author_email_address", "update_author_account_type", "created", "updated"]
PK_COMMENTS = ["comment_id"]
JSON_COMMENTS = []


class JiraWriter:

    def __init__(self, tableOutPath, tableName, incremental, custom_name=""):

        self.paramFields = eval(f'FIELDS_{tableName.upper().replace("-", "_")}')
        self.paramJsonFields = eval(f'JSON_{tableName.upper().replace("-", "_")}')
        self.paramPrimaryKey = eval(f'PK_{tableName.upper().replace("-", "_")}')
        self.paramFieldsRenamed = eval(f'FIELDS_R_{tableName.upper().replace("-", "_")}')
        self.paramPath = tableOutPath
        self.paramTableName = tableName
        self.paramTable = tableName + '.csv'
        if custom_name:
            self.paramTableName = custom_name
            self.paramTable = custom_name + '.csv'
        self.paramTablePath = os.path.join(self.paramPath, self.paramTable)
        self.paramIncremental = incremental

        self.createManifest()
        self.createWriter()

    def createManifest(self):

        template = {
            'incremental': self.paramIncremental,
            'primary_key': self.paramPrimaryKey,
            'columns': self.paramFieldsRenamed
        }

        path = self.paramTablePath + '.manifest'

        with open(path, 'w') as manifest:
            json.dump(template, manifest)

    def createWriter(self):
        with open(self.paramTablePath, 'w', newline='') as csvfile:
            self.writer = csv.DictWriter(csvfile, fieldnames=self.paramFields,
                                         restval='', extrasaction='ignore', quotechar='\"', quoting=csv.QUOTE_ALL)

    def writerows(self, listToWrite, parentDict=None):

        for row in listToWrite:

            _cust = row.get('custom_fields', None)

            row_f = self.flatten_json(x=row)
            _dictToWrite = {}

            for key, value in row_f.items():

                if key in self.paramJsonFields:
                    _dictToWrite[key] = json.dumps(value)

                elif key in self.paramFields:
                    _dictToWrite[key] = value

                else:
                    continue

            if parentDict is not None:
                _dictToWrite = {**_dictToWrite, **parentDict}

            if _cust is not None:
                _dictToWrite = {**_dictToWrite, **{'custom_fields': json.dumps(_cust)}}

            self.writer.writerow(_dictToWrite)

    def flatten_json(self, x, out=None, name=''):
        if out is None:
            out = dict()

        if type(x) is dict:
            for a in x:
                self.flatten_json(x[a], out, name + a + '_')
        else:
            out[name[:-1]] = x

        return out
