import os
import csv
import json


FIELDS_ISSUES = ['id', 'key', 'statuscategorychangedate', 'issuetype_id', 'issuetype_name', 'timespent', 'project_key',
                 'fixVersions', 'aggregatetimespent', 'resolution', 'resolutiondate', 'workratio', 'lastViewed',
                 'created', 'priority_id', 'priority_name', 'labels', 'timeestimate', 'aggregatetimeoriginalestimate',
                 'assignee_accountId', 'assignee_displayName', 'updated', 'status_id', 'status_name', 'components',
                 'timeoriginalestimate', 'security', 'aggregatetimeestimate', 'summary', 'creator_accountId',
                 'creator_emailAddress', 'creator_displayName', 'subtasks', 'reporter_accountId',
                 'reporter_displayName', 'aggregateprogress_progress', 'aggregateprogress_total', 'environment',
                 'due_date', 'progress_progress', 'progress_total', 'custom_fields']
FIELDS_R_ISSUES = ['id', 'key', 'status_category_change_date', 'issue_type_id', 'issue_type_name', 'time_spent',
                   'project_key', 'fix_versions', 'aggregate_time_spent', 'resolution', 'resolution_date', 'work_ratio',
                   'last_viewed', 'created', 'priority_id', 'priority_name', 'labels', 'time_estimate',
                   'aggregate_time_original_estimate', 'assignee_account_id', 'assignee_display_name', 'updated',
                   'status_id', 'status_name', 'components', 'time_original_estimate', 'security',
                   'aggregate_time_estimate', 'summary', 'creator_account_id', 'creator_email_address',
                   'creator_display_name', 'subtasks', 'reporter_account_id', 'reporter_display_name',
                   'aggregate_progress', 'aggregate_progress_total', 'environment', 'due_date', 'progress',
                   'progress_total', 'custom_fields']
PK_ISSUES = ['id', 'key']
JSON_ISSUES = ['fixVersions', 'components', 'subtasks', 'custom_fields']

FIELDS_USERS = ['accountId', 'displayName', 'active', 'accountType']
FIELDS_R_USERS = ['account_id', 'display_name', 'active', 'account_type']
PK_USERS = ['account_id']
JSON_USERS = []

FIELDS_FIELDS = ['id', 'key', 'name', 'custom']
FIELDS_R_FIELDS = FIELDS_FIELDS
PK_FIELDS = ['id', 'key']
JSON_FIELDS = []

FIELDS_PROJECTS = ['id', 'key', 'name', 'projectCategory_id', 'projectCategory_name', 'projectTypeKey', 'isPrivate']
FIELDS_R_PROJECTS = ['id', 'key', 'name', 'project_category_id', 'project_category_name', 'project_type_key',
                     'is_private']
PK_PROJECTS = ['id', 'key']
JSON_PROJECTS = []

FIELDS_WORKLOGS_DELETED = ['worklogId', 'updatedTime']
FIELDS_R_WORKLOGS_DELETED = ['worklog_id', 'updated_time']
PK_WORKLOGS_DELETED = ['worklog_id']
JSON_WORKLOGS_DELETED = []

FIELDS_WORKLOGS = ['id', 'issueId', 'author_accountId', 'author_displayName', 'updateAuthor_accountId',
                   'updateAuthor_displayName', 'created', 'updated', 'started', 'timeSpent', 'timeSpentSeconds']
FIELDS_R_WORKLOGS = ['id', 'issue_id', 'author_account_id', 'author_display_name', 'update_author_account_id',
                     'update_author_display_name', 'created', 'updated', 'started', 'time_spent', 'time_spent_seconds']
PK_WORKLOGS = ['id']
JSON_WORKLOGS = []


class JiraWriter:

    def __init__(self, tableOutPath, tableName, incremental):

        self.paramPath = tableOutPath
        self.paramTableName = tableName
        self.paramTable = tableName + '.csv'
        self.paramTablePath = os.path.join(self.paramPath, self.paramTable)
        self.paramFields = eval(f'FIELDS_{tableName.upper().replace("-", "_")}')
        self.paramJsonFields = eval(f'JSON_{tableName.upper().replace("-", "_")}')
        self.paramPrimaryKey = eval(f'PK_{tableName.upper().replace("-", "_")}')
        self.paramFieldsRenamed = eval(f'FIELDS_R_{tableName.upper().replace("-", "_")}')
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

        self.writer = csv.DictWriter(open(self.paramTablePath, 'w'), fieldnames=self.paramFields,
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
                _dictToWrite = {**_dictToWrite, **{'custom_fields': _cust}}

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
