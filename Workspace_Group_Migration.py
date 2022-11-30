# Databricks notebook source
#notes to add in markdown
# - overall purpose 
# objects in scope of migration
# objects not in scope (feature store) 
# long running api commenting (experiment jobs)
# pre requisite customer need to check (admin token, list of WSG, AG, SCIM switch off etc)

# COMMAND ----------

from WSGroupMigration import GroupMigration

# COMMAND ----------

groupL=['analysts', 'dataengineers']


# COMMAND ----------

#groupL=["analytics-team","field-eng-ssa-apj"]
account_id="9b624b1c-0393-47d4-84bd-7d61db4d38b7"
workspace_url = 'https://e2-demo-field-eng.cloud.databricks.com'
token='dapi9f43cfa7187dd43d52a3f5d515436cdb'

gm=GroupMigration( groupL = groupL , cloud="AWS" , account_id = account_id, workspace_url = workspace_url, pat=token, spark=spark, userName='hari.selvarajan@databricks.com' )

# COMMAND ----------

gm.validateTempWSGroup()

# COMMAND ----------

gm.dryRun(groupL)

# COMMAND ----------

gm.createBackupGroup(groupL)

# COMMAND ----------

gm.applyGroupPermission("Account")

# COMMAND ----------

gm.createAccountGroup()

# COMMAND ----------

from pyspark.sql.types import Row
gm.groupList={'498885132454885': 'db-temp-analysts', '839636208397196': 'db-temp-dataengineers'}
gm.accountGroups={'analysts': '639848920545827', 'dataengineers': '540828940295853'}
gm.groupMembers={'498885132454885': [['Hari Selvarajan', '152578202820718']], '839636208397196': [['Kiran Sreekumar', '3233358849413596']]}
gm.groupEntitlements={'498885132454885': ['allow-cluster-create', 'databricks-sql-access', 'workspace-access'], '839636208397196': ['databricks-sql-access', 'workspace-access']}
gm.groupRoles={'839636208397196': ['arn:aws:iam::997819012307:instance-profile/e2-uc-passrole']}
gm.groupNameDict={'db-temp-analysts': '498885132454885', 'db-temp-dataengineers': '839636208397196'}
gm.passwordPerm={'passwords': [['db-temp-analysts', 'CAN_USE']]}
gm.clusterPerm={'1128-174623-4jv5zbke': [['db-temp-dataengineers', 'CAN_RESTART'], ['db-temp-analysts', 'CAN_MANAGE']]}
gm.clusterPolicyPerm={'E06216CAA000152F': [['db-temp-analysts', 'CAN_USE']], 'E06216CAA000061A': [['db-temp-dataengineers', 'CAN_USE']]}
gm.warehousePerm={'ead10bf07050390f': [['db-temp-analysts', 'CAN_MANAGE'], ['db-temp-dataengineers', 'CAN_USE']]}
gm.dashboardPerm={'bfd04f2c-936c-48c7-b91f-170c189cb3ce': [{'user_name': 'liliana.tang@databricks.com', 'permission_level': 'CAN_MANAGE'}, {'group_name': 'db-temp-analysts', 'permission_level': 'CAN_VIEW'} ,{'group_name': 'admins', 'permission_level': 'CAN_MANAGE'}]}
gm.queryPerm={'468cb893-ccaf-441c-b28b-f90ed14e4b8f': [{'user_name': 'hari.selvarajan@databricks.com', 'permission_level': 'CAN_MANAGE'}, {'user_name': 'megan.fogal@databricks.com', 'permission_level': 'CAN_MANAGE'}, {'group_name': 'db-temp-analysts', 'permission_level': 'CAN_VIEW'}, {'group_name': 'db-temp-dataengineers', 'permission_level': 'CAN_RUN'}, {'group_name': 'admins', 'permission_level': 'CAN_MANAGE'}]}
gm.alertPerm={'a890e53d-2f34-419f-896f-13c386f69d93': [{'user_name': 'hari.selvarajan@databricks.com', 'permission_level': 'CAN_MANAGE'}, {'user_name': 'kamalendu.biswas@databricks.com', 'permission_level': 'CAN_MANAGE'}, {'group_name': 'db-temp-analysts', 'permission_level': 'CAN_EDIT'}, {'group_name': 'admins', 'permission_level': 'CAN_MANAGE'}]}
gm.instancePoolPerm={'0914-113858-clefs15-pool-mms8nsia': [['db-temp-analysts', 'CAN_MANAGE'], ['db-temp-dataengineers', 'CAN_ATTACH_TO']]}
gm.jobPerm={843340100493178: [['db-temp-dataengineers', 'CAN_VIEW'], ['db-temp-analysts', 'CAN_MANAGE_RUN']]}
gm.expPerm={'4360330468576675': [['db-temp-analysts', 'CAN_READ']]}
gm.modelPerm={'5c11adab802a42479a633b80335a2035': [['db-temp-dataengineers', 'CAN_READ']]}
gm.dltPerm={'0141af80-797a-4ddf-bc36-d0b881cf9598': [['db-temp-analysts', 'CAN_RUN'], ['db-temp-dataengineers', 'CAN_VIEW']]}
gm.folderPerm={4313537760850596: [['db-temp-analysts', 'CAN_EDIT']]}
gm.notebookPerm={1578949507851187: [['db-temp-dataengineers', 'CAN_READ']]}
gm.repoPerm={29251126759419: [['db-temp-dataengineers', 'CAN_RUN'], ['db-temp-analysts', 'CAN_READ']]}
gm.tokenPerm={'tokens': [['db-temp-dataengineers', 'CAN_USE']]}
gm.secretScopePerm={'solution-accelerator-cicd': [['db-temp-analysts', 'MANAGE']]}
gm.dataObjectsPerm=[Row(Principal='db-temp-analysts', ActionType='USAGE', ObjectType='DATABASE', ObjectKey='default'), Row(Principal='db-temp-dataengineers', ActionType='USAGE', ObjectType='DATABASE', ObjectKey='default'), Row(Principal='db-temp-dataengineers', ActionType='SELECT', ObjectType='TABLE', ObjectKey='`default`.`2015_2_clickstream`'), Row(Principal='db-temp-analysts', ActionType='USAGE', ObjectType='TABLE', ObjectKey='`default`.`2015_2_clickstream`'), Row(Principal='db-temp-dataengineers', ActionType='USAGE', ObjectType='TABLE', ObjectKey='`default`.`2015_2_clickstream`'), Row(Principal='db-temp-analysts', ActionType='MODIFY', ObjectType='VIEW', ObjectKey='`default`.`fs_customer_rfm`'), Row(Principal='db-temp-dataengineers', ActionType='MODIFY', ObjectType='VIEW', ObjectKey='`default`.`fs_customer_rfm`'), Row(Principal='db-temp-analysts', ActionType='USAGE', ObjectType='VIEW', ObjectKey='`default`.`fs_customer_rfm`'), Row(Principal='db-temp-dataengineers', ActionType='USAGE', ObjectType='VIEW', ObjectKey='`default`.`fs_customer_rfm`'), Row(Principal='db-temp-analysts', ActionType='MODIFY', ObjectType='', ObjectKey='ANY FILE')]



# COMMAND ----------


