# Databricks notebook source
# MAGIC %md
# MAGIC # Workspace Group Migration
# MAGIC 
# MAGIC **Objective** <br/>
# MAGIC Customers who have groups created at workspace level, when they integrate with Unity Catalog and want to enable identity federation for users, groups, service principals at account level, face problems for groups federation. While users and service principals are synched up with account level identities, groups are not. As a result, customers cannot add account level groups to workspace if a workspace group with same name exists, which limits tru identity federation.
# MAGIC This notebook and the associated script is designed to help customer migrate workspace level groups to account level groups.
# MAGIC 
# MAGIC **How it works** <br/>
# MAGIC The script essentially performs following major steps:
# MAGIC  - Initiate the run by providing a list of workspace group to be migrated for a given workspace
# MAGIC  - Script performs inventory of all the ACL permission for the given workspace groups
# MAGIC  - Create back up workspace group of same name but add prefix "db-temp-" and apply the same ACL on them
# MAGIC  - Delete the original workspace groups
# MAGIC  - Add account level groups to the workspace
# MAGIC  - migrate the acl from temp workspace group to the new account level groups
# MAGIC  - delete the temp workspace groups
# MAGIC  
# MAGIC **Scope of ACL** <br/>
# MAGIC Following objects are covered as part of the ACL migration:
# MAGIC - Clusters
# MAGIC - Cluster policies
# MAGIC - Delta Live Tables pipelines
# MAGIC - Directories
# MAGIC - Jobs
# MAGIC - MLflow experiments
# MAGIC - MLflow registered models
# MAGIC - Notebooks
# MAGIC - Pools
# MAGIC - Repos
# MAGIC - Databricks SQL warehouses
# MAGIC - Dashboard
# MAGIC - Query 
# MAGIC - Alerts
# MAGIC - Tokens
# MAGIC - Password (for AWS)
# MAGIC - Instance Profile (for AWS)
# MAGIC - Secrets
# MAGIC - Table ACL (Non UC Cluster)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-requisite
# MAGIC 
# MAGIC Before running the script, please make sure you have the following checks
# MAGIC 1. Ensure you have equivalent account level group created for the workspace group to be migrated
# MAGIC 2. create a PAT token for the workspace which has admin access
# MAGIC 3. Ensure SCIM integration at workspace group is disabled
# MAGIC 4. Ensure no jobs or process is running the workspace using an user/service principal which is member of the workspace group
# MAGIC 5. Identify which ACL needs to be migrated, Ex: if there are no acl defined at experiment level, there is no need to do inventory check (as it may take time)

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to Run
# MAGIC 
# MAGIC Run the script in the following sequence
# MAGIC #### Step 1: Initialize the class
# MAGIC Import the module WSGroupMigration and initialize the class by passing following attributes:
# MAGIC - list of workspace group to be migrated (make sure these are workspace groups and not account level groups)
# MAGIC - if the workspace is AWS or Azure
# MAGIC - account id of the account console
# MAGIC - workspace url
# MAGIC - pat token of the admin to the workspace
# MAGIC - user name of the user whose pat token is generated 
# MAGIC - a list of workspace objects to be performed ACL on

# COMMAND ----------

from WSGroupMigration import GroupMigration
groupL=['analysts', 'dataengineers', '']
account_id="9b624b1c-0393-47d4-84bd-7d61db4d38b7"
workspace_url = 'https://e2-demo-field-eng.cloud.databricks.com'
token='dapi9f43cfa7187dd43d52a3f5d515436cdb'

gm=GroupMigration( groupL = groupL , cloud="AWS" , account_id = account_id, workspace_url = workspace_url, pat=token, spark=spark, userName='hari.selvarajan@databricks.com' )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Perform Dry run
# MAGIC This steps performs a dry run to verify the current ACL on the supplied workspace groups and print outs the permission.
# MAGIC Please verify if all the permissions are covered 

# COMMAND ----------

gm.dryRun(groupL)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Create Back up group
# MAGIC This steps creates the back up groups, applies the ACL on the new temp group from the original workspace group.
# MAGIC - Verify the temp groups are created in the workspace admin console
# MAGIC - check randomly if all the ACL are applied correctly
# MAGIC - there should be one temp group for every workspace group (Ex: db-temp-analysts and analysts with same ACLs)

# COMMAND ----------

gm.createBackupGroup(groupL)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Delete original workspace group
# MAGIC This steps deletes the original workspace group.
# MAGIC - Verify original workspace groups are deleted in the workspace admin console
# MAGIC - end user permissions shouldnt be impacted as ACL permission from temp workspace group should be in effect

# COMMAND ----------

gm.deleteGroups("Workspace")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Create account level groups
# MAGIC This steps adds the account level groups to the workspace and applies the same ACL from the back workspace group to the account level group.
# MAGIC - Ensure account level groups are created upfront before
# MAGIC - verify account level groups are added to the workspace now
# MAGIC - check randomly if all the ACL are applied correctly to the account level groups
# MAGIC - there should be one temp group and account level group present (Ex: db-temp-analysts and analysts (account level group) with same ACLs)

# COMMAND ----------

gm.createAccountGroup()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6: Delete temp workspace group
# MAGIC This steps deletes the temp workspace group.
# MAGIC - Verify temp workspace groups are deleted in the workspace admin console
# MAGIC - end user permissions shouldnt be impacted as ACL permission from account level group should be in effect

# COMMAND ----------

gm.deleteGroups("Account")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Complete
# MAGIC - Repeat the steps for other workspace group in the same workspace
# MAGIC - Repeat the steps for other workspace that require migration

# COMMAND ----------

from WSGroupMigration import GroupMigration

# COMMAND ----------

groupL=['analysts', 'dataengineers', '']


# COMMAND ----------

#groupL=["analytics-team","field-eng-ssa-apj"]
account_id="9b624b1c-0393-47d4-84bd-7d61db4d38b7"
workspace_url = 'https://e2-demo-field-eng.cloud.databricks.com'
token='dapi9f43cfa7187dd43d52a3f5d515436cdb'

gm=GroupMigration( groupL = groupL , cloud="AWS" , account_id = account_id, workspace_url = workspace_url, pat=token, spark=spark, userName='hari.selvarajan@databricks.com' )

# COMMAND ----------




# COMMAND ----------

gm.dryRun(groupL)

# COMMAND ----------

print(len(gm.folderList))
print(len(set(gm.folderList)))
print(len(gm.notebookList))
print(len(set(gm.notebookList)))

# COMMAND ----------

print(len(gm.folderList))
print(len(set(gm.folderList)))
print(len(gm.notebookList))
print(len(set(gm.notebookList)))

# COMMAND ----------

aa=[]
for k,v in gm.folderList.items():
  aa.append([k,v])
#print(aa)  
df = spark.createDataFrame(data=aa, schema = ["key","value"])
df.printSchema()
df.write.mode('Overwrite').saveAsTable('default.folderList')
bb=[]
for k,v in gm.notebookList.items():
  bb.append([k,v])
#print(aa)  
df = spark.createDataFrame(data=bb, schema = ["key","value"])
df.printSchema()
df.write.mode('Overwrite').saveAsTable('default.notebookList')

# COMMAND ----------

s='/User/khaskd'
s.startswith('/User')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from testtable

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table testtable

# COMMAND ----------

# MAGIC %sql
# MAGIC show grant on table default.testtable

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table default.testtable

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.history

# COMMAND ----------


spark.sql("show grants on table {}.`{}`".format("default", "testtable")).show()

# COMMAND ----------


len(set(gm.repolist))

# COMMAND ----------

len(set(gm.repolist))

# COMMAND ----------

gm.repoPerm

# COMMAND ----------

len(gm.notebookList)

# COMMAND ----------

gm.createBackupGroup(groupL)

# COMMAND ----------

gm.applyGroupPermission("Account")

# COMMAND ----------

gm.createAccountGroup()

# COMMAND ----------

from pyspark.sql.types import Row
gm.groupList={'498885132454885': 'db-temp-analysts', '839636208397196': 'db-temp-dataengineers'}
gm.accountGroups={'analysts': '247654616000993', 'dataengineers': '826689402090312'}
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


