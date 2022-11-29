# Databricks notebook source
from WSGroupMigration import GroupMigration

# COMMAND ----------

groupL=['cody_dataengineers', 'Data engineering', 'isv_summit', 'Data Analyst - General', 'finance', 'zacdav-sp', 'Enterprise Data Platform']

# COMMAND ----------

#groupL=["analytics-team","field-eng-ssa-apj"]
account_id="9b624b1c-0393-47d4-84bd-7d61db4d38b7"
workspace_url = 'https://e2-demo-field-eng.cloud.databricks.com'
token='dapi9f43cfa7187dd43d52a3f5d515436cdb'

gm=GroupMigration( groupL = groupL , cloud="AWS" , account_id = account_id, workspace_url = workspace_url, pat=token, spark=spark )

# COMMAND ----------

gm.performInventory()

# COMMAND ----------

gm.groupList={'102499345587938': 'cody_dataengineers', '105297459836836': 'isv_summit', '191400518868607': 'finance', '266299903802400': 'Enterprise Data Platform', '360941307899477': 'Data engineering', '377035608819401': 'zacdav-sp', '848530893517779': 'Data Analyst - General'}

gm.groupMembers={'102499345587938': [['Cody Davis', '20904982561468'], ['isaac gritz', '4700330633718913']], '105297459836836': [['isv_summit_1', '18465440594517']], '266299903802400': [['Digan Parikh', '4452086101528713'], ['Chia-Yui Lee', '8642548114805478']], '848530893517779': [['ricardo@databricks.com', '209936474847338']]}

gm.groupEntitlements={'102499345587938': ['databricks-sql-access', 'workspace-access'], '105297459836836': ['databricks-sql-access'], '848530893517779': ['databricks-sql-access']}

gm.groupRoles={'102499345587938': ['arn:aws:iam::997819012307:instance-profile/shard-demo-s3-access']}

gm.groupNameDict={'cody_dataengineers': '102499345587938', 'isv_summit': '105297459836836', 'finance': '191400518868607', 'Enterprise Data Platform': '266299903802400', 'Data engineering': '360941307899477', 'zacdav-sp': '377035608819401', 'Data Analyst - General': '848530893517779'}

gm.passwordPerm={'passwords': []}

gm.clusterPerm={'1128-100123-cxjace8b': [['Data engineering', 'CAN_RESTART']]}

gm.clusterPolicyPerm={'E06216CAA0000DD8': [['cody_dataengineers', 'CAN_USE']], 'E06216CAA000045C': [['isv_summit', 'CAN_USE']], 'E06216CAA000045D': [['isv_summit', 'CAN_USE']], 'E06216CAA000045E': [['isv_summit', 'CAN_USE']], 'E06216CAA000002E': [['cody_dataengineers', 'CAN_USE']]}

gm.warehousePerm={'d9f8877fcdc09bbd': [['Data Analyst - General', 'CAN_USE']]}

gm.dashboardPerm={'68ca8a53-71d5-4940-b679-360396f4dfb3': [['finance', 'CAN_VIEW']]}

gm.queryPerm={'ec281fa8-e4e6-455b-a1d2-8fd4d9c1fc4b': [['finance', 'CAN_VIEW']], 'ea35a310-0acf-43a1-8ced-31ae7a63f92a': [['finance', 'CAN_VIEW']]}

gm.alertPerm={'a890e53d-2f34-419f-896f-13c386f69d93': [['finance', 'CAN_RUN']]}

gm.instancePoolPerm={'0831-175531-liven19-pool-scoiujgy': [['Data engineering', 'CAN_ATTACH_TO']]}

gm.jobPerm={595653761400223: [['zacdav-sp', 'CAN_VIEW']]}

gm.expPerm={}

gm.modelPerm={'cf03f76bdcef47438ffcfdb5b0e82219': [['Data engineering', 'CAN_READ']]}

gm.dltPerm={'0141af80-797a-4ddf-bc36-d0b881cf9598': [['Enterprise Data Platform', 'CAN_RUN']]}

gm.folderPerm={1578949507851185: [['finance', 'CAN_READ']]}

gm.notebookPerm={4313537760850635: [['finance', 'CAN_READ']]}

gm.repoPerm={29251126759419: [['Enterprise Data Platform', 'CAN_READ']]}

gm.tokenPerm={'tokens': [['Data engineering', 'CAN_USE']]}

gm.secretScopePerm={'solution-accelerator-cicd': [['finance', 'MANAGE']]}

#dataObjectsPerm=[Row(Principal='finance', ActionType='USAGE', ObjectType='DATABASE', ObjectKey='default'), Row(Principal='finance', ActionType='USAGE', ObjectType='TABLE', #ObjectKey='`default`.`2015_2_clickstream`'), Row(Principal='finance', ActionType='SELECT', ObjectType='VIEW', ObjectKey='`default`.`fs_customer_rfm`'), Row(Principal='finance', #ActionType='USAGE', ObjectType='VIEW', ObjectKey='`default`.`fs_customer_rfm`'), Row(Principal='finance', ActionType='MODIFY', ObjectType='ANY_FILE', ObjectKey='ANY FILE')]


# COMMAND ----------

print(gm.groupList)
print(gm.groupMembers)
print(gm.groupEntitlements)
print(gm.groupRoles)
print(gm.groupNameDict)  
print(gm.passwordPerm)
print(gm.clusterPerm)
print(gm.clusterPolicyPerm)
print(gm.warehousePerm)
print(gm.dashboardPerm)
print(gm.queryPerm)
print(gm.alertPerm)
print(gm.instancePoolPerm)
print(gm.jobPerm)
print(gm.expPerm)
print(gm.modelPerm)
print(gm.dltPerm)
print(gm.folderPerm)
print(gm.notebookPerm)
print(gm.repoPerm)
print(gm.tokenPerm)
print(gm.secretScopePerm)
print(gm.dataObjectsPerm)

# COMMAND ----------

gm.applyGroupPermission("Workspace")

# COMMAND ----------

gm.groupWSGNameDict[gm.groupList['102499345587938']+"_WSG"]

# COMMAND ----------

gm.createBackupGroup(groupL)

# COMMAND ----------

gm.groupMembers={'102499345587938': [['Cody Davis', '20904982561468'],
  ['isaac gritz', '4700330633718913']],
 '105297459836836': [['isv_summit_1', '18465440594517']],
 '266299903802400': [['Digan Parikh', '4452086101528713'],
  ['Chia-Yui Lee', '8642548114805478']],
 '848530893517779': [['ricardo@databricks.com', '209936474847338']]}

# COMMAND ----------

gm.groupNameDict={'cody_dataengineers': '102499345587938',
 'isv_summit': '105297459836836',
 'finance': '191400518868607',
 'Enterprise Data Platform': '266299903802400',
 'Data engineering': '360941307899477',
 'zacdav-sp': '377035608819401',
 'Data Analyst - General': '848530893517779'}

# COMMAND ----------

gm.groupWSGList={'562567771745791': 'cody_dataengineers_WSG',
 '868549194045217': 'Data engineering_WSG',
 '227384391126429': 'isv_summit_WSG',
 '1053290162719188': 'Data Analyst - General_WSG',
 '358561530984714': 'finance_WSG',
 '559528474475223': 'zacdav-sp_WSG',
 '690150727317692': 'Enterprise Data Platform_WSG'}

# COMMAND ----------

gm.groupWSGNameDict={'cody_dataengineers_WSG': '562567771745791',
 'Data engineering_WSG': '868549194045217',
 'isv_summit_WSG': '227384391126429',
 'Data Analyst - General_WSG': '1053290162719188',
 'finance_WSG': '358561530984714',
 'zacdav-sp_WSG': '559528474475223',
 'Enterprise Data Platform_WSG': '690150727317692'}

# COMMAND ----------

gm.applyGroupPermission("Workspace")

# COMMAND ----------

gm.groupMembers[gm.groupNameDict['cody_dataengineers']]

# COMMAND ----------

m='12121'

# COMMAND ----------

memberList="{\"value\":\""+m+"\"},"

# COMMAND ----------

memberList

# COMMAND ----------

import json
g='cody_dataengineers'
memberList=[]
for mem in gm.groupMembers[gm.groupNameDict[g]]:
  memberList.append({"value":mem[1]})
#memberList=memberList[:-1]
print(memberList)
data={
        "schemas": [ "urn:ietf:params:scim:schemas:core:2.0:Group" ],
        "displayName": g+'_WSG',
        "members": memberList
    }
print(json.dumps(data))

# COMMAND ----------

headers={'Authorization': 'Bearer %s' % token}
import requests,json
cloud='AWS'


# COMMAND ----------

data={'experiment_id':'4360330468404579'}

# COMMAND ----------

res=requests.get(f"{workspace_url}/api/2.0/mlflow/experiments/get", headers=headers, data=json.dumps(data))

# COMMAND ----------

res=requests.get(f"{workspace_url}/api/2.0/permissions/experiments/4360330468404579", headers=headers)

# COMMAND ----------

print(res.status_code)

# COMMAND ----------

passwordPerm=gm.getPasswordACL()

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT MODIFY On ANY FILE   to finance

# COMMAND ----------


