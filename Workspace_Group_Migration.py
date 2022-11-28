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

gm.dryRun(groupL)

# COMMAND ----------

gm.getDataObjectsACL()

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


