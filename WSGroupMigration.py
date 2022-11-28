from os import getgrouplist
import requests
import json, math
from pyspark.sql.functions import lit,col,column
from functools import reduce
from pyspark.sql import DataFrame, session

class GroupMigration:

    def __init__(self, groupL : list, cloud : str, account_id : str, workspace_url : str, pat : str, spark : session.SparkSession):
        self.groupL=groupL
        self.cloud=cloud    
        self.workspace_url = workspace_url
        self.account_id=account_id
        self.token=pat
        self.headers={'Authorization': 'Bearer %s' % self.token}
        self.groupList={}
        self.groupWSGList={}
        self.accountGroups={}
        self.groupMembers={}
        self.groupEntitlements={}
        self.groupNameDict={}
        self.groupWSGNameDict={}
        self.groupRoles={}
        self.passwordPerm={}
        self.clusterPerm={}
        self.clusterPolicyPerm={}
        self.warehousePerm={}
        self.dashboardPerm={}
        self.queryPerm={}
        self.alertPerm={}
        self.instancePoolPerm={}
        self.jobPerm={}
        self.expPerm={}
        self.modelPerm={}
        self.dltPerm={}
        self.folderPerm={}
        self.notebookPerm={}
        self.repoPerm={}
        self.tokenPerm={}
        self.secretScopePerm={}
        self.dataObjectsPerm=[]
        self.folderList={}
        self.notebookList={}
        self.spark=spark
    


    def getGroupObjects(self)->list:
        try:
            groupList={}
            groupMembers={}
            groupEntitlements={}
            groupRoles={}
            res=requests.get(f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups", headers=self.headers)
            resJson=res.json()
            for e in resJson['Resources']:
                if e['displayName'] in self.groupL:
                  groupList[e['id']]=e['displayName']
                members=[]
                try:
                    for mem in e['members']:
                        members.append(list([mem['display'],mem['value']]))
                except KeyError:
                    continue
                if e['displayName'] in self.groupL:
                  groupMembers[e['id']]=members
                entms=[]
                try:
                    for ent in e['entitlements']:
                        entms.append(ent['value'])
                except:
                    pass
                if len(entms)==0:
                    continue
                if e['displayName'] in self.groupL:
                  groupEntitlements[e['id']]=entms
                entms=[]
                if self.cloud=='AWS':
                  try:
                      for ent in e['roles']:
                          entms.append(ent['value'])
                  except:
                      pass
                  if len(entms)==0:
                    continue
                  if e['displayName'] in self.groupL:
                    groupRoles[e['id']]=entms   
            return [groupList, groupMembers, groupEntitlements, groupRoles]
        except Exception as e:
            print(f'error in retriveing group objects : {e}')


    def getACL(self, acls:dict)->list:
        aclList=[]
        for acl in acls:
            try:
                if acl['all_permissions'][0]['inherited']==True:continue
                aclList.append(list([acl['group_name'],acl['all_permissions'][0]['permission_level']]))
            except KeyError:
                continue
        aclList=[acl for acl in aclList if acl[0] in self.groupL]
        return aclList
    def getACL2(self, acls:dict)->list:
        aclList=[]
        for acl in acls:
            try:
                aclList.append(list([acl['group_name'],acl['permission_level']]))
            except KeyError:
                continue
        aclList=[acl for acl in aclList if acl[0] in self.groupL]
        return aclList
    def getClusterACL(self)-> dict:
        try:

            resC=requests.get(f"{self.workspace_url}/api/2.0/clusters/list", headers=self.headers)
            resCJson=resC.json()
            clusterPerm={}
            for c in resCJson['clusters']:
                clusterId=c['cluster_id']
                resCPerm=requests.get(f"{self.workspace_url}/api/2.0/preview/permissions/clusters/{clusterId}", headers=self.headers)
                if resCPerm.status_code==404:
                    print(f'cluster ACL not enabled for the cluster: {clusterId}')
                    pass
                resCPermJson=resCPerm.json()            
                aclList=self.getACL(resCPermJson['access_control_list'])
                if len(aclList)==0:continue
                clusterPerm[clusterId]=aclList                
            return clusterPerm    
        except Exception as e:
            print(f'error in retriveing cluster permission: {e}')

    def getClusterPolicyACL(self)-> dict:
        try:
            resCP=requests.get(f"{self.workspace_url}/api/2.0/policies/clusters/list", headers=self.headers)
            resCPJson=resCP.json()
            if resCPJson['total_count']==0:
                print('No cluster policies defined.')
                return {}
            clusterPolicyPerm={}
            for c in resCPJson['policies']:
                policyid=c['policy_id']
                resCPPerm=requests.get(f"{self.workspace_url}/api/2.0/preview/permissions/cluster-policies/{policyid}", headers=self.headers)
                if resCPPerm.status_code==404:
                    print(f'cluster policy feature is not enabled for this tier.')
                    pass
                resCPPermJson=resCPPerm.json()            
                aclList=self.getACL(resCPPermJson['access_control_list'])
                if len(aclList)==0:continue
                clusterPolicyPerm[policyid]=aclList                
            return clusterPolicyPerm
        except Exception as e:
            print(f'error in retriveing cluster policy permission: {e}')

    def getWarehouseACL(self)-> dict:
        try:
            resW=requests.get(f"{self.workspace_url}/api/2.0/sql/warehouses", headers=self.headers)
            resWJson=resW.json()
            warehousePerm={}
            for c in resWJson['warehouses']:
                warehouseId=c['id']
                resWPerm=requests.get(f"{self.workspace_url}/api/2.0/preview/permissions/sql/warehouses/{warehouseId}", headers=self.headers)
                if resWPerm.status_code==404:
                    print(f'feature not enabled for this tier')
                    pass
                resWPermJson=resWPerm.json()            
                aclList=self.getACL(resWPermJson['access_control_list'])                   
                if len(aclList)==0:continue
                warehousePerm[warehouseId]=aclList               
            return warehousePerm
        except Exception as e:
            print(f'error in retriveing warehouse permission: {e}')

    def getDashboardACL(self)-> dict:
        try:
            resD=requests.get(f"{self.workspace_url}/api/2.0/preview/sql/dashboards", headers=self.headers)
            resDJson=resD.json()
            pages=math.ceil(resDJson['count']/resDJson['page_size'])
            
            dashboardPerm={}
            for pg in range(1,4+1):
                resD=requests.get(f"{self.workspace_url}/api/2.0/preview/sql/dashboards?page={str(pg)}", headers=self.headers)
                resDJson=resD.json()            
                for c in resDJson['results']:
                    dashboardId=c['id']
                    resDPerm=requests.get(f"{self.workspace_url}/api/2.0/preview/sql/permissions/dashboards/{dashboardId}", headers=self.headers)
                    if resDPerm.status_code==404:
                        print(f'feature not enabled for this tier')
                        pass
                    resDPermJson=resDPerm.json() 
                    aclList=self.getACL2(resDPermJson['access_control_list']  )      
                    if len(aclList)==0:continue
                    dashboardPerm[dashboardId]=aclList               
            return dashboardPerm

        except Exception as e:
            print(f'error in retriveing dashboard permission: {e}')
            raise e
    def getQueriesACL(self)-> dict:
        try:
            resQ=requests.get(f"{self.workspace_url}/api/2.0/preview/sql/queries", headers=self.headers)
            resQJson=resQ.json()
            queryPerm={}
            pages=math.ceil(resQJson['count']/resQJson['page_size'])
            for pg in range(1,4+1):
                resQ=requests.get(f"{self.workspace_url}/api/2.0/preview/sql/queries?page={str(pg)}", headers=self.headers)
                resQJson=resQ.json()    
                for c in resQJson['results']:
                    queryId=c['id']
                    resQPerm=requests.get(f"{self.workspace_url}/api/2.0/preview/sql/permissions/queries/{queryId}", headers=self.headers)
                    if resQPerm.status_code==404:
                        print(f'feature not enabled for this tier')
                        pass
                    resQPermJson=resQPerm.json() 
                    aclList=self.getACL2(resQPermJson['access_control_list']  )   
                    if len(aclList)==0:continue
                    queryPerm[queryId]=aclList               
            return queryPerm

        except Exception as e:
            print(f'error in retriveing query permission: {e}')
    def getAlertsACL(self)-> dict:
        try:
            resA=requests.get(f"{self.workspace_url}/api/2.0/preview/sql/alerts", headers=self.headers)
            resAJson=resA.json()
            alertPerm={}
            for c in resAJson:
                alertId=c['id']
                resAPerm=requests.get(f"{self.workspace_url}/api/2.0/preview/sql/permissions/alerts/{alertId}", headers=self.headers)
                if resAPerm.status_code==404:
                    print(f'feature not enabled for this tier')
                    pass
                resAPermJson=resAPerm.json() 
                aclList=self.getACL2(resAPermJson['access_control_list'] )                
                if len(aclList)==0:continue
                alertPerm[alertId]=aclList               
            return alertPerm

        except Exception as e:
            print(f'error in retriveing alerts permission: {e}')

    def getPasswordACL(self)-> dict:
        try:
            if self.cloud!='AWS':
              return
            resP=requests.get(f"{self.workspace_url}/api/2.0/preview/permissions/authorization/passwords", headers=self.headers)
            resPJson=resP.json()
            if len(resPJson)==0:
                print('No password acls defined.')
                return {}
            passwordPerm={}
            passwordPerm['passwords']=self.getACL(resPJson['access_control_list'])            
            return passwordPerm
        except Exception as e:
            print(f'error in retriveing password  permission: {e}')

    def getPoolACL(self)-> dict:
        try:
            resIP=requests.get(f"{self.workspace_url}/api/2.0/instance-pools/list", headers=self.headers)
            resIPJson=resIP.json()
            if len(resIPJson)==0:
                print('No Instance Pools defined.')
                return {}
            instancePoolPerm={}
            for c in resIPJson['instance_pools']:
                instancePID=c['instance_pool_id']
                resIPPerm=requests.get(f"{self.workspace_url}/api/2.0/preview/permissions/instance-pools/{instancePID}", headers=self.headers)
                if resIPPerm.status_code==404:
                    print(f'feature not enabled for this tier')
                    pass
                resIPPermJson=resIPPerm.json()   
                aclList=self.getACL(resIPPermJson['access_control_list'])            
                if len(aclList)==0:continue
                instancePoolPerm[instancePID]=aclList                
            return instancePoolPerm
        except Exception as e:
            print(f'error in retriveing Instance Pool permission: {e}') 

    def getJobACL(self)-> dict: 
        try:
            jobPerm={}
            while True:
                resJob=requests.get(f"{self.workspace_url}/api/2.1/jobs/list", headers=self.headers)
                resJobJson=resJob.json()
                if resJob.text=="{\"has_more\":false}":
                    print('No jobs available')
                    return {}
                #print(resJobJson['jobs'])
                for c in resJobJson['jobs']:
                    jobID=c['job_id']
                    resJobPerm=requests.get(f"{self.workspace_url}/api/2.0/permissions/jobs/{jobID}", headers=self.headers)
                    if resJobPerm.status_code==404:
                        print(f'feature not enabled for this tier')
                        pass
                    resJobPermJson=resJobPerm.json()   
                    aclList=self.getACL(resJobPermJson['access_control_list'])                
                    if len(aclList)==0:continue
                    jobPerm[jobID]=aclList    
                if resJobJson['has_more']==True:
                    break    
            return jobPerm
        except Exception as e:
            print(f'error in retriveing job permission: {e}')
    def getExperimentACL(self)-> dict:
        try:
            nextPageToken='' 
            expPerm={}
            while True:
                data={}
                data={'max_results':20}
                if nextPageToken!="":    
                    data={'page_token':nextPageToken, 'max_results':'20'}
                
                resExp=requests.get(f"{self.workspace_url}/api/2.0/mlflow/experiments/list", headers=self.headers,data=json.dumps(data))
                resExpJson=resExp.json()
                #print(resExpJson['experiments'])
                if len(resExpJson)==0:
                    print('No experiments available')
                    return {}
                for c in resExpJson['experiments']:
                    
                    expID=c['experiment_id']
                    resExpPerm=requests.get(f"{self.workspace_url}/api/2.0/permissions/experiments/{expID}", headers=self.headers)
                    if resExpPerm.status_code==404:
                        print(f'feature not enabled for this tier')
                        pass
                    resExpPermJson=resExpPerm.json()   
                    if resExpPerm.status_code!=200:
                      print(f'unable to get permission for experiment {expID}')
                      continue 
                    aclList=self.getACL(resExpPermJson['access_control_list'])                
                    if len(aclList)==0:continue
                    
                    expPerm[expID]=aclList  
                try:
                    nextPageToken=resExpJson['next_page_token']
                    break
                except KeyError:
                    break
            return expPerm
        except Exception as e:
            print(f'error in retriveing experiment permission: {e}')
    def getModelACL(self)-> dict:
        try:
            nextPageToken=''
            expPerm={}
            while True:    
                data={}
                data={'max_results':20}
                if nextPageToken!="":    
                    data={'page_token':nextPageToken}    
                resModel=requests.get(f"{self.workspace_url}/api/2.0/mlflow/registered-models/list", headers=self.headers,data=json.dumps(data))
                resModelJson=resModel.json()
                if len(resModelJson)==0:
                    print('No models available')
                    return {}
                modelPerm={}
                for c in resModelJson['registered_models']:
                    modelName=c['name']
                    param={'name':modelName}
                    modIDRes=requests.get(f"{self.workspace_url}/api/2.0/mlflow/databricks/registered-models/get", headers=self.headers, data=json.dumps(param))
                    modelID=modIDRes.json()['registered_model_databricks']['id']
                    resModelPerm=requests.get(f"{self.workspace_url}/api/2.0/permissions/registered-models/{modelID}", headers=self.headers)
                    if resModelPerm.status_code==404:
                        print(f'feature not enabled for this tier')
                        pass
                    resModelPermJson=resModelPerm.json()   
                    aclList=self.getACL(resModelPermJson['access_control_list'])                
                    if len(aclList)==0:continue
                    modelPerm[modelID]=aclList  
                try:
                    nextPageToken=resModelJson['next_page_token']
                    break
                except KeyError:
                    break
            return modelPerm
        except Exception as e:
            print(f'error in retriveing model permission: {e}')
    def getDLTACL(self)-> dict:
        try:
            nextPageToken=''
            dltPerm={}
            while True:
                data={}
                data={'max_results':20}
                if nextPageToken!="":    
                    data={'page_token':nextPageToken}
                resDlt=requests.get(f"{self.workspace_url}/api/2.0/pipelines", headers=self.headers,data=json.dumps(data))
                resDltJson=resDlt.json()
                if len(resDltJson)==0:
                    print('No dlt pipelines available')
                    return {}
                for c in resDltJson['statuses']:
                    dltID=c['pipeline_id']
                    resDltPerm=requests.get(f"{self.workspace_url}/api/2.0/permissions/pipelines/{dltID}", headers=self.headers)
                    if resDltPerm.status_code==404:
                        print(f'feature not enabled for this tier')
                        pass
                    resDltPermJson=resDltPerm.json()   
                    aclList=self.getACL(resDltPermJson['access_control_list'])
                    if len(aclList)==0:continue
                    dltPerm[dltID]=aclList  
                try:
                    nextPageToken=resDltJson['next_page_token']
                    break
                except KeyError:
                    break

            return dltPerm
        except Exception as e:
            print(f'error in retriveing dlt pipelines permission: {e}')

    def getFolderList(self, path:str)-> dict:
        try:
            data={'path':path}
            resFolder=requests.get(f"{self.workspace_url}/api/2.0/workspace/list", headers=self.headers, data=json.dumps(data))
            resFolderJson=resFolder.json()
            folderPerm={}
            if len(resFolderJson)==0:
                return
            for c in resFolderJson['objects']:
                if c['object_type']=="DIRECTORY":
                    self.folderList[c['object_id']]=c['path']
                    self.getFolderList(c['path'])
                elif c['object_type']=="NOTEBOOK":
                    self.notebookList[c['object_id']]=c['path']

            return 
        except Exception as e:
            print(f'error in retriving folder details: {e}')


    def getFoldersNotebookACL(self)-> list:
        try:
            self.getFolderList("/Users/hari.selvarajan@databricks.com")
            folderPerm={}
            notebookPerm={}
            #print(notebookList)
            for k,v in self.folderList.items():
                resFolderPerm=requests.get(f"{self.workspace_url}/api/2.0/permissions/directories/{k}", headers=self.headers)
                if resFolderPerm.status_code==404:
                    print(f'feature not enabled for this tier')
                    pass
                resFolderPermJson=resFolderPerm.json()   
                aclList=self.getACL(resFolderPermJson['access_control_list'])            
                if len(aclList)==0:continue
                folderPerm[k]=aclList  
            for k,v in self.notebookList.items():
                resNotebookPerm=requests.get(f"{self.workspace_url}/api/2.0/permissions/notebooks/{k}", headers=self.headers)
                if resNotebookPerm.status_code==404:
                    print(f'feature not enabled for this tier')
                    pass
                resNotebookPermJson=resNotebookPerm.json()   
                aclList=self.getACL(resNotebookPermJson['access_control_list'])
                if len(aclList)==0:continue
                notebookPerm[k]=aclList  
            return folderPerm, notebookPerm
        except Exception as e:
            print(f'error in retriveing folder permission: {e}')

    def getRepoACL(self)-> dict:
        try:
            nextPageToken=''
            repoPerm={}
            while True:
                data={}
                data={'max_results':20}
                if nextPageToken!="":    
                    data={'page_token':nextPageToken}
                resRepo=requests.get(f"{self.workspace_url}/api/2.0/repos", headers=self.headers,data=json.dumps(data))
                resRepoJson=resRepo.json()
                if len(resRepoJson)==0:
                    print('No repos available')
                    return {}
                for c in resRepoJson['repos']:
                    repoID=c['id']
                    resRepoPerm=requests.get(f"{self.workspace_url}/api/2.0/permissions/repos/{repoID}", headers=self.headers)
                    if resRepoPerm.status_code==404:
                        print(f'feature not enabled for this tier')
                        pass
                    resRepoPermJson=resRepoPerm.json()   
                    aclList=self.getACL(resRepoPermJson['access_control_list'])
                    if len(aclList)==0:continue
                    repoPerm[repoID]=aclList  
                try:
                    nextPageToken=resRepoJson['next_page_token']
                    break
                except KeyError:
                    break

            return repoPerm
        except Exception as e:
            print(f'error in retriveing repos permission: {e}')
    def getTokenACL(self)-> dict:
        try:
            tokenPerm={}
            resTokenPerm=requests.get(f"{self.workspace_url}/api/2.0/preview/permissions/authorization/tokens", headers=self.headers)
            if resTokenPerm.status_code==404:
                print(f'feature not enabled for this tier')
                pass
            resTokenPermJson=resTokenPerm.json()   
            aclList=[]     
            for acl in resTokenPermJson['access_control_list']:
                try:
                    if acl['all_permissions'][0]['inherited']==True:continue
                    aclList.append(list([acl['group_name'],acl['all_permissions'][0]['permission_level']]))
                except KeyError:
                    continue
            aclList=[acl for acl in aclList if acl[0] in self.groupL]
            tokenPerm['tokens']=aclList  
            return tokenPerm
        except Exception as e:
            print(f'error in retriveing Token permission: {e}')
    def getSecretScoppeACL(self)-> dict:
        try:

            resSScope=requests.get(f"{self.workspace_url}/api/2.0/secrets/scopes/list", headers=self.headers)
            resSScopeJson=resSScope.json()
            if len(resSScopeJson)==0:
                print('No secret scopes defined.')
                return {}
            secretScopePerm={}
            for c in resSScopeJson['scopes']:
                scopeName=c['name']
                data={'scope':scopeName}
                resSSPerm=requests.get(f"{self.workspace_url}/api/2.0/secrets/acls/list/", headers=self.headers, data=json.dumps(data))  
                if resSSPerm.status_code==404:
                    print(f'feature not enabled for this tier')
                    pass
                resSSPermJson=resSSPerm.json()   
                aclList=[]

                for acl in resSSPermJson['items']:
                    try:
                        if acl['principal'] in self.groupL:
                            aclList.append(list([acl['principal'],acl['permission']]))
                    except KeyError:
                        continue
                if len(aclList)==0:continue
                secretScopePerm[scopeName]=aclList    

            return secretScopePerm
        except Exception as e:
            print(f'error in retriving Secret Scope permission: {e}')


    def updateGroupEntitlements(self, groupEntitlements:dict, level:str):
        try:
            for group_id, etl in groupEntitlements.items():
                entitlementList=[]
                if level=="Workspace":
                  groupId=self.groupWSGNameDict[self.groupList[group_id]]
                else:
                  groupId=self.accountGroups[self.groupList[group_id]]
                for e in etl:
                    entitlementList.append({"value":e})
                entitlements = {
                                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                                "Operations": [{"op": "add",
                                            "path": "entitlements",
                                            "value": entitlementList}]
                            }
                resPatch=requests.patch(f'{self.workspace_url}/api/2.0/preview/scim/v2/Groups/{group_id}', headers=self.headers, data=json.dumps(entitlements))
        except Exception as e:
            print(f'error applying entitiement for group id: {group_id}.')

    def updateGroupRoles(self, groupRoles:dict, level:str):
        try:

            for group_id, roles in groupRoles.items():
                roleList=[]
                if level=="Workspace":
                  groupId=self.groupWSGNameDict[self.groupList[group_id]]
                else:
                  groupId=self.accountGroups[self.groupList[group_id]]
                for e in roles:
                    roleList.append({"value":e})
                instanceProfileRoles = {
                                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                                "Operations": [{"op": "add",
                                            "path": "roles",
                                            "value": roleList}]
                            }
                resPatch=requests.patch(f'{self.workspace_url}/api/2.0/preview/scim/v2/Groups/{group_id}', headers=self.headers, data=json.dumps(instanceProfileRoles))
        except Exception as e:
            print(f'error applying role for group id: {group_id}.')

    def updateGroupPermission(self, object:str, groupPermission : dict, level:str):
        try:
          suffix=""
          if level=="Workspace":suffix="_WSG"

          for object_id,aclList in groupPermission.items(): 
              dataAcl=[]
              for  acl in aclList:
                  dataAcl.append({"group_name":acl[0]+suffix,"permission_level":acl[1]})
              data={"access_control_list":dataAcl}
              resAppPerm=requests.patch(f"{self.workspace_url}/api/2.0/preview/permissions/{object}/{object_id}", headers=self.headers, data=json.dumps(data))
        except Exception as e:
            print(f'Error setting permission for {object} {object_id}. {e} ')
    def updateGroup2Permission(self, object:str, groupPermission : dict, level:str):
        try:
          suffix=""
          if level=="Workspace":suffix="_WSG"

          for object_id,aclList in groupPermission.items(): 
              dataAcl=[]
              aclList=[[acl[0]+suffix,acl[1]] for acl in aclList]
              data={"access_control_list":aclList}
              resAppPerm=requests.post(f"{self.workspace_url}/api/2.0/preview/sql/permissions/{object}/{object_id}", headers=self.headers, data=json.dumps(data))
        except Exception as e:
            print(f'Error setting permission for {object} {object_id}. {e} ')
    def updateSecretPermission(self, secretPermission : dict):
        try:
            for object_id,aclList in secretPermission.items(): 
                dataAcl=[]
                for  acl in aclList:
                    data={"scope":object_id, "principal":acl[0], "permission":acl[1]}
                    resAppPerm=requests.post(f"{self.workspace_url}/api/2.0/secrets/acls/put", headers=self.headers, data=json.dumps(data))
        except Exception as e:
            print(f'Error setting permission for scope {object_id}. {e} ')
    def getDataObjectsACL(self)-> list:
      dbs = self.spark.sql("show databases")
      aclList = []

      for db in dbs.collect():
        databaseName = ""

        databaseName = db.databaseName
        databaseName = 'default'

        # append the database df to the list
        df=(self.spark.sql("SHOW GRANT ON DATABASE {}".format(databaseName))
                       .withColumn("ObjectKey", lit(databaseName))
                       .withColumn("ObjectType", lit("DATABASE"))
                       .filter(col("ActionType")!="OWN")
           )
        aclList=df.collect()
        tables = self.spark.sql("show tables in {}".format(databaseName)).filter(col("isTemporary") == False)
        for table in tables.collect():
          dft=(self.spark.sql("show grant on table {}.{}".format(table.database, table.tableName))
                         .withColumn("ObjectKey", lit("`" + table.database + "`.`" + table.tableName + "`"))
                         .withColumn("ObjectType", lit("TABLE"))
                        )
          aclList+=dft.collect()
          break

        views = self.spark.sql("show views in {}".format(databaseName)).filter(col("isTemporary") == False)
        for view in views.collect():
          dft=(self.spark.sql("show grant on view {}.{}".format(view.namespace, view.viewName))
                         .withColumn("ObjectKey", lit("`" + view.namespace + "`.`" + view.viewName + "`"))
                         .withColumn("ObjectType", lit("VIEW"))
                        )
          aclList+=dft.collect()
          break

        functions = self.spark.sql("show functions in {}".format(databaseName)).filter(col("function").startswith(databaseName+"."))
        for function in functions.collect():
          dft=(self.spark.sql("show grant on function {}".format( function.function))
                         .withColumn("ObjectKey", lit("`" + function.function + "`"))
                         .withColumn("ObjectType", lit("FUNCTION"))
                        )
          aclList+=dft.collect()
          break


        break
      dft=(self.spark.sql("show grant on any file ")
                     .withColumn("ObjectKey", lit("ANY FILE"))
                     .withColumn("ObjectType", lit("ANY_FILE"))
                    )
      aclList+=dft.collect()
      #aclFinalList=[acl for acl in aclList]
      aclFinalList=[acl for acl in aclList if acl[0] in self.groupL]

      return aclFinalList
    def updateDataObjectsPermission(aclList : list, level:str):
        try:
            suffix=""
            if level=="Workspace":suffix="_WSG"
            for acl in aclList: 
                aclQuery = "GRANT {} ON {} {} TO `{}`".format(acl.ActionType, acl.ObjectType, acl.ObjectKey, acl.Principal+suffix)
                print(aclQuery)
                spark.sql(aclQuery)
        except Exception as e:
            print(f'Error setting permission, {e} ')
    def performInventory(self):
      try:
        res=requests.get(f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups", headers=self.headers)

        self.groupList, self.groupMembers, self.groupEntitlements, self.groupRoles=self.getGroupObjects()
        groupNames=[v for k,v in self.groupList.items()]
        for k,v in self.groupList.items():
          self.groupNameDict[v]=k    
        self.passwordPerm= self.getPasswordACL()
        self.clusterPerm=self.getClusterACL()
        self.clusterPolicyPerm=self.getClusterPolicyACL()
        self.warehousePerm=self.getWarehouseACL()
        self.dashboardPerm=self.getDashboardACL()
        self.queryPerm=self.getQueriesACL()
        self.alertPerm=self.getAlertsACL()
        self.instancePoolPerm=self.getPoolACL()
        self.jobPerm=self.getJobACL()
        self.expPerm=self.getExperimentACL()
        self.modelPerm=self.getModelACL()
        self.dltPerm=self.getDLTACL()
        self.folderPerm, self.notebookPerm=self.getFoldersNotebookACL()
        self.repoPerm=self.getRepoACL()
        self.tokenPerm=self.getTokenACL()
        self.secretScopePerm=self.getSecretScoppeACL()
        self.dataObjectsPerm=self.getDataObjectsACL()

      except Exception as e:
        print(f" Error creating group inventory, {e}")
    def dryRun(self, groupList:list ):
      try:
        self.performInventory()        
        print('Displaying existing ACL of the selected groups:')
        print('Group List:')
        print("{:<20} {:<10}".format('Group ID', 'Group Name'))
        for key, value in self.groupList.items():print("{:<20} {:<10}".format(key, value))
        print('Group Members:')
        print("{:<20} {:<100}".format('Group ID', 'Group Member'))
        for key, value in self.groupMembers.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Group Entitlements:')
        print("{:<20} {:<100}".format('Group ID', 'Group Entitlements'))
        for key, value in self.groupEntitlements.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Group Roles:')
        print("{:<20} {:<100}".format('Group ID', 'Group Roles'))
        for key, value in self.groupRoles.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Group Passwords:')
        print("{:<20} {:<100}".format('Password', 'Group Names'))
        for key, value in self.passwordPerm.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Cluster Permission:')
        print("{:<20} {:<100}".format('Cluster ID', 'Group Permission'))
        for key, value in self.clusterPerm.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Cluster Policy Permission:')
        print("{:<20} {:<100}".format('Cluster Policy ID', 'Group Permission'))
        for key, value in self.clusterPolicyPerm.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Warehouse Permission:')
        print("{:<20} {:<100}".format('SQL Warehouse ID', 'Group Permission'))
        for key, value in self.warehousePerm.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Dashboard Permission:')
        print("{:<20} {:<100}".format('Dashboard ID', 'Group Permission'))
        for key, value in self.dashboardPerm.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Query Permission:')
        print("{:<20} {:<100}".format('Query ID', 'Group Permission'))
        for key, value in self.queryPerm.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Alerts Permission:')
        print("{:<20} {:<100}".format('Alerts ID', 'Group Permission'))
        for key, value in self.alertPerm.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Instance Pool Permission:')
        print("{:<20} {:<100}".format('InstancePool ID', 'Group Permission'))
        for key, value in self.instancePoolPerm.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Jobs Permission:')
        print("{:<20} {:<100}".format('Job ID', 'Group Permission'))
        for key, value in self.jobPerm.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Experiments Permission:')
        print("{:<20} {:<100}".format('Experiment ID', 'Group Permission'))
        for key, value in self.expPerm.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Models Permission:')
        print("{:<20} {:<100}".format('Model ID', 'Group Permission'))
        for key, value in self.modelPerm.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Delta Live Tables Permission:')
        print("{:<20} {:<100}".format('Pipeline ID', 'Group Permission'))
        for key, value in self.dltPerm.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Repos Permission:')
        print("{:<20} {:<100}".format('Repo ID', 'Group Permission'))
        for key, value in self.repoPerm.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Tokens Permission:')
        print("{:<20} {:<100}".format('Token ID', 'Group Permission'))
        for key, value in self.tokenPerm.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Secret Scopes Permission:')
        print("{:<20} {:<100}".format('SecretScope ID', 'Group Permission'))
        for key, value in self.secretScopePerm.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Folder  Permission:')
        print("{:<20} {:<100}".format('Folder ID', 'Group Permission'))
        for key, value in self.folderPerm.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Notebook  Permission:')
        print("{:<20} {:<100}".format('Notebook ID', 'Group Permission'))
        for key, value in self.notebookPerm.items():print("{:<20} {:<100}".format(key, str(value)))
        print('TableACL  Permission:')
        for item in self.dataObjectsPerm:print(item)
        
      except Exception as e:
        print(f" Error getting group permission, {e}")
    def applyGroupPermission(self, groupList:list, level:str ):
      try:
        self.updateGroupEntitlements(self.groupEntitlements,level)
        self.updateGroupPermission('clusters',self.clusterPerm,level)
        self.updateGroupPermission('cluster-policies',self.clusterPolicyPerm,level)
        self.updateGroupPermission('sql/warehouses',self.warehousePerm,level)
        self.updateGroupPermission('instance-pools',self.instancePoolPerm,level)
        self.updateGroupPermission('jobs',self.jobPerm,level)
        self.updateGroupPermission('experiments',self.expPerm,level)
        self.updateGroupPermission('registered-models',self.modelPerm,level)
        self.updateGroupPermission('pipelines',self.dltPerm,level)
        self.updateGroupPermission('directories',self.folderPerm,level)
        self.updateGroupPermission('notebooks',self.notebookPerm,level)
        self.updateGroupPermission('repos',self.repoPerm,level)
        self.updateGroupPermission('authorization',self.tokenPerm,level)
        self.updateSecretPermission(self.secretScopePerm,level)
        self.updateGroup2Permission('dashboards',self.dashboardPerm,level)
        self.updateGroup2Permission('queries',self.queryPerm,level)
        self.updateGroup2Permission('alerts',self.alertPerm,level)
        self.updateGroupPermission('authorization',self.passwordPerm,level)
        self.updateDataObjectsPermission(self.dataObjectsPerm,self.groupRoles,level)
        self.updateGroupRoles(level)
      except Exception as e:
        print(f" Error applying group permission, {e}")
    def deleteGroups(self, groupL:list, mode:str):
      try:
        for g in groupL:
          if mode=="Original":
            gID=self.groupNameDict[g]
          else:
            gID=self.groupWSGNameDict[g]
          res=requests.delete(f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups/{gID}", headers=self.headers)
      except Exception as e:
        print(f" Error deleting groups , {e}")
        self.performInventory(self.groupL)
    def createBackupGroup(self, groupL:list, mode:str):
      try:
        self.performInventory()
        for g in self.groupL:
          memberList="{"
          for mem in self.groupMembers[self.groupNameDict[g]]:
            memberList+="\"value\":\""+mem+"\""
          memberList+="}"
          data={
                  "schemas": [ "urn:ietf:params:scim:schemas:core:2.0:Group" ],
                  "displayName": g+'_WSG',
                  "members": [
                      {
                        memberList
                      }
                    ]
              }
          res=requests.post(f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups", headers=self.headers, data=json.dumps(data))
          self.groupWSGList[res.json()["id"]]=g
          self.groupWSGNameDict[g]=res.json()["id"]
        self.applyGroupPermission(self.workspace_url, "Workspace")
        self.deleteWSGroup(self.workspace_url, groupL, "Original")
      except Exception as e:
        print(f" Error deleting groups , {e}")
        self.performInventory(self.groupL)
    
    def validateAccountGroup(self):
      try:
        res=requests.get(f"{self.workspace_url}/api/2.0/account/scim/v2/Groups", headers=self.headers)
        for grp in res.json()['Resources']:
          self.accountGroups[grp['displayName']]=grp['id']
        for g in self.groupL:
          if g not in self.accountGroups:
            print(f"group {g} is not present in account level, please add correct group and try again")
            return 1
      except Exception as e:
        print(f" Error validating account level group, {e}")
    def createAccountGroup(self):
      try:
        if self.validateAccountGroup(self.workspace_url, self.groupL)==1: return
        data={
                  "permissions": ["USER"]
              }
        for g in self.groupL:     
          res=requests.put(f"{self.workspace_url}/api/2.0/preview/permissionassignments/principals/{self.accountGroups[g]}", headers=self.headers, data=json.dumps(data))
          print(res.text)
        self.applyGroupPermission(self.workspace_url, "Account")
        self.deleteWSGroup(self.workspace_url, self.groupL, "Backup")

      except Exception as e:
        print(f" Error creating account level group, {e}")