from os import getgrouplist
import requests
import json, math
from pyspark.sql.functions import lit,col,column
from functools import reduce
from pyspark.sql import DataFrame, session

class GroupMigration:

    def __init__(self, groupL : list, cloud : str, account_id : str, workspace_url : str, pat : str, spark : session.SparkSession, userName : str, checkTableACL : False, autoGenerateList : bool = False):
        self.groupL=groupL
        self.cloud=cloud    
        self.workspace_url = workspace_url.rstrip("/")
        self.account_id=account_id
        self.token=pat
        self.headers={'Authorization': 'Bearer %s' % self.token}
        self.groupList={}
        self.accountGroups={}
        self.groupMembers={}
        self.groupEntitlements={}
        self.groupNameDict={}
        self.groupWSGList={} #map : temp group id => temp group name
        self.groupWSGNameDict={} #map : temp group name => temp group id
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
        self.userName=userName
        self.checkTableACL=checkTableACL
        
        #Check if we should automatically generate list, and do it immediately.
        #Implementers Note: Could change this section to a lazy calculation by setting groupL to nil or some sentinel value and adding checks before use.
        if(autoGenerateList) :
            print("autoGenerateList parameter is set to TRUE. Ignoring groupL parameter and instead will automatically generate list of migraiton groups.")
            self.groupL = self.findMigrationEligibleGroups()
        
        #Finish setting some params that depend on groupL
        if(len(self.groupL) == 0):
            raise Exception("Migration group list (groupL) is empty!")
        
        self.TempGroupNames=["db-temp-"+g for g in self.groupL]
        self.WorkspaceGroupNames=self.groupL
        
        print(f"Successfully initialized GroupMigration class with {len(self.groupL)} workspace-local groups to migrate. Groups to migrate:")
        for i, group in enumerate(self.groupL, start=1):
            print(f"{i}. {group}")
        print(f"Done listing {len(self.groupL)} groups to migrate.")
                    
    def findMigrationEligibleGroups(self):
        print("Begin automatic generation of all migration eligible groups.")
        try:
            print("Executing request to list workspace groups")
            res=requests.get(f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups", headers=self.headers)
            if res.status_code != 200:
                raise Exception(f'Bad status code. Expected: 200. Got: {res.status_code}')
                    
            resJson=res.json()

            allWsLocalGroups = [o["displayName"] for o in resJson["Resources"] if o['meta']['resourceType'] == "WorkspaceGroup" ]

            #Prune special groups.
            prune_groups = ["admins", "users"]
            allWsLocalGroups = [g for g in allWsLocalGroups if g not in prune_groups]

            allWsLocalGroups.sort()
            print(f"\nFound {len(allWsLocalGroups)} workspace local groups. Listing (alphabetical order): \n" + "\n".join(f"{i+1}. {name}" for i, name in enumerate(allWsLocalGroups)))

        except Exception as e:
            print(f'ERROR in retrieving workspace group list: {e}') 
            raise
        try:
            print("\nExecuting request to list account groups")
            res=requests.get(f"{self.workspace_url}/api/2.0/account/scim/v2/Groups", headers=self.headers)
            if res.status_code != 200:
                raise Exception(f'Bad status code. Expected: 200. Got: {res.status_code}')
            resJson2=res.json()
            allAccountGroups = [r['displayName'] for r in resJson2['Resources']]
            allAccountGroups.sort()

            # Get set intersection of both lists
            migration_eligible = list(set(allWsLocalGroups) & set(allAccountGroups))
            migration_eligible.sort()

            # Get list of items in allWsLocalGroups that are not in allAccountGroups
            not_in_account_groups = [group for group in allWsLocalGroups if group not in allAccountGroups]
            not_in_account_groups.sort()

            # Print count and membership of not_in_account_groups
            print(f"Unable to match {len(not_in_account_groups)} current workspace-local groups. No matching account level group with the same name found. These groups WILL NOT MIGRATE:")
            for i, group in enumerate(not_in_account_groups, start=1):
                print(f"{i}. {group} (WON'T MIGRATE)")

            if(len(migration_eligible) > 0):
                # Print count and membership of intersection
                print(f"\nFound {len(migration_eligible)} current workspace-local groups to account level groups. These groups WILL BE MIGRATED.")
                for i, group in enumerate(migration_eligible, start=1):
                    print(f"{i}. {group} (WILL MIGRATE)")
                print('')

                return migration_eligible
            else:
                print("There are no migration eligible groups. All existing workspace-local groups do not exist at the account level.\nNO MIGRATION WILL BE PERFORMED.")
                return []
        except Exception as e:
            print(f'ERROR in retrieving account group list : {e}')
            raise
        
    def validateWSGroup(self)->list:
        try:
            res=requests.get(f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups", headers=self.headers)
            resJson=res.json()
            for e in resJson['Resources']:
                if e['meta']['resourceType']=="Group" and e['displayName'] in self.groupL:
                  print(f"{e['displayName']} is a Account level group, please provide workspace group" )
                  return 0
            return 1
        except Exception as e:
            print(f'error in retrieving group objects : {e}')
            
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
                #print(groupList)
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
                    continue
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
                      continue
                  if len(entms)==0:
                    continue
                  if e['displayName'] in self.groupL:
                    groupRoles[e['id']]=entms   
            return [groupList, groupMembers, groupEntitlements, groupRoles]
        except Exception as e:
            print(f'error in retrieving group objects : {e}')


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
    def getACL3(self, acls:dict)->list:
        aclList=[]
        for acl in acls:
            try:
                aclList.append(list([acl['group_name'],acl['all_permissions'][0]['permission_level']]))
            except KeyError:
                continue
        aclList=[acl for acl in aclList if acl[0] in self.groupL]
        return aclList
    def getACL2(self, acls:dict)->list:
        aclList=[]
        for acl in acls:
            try:
                l=[]
                for k,v in acl.items():
                  l.append(v)
                aclList.append(l)
            except KeyError:
                continue
        for acl in aclList:
          if acl[0] in self.groupL:
            return aclList
        return {}
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
                    continue
                resCPermJson=resCPerm.json()            
                aclList=self.getACL(resCPermJson['access_control_list'])
                if len(aclList)==0:continue
                clusterPerm[clusterId]=aclList                
            return clusterPerm    
        except Exception as e:
            print(f'error in retrieving cluster permission: {e}')

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
                    continue
                resCPPermJson=resCPPerm.json()            
                aclList=self.getACL(resCPPermJson['access_control_list'])
                if len(aclList)==0:continue
                clusterPolicyPerm[policyid]=aclList                
            return clusterPolicyPerm
        except Exception as e:
            print(f'error in retrieving cluster policy permission: {e}')

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
                    continue
                resWPermJson=resWPerm.json()            
                aclList=self.getACL(resWPermJson['access_control_list'])                   
                if len(aclList)==0:continue
                warehousePerm[warehouseId]=aclList               
            return warehousePerm
        except Exception as e:
            print(f'error in retrieving warehouse permission: {e}')

    def getDashboardACL(self)-> dict:
        try:
            resD=requests.get(f"{self.workspace_url}/api/2.0/preview/sql/dashboards", headers=self.headers)
            resDJson=resD.json()
            pages=math.ceil(resDJson['count']/resDJson['page_size'])
            
            dashboardPerm={}
            #print(str(pages))
            for pg in range(1,pages+1):
                #print(str(pg))
                resD=requests.get(f"{self.workspace_url}/api/2.0/preview/sql/dashboards?page={str(pg)}", headers=self.headers)
                resDJson=resD.json()            
                for c in resDJson['results']:
                    dashboardId=c['id']
                    resDPerm=requests.get(f"{self.workspace_url}/api/2.0/preview/sql/permissions/dashboards/{dashboardId}", headers=self.headers)
                    if resDPerm.status_code==404:
                        print(f'feature not enabled for this tier')
                        continue
                    resDPermJson=resDPerm.json() 
                    aclList=resDPermJson['access_control_list']       
                    if len(aclList)==0:continue
                    for acl in aclList:
                      try:
                        if acl['group_name'] in self.groupL:
                          dashboardPerm[dashboardId]=aclList  
                          break
                      except KeyError:
                        continue              
            return dashboardPerm

        except Exception as e:
            print(f'error in retrieving dashboard permission: {e}')
            raise e
    def getQueriesACL(self)-> dict:
        try:
            resQ=requests.get(f"{self.workspace_url}/api/2.0/preview/sql/queries", headers=self.headers)
            resQJson=resQ.json()
            queryPerm={}
            pages=math.ceil(resQJson['count']/resQJson['page_size'])
            #print(str(pages))
            for pg in range(1,pages+1):
                #print(str(pg))
                resQ=requests.get(f"{self.workspace_url}/api/2.0/preview/sql/queries?page={str(pg)}", headers=self.headers)
                resQJson=resQ.json()    
                for c in resQJson['results']:
                    queryId=c['id']
                    resQPerm=requests.get(f"{self.workspace_url}/api/2.0/preview/sql/permissions/queries/{queryId}", headers=self.headers)
                    if resQPerm.status_code==404:
                        print(f'feature not enabled for this tier')
                        continue
                    resQPermJson=resQPerm.json() 
                    aclList=resQPermJson['access_control_list']  
                    if len(aclList)==0:continue
                    for acl in aclList:
                      try:
                        if acl['group_name'] in self.groupL:
                          queryPerm[queryId]=aclList  
                          break
                      except KeyError:
                        continue
            return queryPerm

        except Exception as e:
            print(f'error in retrieving query permission: {e}')
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
                    continue
                resAPermJson=resAPerm.json() 
                aclList=resAPermJson['access_control_list']               
                if len(aclList)==0:continue
                for acl in aclList:
                  try:
                    if acl['group_name'] in self.groupL:
                      alertPerm[alertId]=aclList  
                      break
                  except KeyError:
                    continue             
            return alertPerm

        except Exception as e:
            print(f'error in retrieving alerts permission: {e}')

    def getPasswordACL(self)-> dict:
        try:
            if self.cloud!='AWS':
              return
            resP=requests.get(f"{self.workspace_url}/api/2.0/preview/permissions/authorization/passwords", headers=self.headers)
            resPJson=resP.json()
            if len(resPJson)<3:
                print('No password acls defined.')
                return {}
              
            passwordPerm={}
            passwordPerm['passwords']=self.getACL(resPJson['access_control_list'])            
            return passwordPerm
        except Exception as e:
            print(f'error in retrieving password  permission: {e}')

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
                    continue
                resIPPermJson=resIPPerm.json()   
                aclList=self.getACL(resIPPermJson['access_control_list'])            
                if len(aclList)==0:continue
                instancePoolPerm[instancePID]=aclList                
            return instancePoolPerm
        except Exception as e:
            print(f'error in retrieving Instance Pool permission: {e}') 

    def getJobACL(self)-> dict: 
        try:
            jobPerm={}
            offset=0
            limit=25
            while True:
               # print(str(offset))
                resJob=requests.get(f"{self.workspace_url}/api/2.1/jobs/list?limit={str(limit)}&offset={str(offset)}", headers=self.headers)
                resJobJson=resJob.json()
                if resJobJson['has_more']==False and len(resJobJson)==1:
                    print('No jobs available')
                    break  
                for c in resJobJson['jobs']:
                    jobID=c['job_id']
                    resJobPerm=requests.get(f"{self.workspace_url}/api/2.0/permissions/jobs/{jobID}", headers=self.headers)
                    if resJobPerm.status_code==404:
                        print(f'feature not enabled for this tier')
                        continue
                    resJobPermJson=resJobPerm.json()   
                    aclList=self.getACL(resJobPermJson['access_control_list'])                
                    if len(aclList)==0:continue
                    jobPerm[jobID]=aclList    
                if resJobJson['has_more']==False:
                    break  
                offset+=25
            return jobPerm
        except Exception as e:
            print(f'error in retrieving job permission: {e}')
    def getExperimentACL(self)-> dict:
        try:
            nextPageToken='' 
            expPerm={}
            while True:
                data={}
                data={'max_results':100}
                if nextPageToken!="":    
                    data={'page_token':nextPageToken, 'max_results':'100'}
                
                resExp=requests.get(f"{self.workspace_url}/api/2.0/mlflow/experiments/list", headers=self.headers,data=json.dumps(data))
                resExpJson=resExp.json()
                if len(resExpJson)==0:
                    print('No experiments available')
                    return {}
                for c in resExpJson['experiments']:                    
                    expID=c['experiment_id']
                    resExpPerm=requests.get(f"{self.workspace_url}/api/2.0/permissions/experiments/{expID}", headers=self.headers)
                    if resExpPerm.status_code==404:
                        print(f'feature not enabled for this tier')
                        continue
                    resExpPermJson=resExpPerm.json()   
                    if resExpPerm.status_code!=200:
                      print(f'unable to get permission for experiment {expID}')
                      continue 
                    aclList=self.getACL(resExpPermJson['access_control_list'])                
                    if len(aclList)==0:continue
                    
                    expPerm[expID]=aclList  
                try:
                    nextPageToken=resExpJson['next_page_token']
                    #break
                except KeyError:
                    break
            return expPerm
        except Exception as e:
            print(f'error in retrieving experiment permission: {e}')
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
                        continue
                    resModelPermJson=resModelPerm.json()   
                    aclList=self.getACL(resModelPermJson['access_control_list'])                
                    if len(aclList)==0:continue
                    modelPerm[modelID]=aclList  
                try:
                    nextPageToken=resModelJson['next_page_token']
                    #break
                except KeyError:
                    break
            return modelPerm
        except Exception as e:
            print(f'error in retrieving model permission: {e}')
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
                        continue
                    resDltPermJson=resDltPerm.json()   
                    aclList=self.getACL(resDltPermJson['access_control_list'])
                    if len(aclList)==0:continue
                    dltPerm[dltID]=aclList  
                try:
                    nextPageToken=resDltJson['next_page_token']
                    #break
                except KeyError:
                    break

            return dltPerm
        except Exception as e:
            print(f'error in retrieving dlt pipelines permission: {e}')

    def getFolderList(self, path:str)-> dict:
        try:
            data={'path':path}
            print(f"Requesting file list for path: {path}")
            resFolder=requests.get(f"{self.workspace_url}/api/2.0/workspace/list", headers=self.headers, data=json.dumps(data))
            resFolderJson=resFolder.json()
            folderPerm={}
            if len(resFolderJson)==0:
                return
            for c in resFolderJson['objects']:
                if c['object_type']=="DIRECTORY" and c['path'].startswith('/Repos') == False and c['path'].startswith('/Shared') == False:
                    #if c['path'][-4:]==".com": print(c['path'])
                    self.folderList[c['object_id']]=c['path']
                    self.getFolderList(c['path'])
                elif c['object_type']=="NOTEBOOK" and c['path'].startswith('/Repos') == False and c['path'].startswith('/Shared') == False:
                    self.notebookList[c['object_id']]=c['path']
        except Exception as e:
            print(f'error in retriving directory details: {e}')


    def getFoldersNotebookACL(self)-> list:
        try:
            self.getFolderList("/")
            
            folderPerm={}
            for id,path in self.folderList.items():
                #Skip user's Trash folders -- causes ignorable error message on azure
                if path.startswith('/Users') and path.endswith('/Trash'):
                    continue

                resFolderPerm=requests.get(f"{self.workspace_url}/api/2.0/permissions/directories/{id}", headers=self.headers)
                if resFolderPerm.status_code==404:
                    print(f'feature not enabled for this tier')
                    continue
                if resFolderPerm.status_code==403:
                    print('Error retrieving permission for '+path+ ' '+ resFolderPerm.json()['message'])
                    continue
                resFolderPermJson=resFolderPerm.json()   
                try:
                  aclList=self.getACL(resFolderPermJson['access_control_list'])   
                except Exception as e:
                  print(f'error in retriving folder details: {e}')
                  #print('id: ',id)
                  #print('path: ',path)
                if len(aclList)==0:continue
                folderPerm[id]=aclList

            notebookPerm={}
            for id,path in self.notebookList.items():
                resNotebookPerm=requests.get(f"{self.workspace_url}/api/2.0/permissions/notebooks/{id}", headers=self.headers)
                if resNotebookPerm.status_code==404:
                    print(f'feature not enabled for this tier')
                    continue
                if resNotebookPerm.status_code==403:
                    print('Error retrieving permission for '+path+ ' '+ resNotebookPerm.json()['message'])
                    continue
                resNotebookPermJson=resNotebookPerm.json()   
                try:
                  aclList=self.getACL(resNotebookPermJson['access_control_list'])
                except Exception as e:
                  print(f'error in retriving notebook details: {e}')
                if len(aclList)==0:continue
                notebookPerm[id]=aclList  
            return folderPerm, notebookPerm
        except Exception as e:
            print(f'error in retrieving folder permission: {e}')

    def getRepoACL(self)-> dict:
        try:
            nextPageToken=''
            repoPerm={}
            while True:
                data={}
                data={'max_results':20}
                if nextPageToken!="":    
                    data={'next_page_token':nextPageToken}
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
                        continue
                    resRepoPermJson=resRepoPerm.json()   
                    aclList=self.getACL3(resRepoPermJson['access_control_list'])
                    if len(aclList)==0:continue
                    repoPerm[repoID]=aclList  
                try:
                    nextPageToken=resRepoJson['next_page_token']
                except KeyError:
                    break

            return repoPerm
        except Exception as e:
            print(f'error in retrieving repos permission: {e}')
    def getTokenACL(self)-> dict:
        try:
            tokenPerm = {}
            resTokenPerm=requests.get(f"{self.workspace_url}/api/2.0/preview/permissions/authorization/tokens", headers=self.headers)
            if resTokenPerm.status_code==404:
                print(f'feature not enabled for this tier')
                return {}
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
            print(f'error in retrieving Token permission: {e}')
            return {}
    def getSecretScoppeACL(self)-> dict:
        try:

            resSScope=requests.get(f"{self.workspace_url}/api/2.0/secrets/scopes/list", headers=self.headers)
            resSScopeJson=resSScope.json()
            if len(resSScopeJson)==0:
                raise Exception('No secret scopes defined.')

            secretScopePerm={}
            for c in resSScopeJson['scopes']:
                scopeName=c['name']
                data={'scope':scopeName}
                resSSPerm=requests.get(f"{self.workspace_url}/api/2.0/secrets/acls/list/", headers=self.headers, data=json.dumps(data))  
                
                if resSSPerm.status_code==404:
                    print(f'feature not enabled for this tier')
                    continue
                if resSSPerm.status_code!=200:
                    print(f'Error retrieving ACL for Secret Scope: {scopeName}. HTTP Status Code {resSSPerm.status_code}')
                    continue

                resSSPermJson=resSSPerm.json()
                if not 'items' in resSSPermJson:
                    #print(f'ACL for Secret Scope  {scopeName} missing "items" key. Contents:\n{resSSPermJson}\nSkipping...')
                    #This seems to be expected behaviour if there are no ACLs, silently ignore
                    continue

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
                  groupId=self.groupWSGNameDict["db-temp-"+self.groupList[group_id]]
                else:
                  groupId=self.accountGroups[self.groupList[group_id][8:]]
                #print(groupId)
                for e in etl:
                    entitlementList.append({"value":e})
                entitlements = {
                                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                                "Operations": [{"op": "add",
                                            "path": "entitlements",
                                            "value": entitlementList}]
                            }
                resPatch=requests.patch(f'{self.workspace_url}/api/2.0/preview/scim/v2/Groups/{groupId}', headers=self.headers, data=json.dumps(entitlements))
        except Exception as e:
            print(f'error applying entitiement for group id: {group_id}.')

    def updateGroupRoles(self, level:str):
        try:
            for group_id, roles in self.groupRoles.items():
                roleList=[]
                if level=="Workspace":
                  groupId=self.groupWSGNameDict["db-temp-"+self.groupList[group_id]]
                else:
                  groupId=self.accountGroups[self.groupList[group_id][8:]]
                for e in roles:
                    roleList.append({"value":e})
                instanceProfileRoles = {
                                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                                "Operations": [{"op": "add",
                                            "path": "roles",
                                            "value": roleList}]
                            }
                resPatch=requests.patch(f'{self.workspace_url}/api/2.0/preview/scim/v2/Groups/{groupId}', headers=self.headers, data=json.dumps(instanceProfileRoles))
        except Exception as e:
            print(f'error applying role for group id: {group_id}.')

    def updateGroupPermission(self, object:str, groupPermission : dict, level:str):
        try:
          suffix=""
          for object_id,aclList in groupPermission.items(): 
              dataAcl=[]
              for  acl in aclList:
                  if level=="Workspace":
                    gName="db-temp-"+acl[0]
                  elif level=="Account":
                    gName=acl[0][8:]
                  dataAcl.append({"group_name":gName,"permission_level":acl[1]})
              data={"access_control_list":dataAcl}
              resAppPerm=requests.patch(f"{self.workspace_url}/api/2.0/preview/permissions/{object}/{object_id}", headers=self.headers, data=json.dumps(data))
        except Exception as e:
            print(f'Error setting permission for {object} {object_id}. {e} ')
    def updateGroup2Permission(self, object:str, groupPermission : dict, level:str):
        try:
          for object_id,aclList in groupPermission.items(): 
              dataAcl=[]
              for acl in aclList:
                try:
                  gName=acl['group_name']
                  if level=="Workspace":
                    if acl['group_name'] in self.WorkspaceGroupNames:
                      gName="db-temp-"+acl['group_name']
                  elif level=="Account":
                    if acl['group_name'] in self.TempGroupNames:
                      gName=acl['group_name'][8:]
                  else:
                    gName=acl['group_name']
                  acl['group_name']=gName
                  dataAcl.append(acl)
                except KeyError:
                  dataAcl.append(acl)
                  continue  
              dataAcl.append({"user_name": self.userName,"permission_level": "CAN_MANAGE"})
              data={"access_control_list":dataAcl}
              resAppPerm=requests.post(f"{self.workspace_url}/api/2.0/preview/sql/permissions/{object}/{object_id}", headers=self.headers, data=json.dumps(data))
        except Exception as e:
            print(f'Error setting permission for {object} {object_id}. {e} ')
    def updateSecretPermission(self, secretPermission : dict, level:str):
        try:
            suffix=""
            for object_id,aclList in secretPermission.items(): 
                dataAcl=[]
                for  acl in aclList:
                    if level=="Workspace":
                      gName="db-temp-"+acl[0]
                    elif level=="Account":
                      gName=acl[0][8:]
                    data={"scope":object_id, "principal":gName, "permission":acl[1]}
                    resAppPerm=requests.post(f"{self.workspace_url}/api/2.0/secrets/acls/put", headers=self.headers, data=json.dumps(data))
        except Exception as e:
            print(f'Error setting permission for scope {object_id}. {e} ')
    def getDataObjectsACL(self)-> list:
      dbs = self.spark.sql("show databases")
      aclList = []
      try:

        for db in dbs.collect():
          databaseName = ""

          databaseName = db.databaseName
          #databaseName = 'default'

          # append the database df to the list
          df=(self.spark.sql("SHOW GRANT ON DATABASE {}".format(databaseName))
                         .withColumn("ObjectKey", lit(databaseName))
                         .withColumn("ObjectType", lit("DATABASE"))
                         .filter(col("ActionType")!="OWN")
             )
          aclList=df.collect()
          tables = self.spark.sql("show tables in {}".format(databaseName)).filter(col("isTemporary") == False)
          for table in tables.collect():
            try:
              #print(table)
              #if table.tableName=='testtable': continue
              dft=(self.spark.sql("show grant on table {}.`{}`".format(table.database, table.tableName))
                             .withColumn("ObjectKey", lit("`" + table.database + "`.`" + table.tableName + "`"))
                             .withColumn("ObjectType", lit("TABLE"))
                            )
              aclList+=dft.collect()
            except Exception as e:
              print(f'error retriving acl for table {table.tableName}.')
            #break

          views = self.spark.sql("show views in {}".format(databaseName)).filter(col("isTemporary") == False)
          for view in views.collect():
            try:
              
              dft=(self.spark.sql("show grant on view {}.`{}`".format(view.namespace, view.viewName))
                             .withColumn("ObjectKey", lit("`" + view.namespace + "`.`" + view.viewName + "`"))
                             .withColumn("ObjectType", lit("VIEW"))
                            )
              aclList+=dft.collect()
            except Exception as e:
              print(f'error retriving acl for view {view.viewName}.')
            #break

          functions = self.spark.sql("show functions in {}".format(databaseName)).filter(col("function").startswith('spark_catalog.'+databaseName+"."))
          for function in functions.collect():
            try:
              
              dft=(self.spark.sql("show grant on function `{}`".format( function.function))
                             .withColumn("ObjectKey", lit("`" + function.function + "`"))
                             .withColumn("ObjectType", lit("FUNCTION"))
                            )
              aclList+=dft.collect()
            except Exception as e:
              print(f'error retriving acl for function {function.function}.')
            #break
          break
        dft=(self.spark.sql("show grant on any file ")
                       .withColumn("ObjectKey", lit("ANY FILE"))
                       .withColumn("ObjectType", lit(""))
                      )
        aclList+=dft.collect()
        aclFinalList=[acl for acl in aclList if acl[0] in self.groupL]
      except Exception as e:
        print(f'Error retriving table acl object permission {e}')
      return aclFinalList
    def updateDataObjectsPermission(self, aclList : list, level:str):
        try:
            suffix=""
            for acl in aclList: 
                if acl.ObjectType!="DATABASE" and acl.ActionType=="USAGE": continue
                if level=="Workspace":
                  gName="db-temp-"+acl.Principal
                elif level=="Account":
                  gName=acl.Principal[8:]
                aclQuery = "GRANT {} ON {} {} TO `{}`".format(acl.ActionType, acl.ObjectType, acl.ObjectKey, gName)
                #print(aclQuery)
                self.spark.sql(aclQuery)
        except Exception as e:
            print(f'Error setting permission, {e} ')
    def performInventory(self, mode : str):
      try:
        if mode=="Workspace":
          self.groupL=self.WorkspaceGroupNames
        elif mode=="Account":
          self.groupL=self.TempGroupNames   
        
        print(f'Performing group inventory with mode={mode}')
        res=requests.get(f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups", headers=self.headers)
        self.groupList, self.groupMembers, self.groupEntitlements, self.groupRoles=self.getGroupObjects()
        groupNames=[v for k,v in self.groupList.items()]
        for k,v in self.groupList.items():
          self.groupNameDict[v]=k    
        if self.cloud=="AWS":
          print('performing password inventory')
          self.passwordPerm= self.getPasswordACL()
        print('performing cluster inventory')
        self.clusterPerm=self.getClusterACL()
        print('performing cluster policy inventory')
        self.clusterPolicyPerm=self.getClusterPolicyACL()
        print('performing warehouse inventory')
        self.warehousePerm=self.getWarehouseACL()
        print('performing dashboard inventory')
        self.dashboardPerm=self.getDashboardACL() # 5 mins
        print('performing queries inventory')
        self.queryPerm=self.getQueriesACL()
        print('performing alerts inventory')
        self.alertPerm=self.getAlertsACL()
        print('performing instance pools inventory')
        self.instancePoolPerm=self.getPoolACL()
        print('performing jobs inventory')
        self.jobPerm=self.getJobACL() #33 mins
        print('performing experiments inventory')
        self.expPerm=self.getExperimentACL()
        print('performing registered models inventory')
        self.modelPerm=self.getModelACL()
        print('performing DLT inventory')
        self.dltPerm=self.getDLTACL()
        print('performing folders and notebook inventory')
        self.folderPerm, self.notebookPerm=self.getFoldersNotebookACL()
        print('performing repos inventory')
        self.repoPerm=self.getRepoACL()
        print('performing token inventory')
        self.tokenPerm=self.getTokenACL()
        print('performing secret scope inventory')
        self.secretScopePerm=self.getSecretScoppeACL()
        if self.checkTableACL==True:
          print('performing Tabel ACL object inventory')
          self.dataObjectsPerm=self.getDataObjectsACL()

      except Exception as e:
        print(f" Error creating group inventory, {e}")
    
    def printInventory(self):
        print('Displaying Inventory Results -- ACLs of selected groups:')
        print('Group List:')
        print("{:<20} {:<10}".format('Group ID', 'Group Name'))
        for key, value in self.groupList.items():print("{:<20} {:<10}".format(key, value))
        print('Group Members:')
        print("{:<20} {:<100}".format('Group ID', 'Group Member'))
        for key, value in self.groupMembers.items():print("{:<20} {:<100}".format(key, str(value)))
        print('Group Entitlements:')
        print("{:<20} {:<100}".format('Group ID', 'Group Entitlements'))
        for key, value in self.groupEntitlements.items():print("{:<20} {:<100}".format(key, str(value)))
        if self.cloud=="AWS":
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
        if self.checkTableACL==True:
          print('TableACL  Permission:')
          for item in self.dataObjectsPerm:print(item)

    def dryRun(self):
        self.performInventory('Workspace')        
        self.printInventory()

    def applyGroupPermission(self, level:str ):
      try:
        
        print('applying group entitlement permissions')
        self.updateGroupEntitlements(self.groupEntitlements,level)
        print('applying cluster permissions')
        self.updateGroupPermission('clusters',self.clusterPerm,level)
        print('applying cluster policy permissions')
        self.updateGroupPermission('cluster-policies',self.clusterPolicyPerm,level)
        print('applying warehouse permissions')
        self.updateGroupPermission('sql/warehouses',self.warehousePerm,level)
        print('applying instance pool permissions')
        self.updateGroupPermission('instance-pools',self.instancePoolPerm,level)
        print('applying jobs permissions')
        self.updateGroupPermission('jobs',self.jobPerm,level)
        print('applying experiments permissions')
        self.updateGroupPermission('experiments',self.expPerm,level)
        print('applying model permissions')
        self.updateGroupPermission('registered-models',self.modelPerm,level)
        print('applying DLT permissions')
        self.updateGroupPermission('pipelines',self.dltPerm,level)
        print('applying folders permissions')
        self.updateGroupPermission('directories',self.folderPerm,level)
        print('applying notebooks permissions')
        self.updateGroupPermission('notebooks',self.notebookPerm,level)
        print('applying repos permissions')
        self.updateGroupPermission('repos',self.repoPerm,level)
        print('applying token permissions')
        self.updateGroupPermission('authorization',self.tokenPerm,level)
        print('applying secret scope permissions')
        self.updateSecretPermission(self.secretScopePerm,level)
        print('applying dashboard permissions')
        self.updateGroup2Permission('dashboards',self.dashboardPerm,level)
        print('applying query permissions')
        self.updateGroup2Permission('queries',self.queryPerm,level)
        print('applying alerts permissions')
        self.updateGroup2Permission('alerts',self.alertPerm,level)
        if self.cloud=="AWS":
          print('applying password permissions')
          self.updateGroupPermission('authorization',self.passwordPerm,level)
          print('applying instance profile permissions')
          self.updateGroupRoles(level)
        if self.checkTableACL==True:
          print('applying table acl object permissions')
          self.updateDataObjectsPermission(self.dataObjectsPerm,level)
        
      except Exception as e:
        print(f" Error applying group permission, {e}")
    def validateTempWSGroup(self)->list:
        try:
            res=requests.get(f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups", headers=self.headers)
            resJson=res.json()
            WSGGroup=[e['displayName'] for e in resJson['Resources'] if e['meta']['resourceType']=="WorkspaceGroup"]
            for g in self.groupL:
              if "db-temp-"+g not in WSGGroup:
                print(f'temp workspace group db-temp-{g} not present, please check')
                return 0                
            return 1
        except Exception as e:
            print(f'error validating WS group objects : {e}')
    def deleteGroups(self, mode:str):
      try:
        for g in self.groupL:
          gID=self.groupNameDict[g]
          res=requests.delete(f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups/{gID}", headers=self.headers)
      except Exception as e:
        print(f" Error deleting groups , {e}")

    def createBackupGroup(self):
      try:
        if self.validateWSGroup()==0: return
        self.performInventory('Workspace')

        for g in self.groupL:
          memberList=[]
          if self.groupNameDict[g] in self.groupMembers:
            for mem in self.groupMembers[self.groupNameDict[g]]:
              memberList.append({"value":mem[1]})
          data={
                  "schemas": [ "urn:ietf:params:scim:schemas:core:2.0:Group" ],
                  "displayName": "db-temp-"+g,
                  "members": memberList
              }
          res=requests.post(f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups", headers=self.headers, data=json.dumps(data))
          if res.status_code == 409:
            print(f'group with name "db-temp-"{g} already present, please delete and try again.')
            continue                       
          self.groupWSGList[res.json()["id"]]="db-temp-"+g
          self.groupWSGNameDict["db-temp-"+g]=res.json()["id"]
        self.applyGroupPermission("Workspace")
        #self.deleteGroups("Workspace")
      except Exception as e:
        print(f" Error creating backup groups , {e}")
    
    def validateAccountGroup(self):
      try:
        res=requests.get(f"{self.workspace_url}/api/2.0/account/scim/v2/Groups", headers=self.headers)
        for grp in res.json()['Resources']:
          self.accountGroups[grp['displayName']]=grp['id']
        for g in self.WorkspaceGroupNames:
          if g not in self.accountGroups:
            print(f"group {g} is not present in account level, please add correct group and try again")
            return 1
        return 0
      except Exception as e:
        print(f" Error validating account level group, {e}")
    def createAccountGroup(self):
      try:
        if self.validateAccountGroup()==1: return
        if self.validateTempWSGroup()==0: return
        self.performInventory('Account')
        data={
                  "permissions": ["USER"]
              }
        for g in self.WorkspaceGroupNames:     
          res=requests.put(f"{self.workspace_url}/api/2.0/preview/permissionassignments/principals/{self.accountGroups[g]}", headers=self.headers, data=json.dumps(data))
        self.applyGroupPermission("Account")
        #self.deleteGroups("Account")

      except Exception as e:
        print(f" Error creating account level group, {e}")