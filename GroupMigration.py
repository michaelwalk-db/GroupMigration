from os import getgrouplist
import requests
import json, math

workspace_url = 'https://e2-demo-field-eng.cloud.databricks.com'
workspace_url = 'https://adb-5932067186463130.10.azuredatabricks.net'

token='dapi9f43cfa7187dd43d52a3f5d515436cdb'
token='dapi61015f016e07c4f4628f0b3cabab22ca'
headers={'Authorization': 'Bearer %s' % token}
res=requests.get(f"{workspace_url}/api/2.0/preview/scim/v2/Groups", headers=headers)
#res=requests.get(f"{workspace_url}/api/2.0/clusters/list", headers=headers)
groupList=[]
groupMembers={}
groupEntitlements={}
groupRoles={}
resJson=res.json()
folderList={}
notebookList={}


def getGroupList(resJson:json)->list:
    try:
        for e in resJson['Resources']:
            groupList.append(list([e['displayName'],e['id']]))

        return groupList
    except Exception as e:
        print(f'error in retriveing groupList : {e}')
        
def getGroupMembers(resJson:json)->dict:
    try:
        groupMembers={}
        for e in resJson['Resources']:
            members=[]
            try:
                for mem in e['members']:
                    members.append(list([mem['display'],mem['value']]))
            except KeyError:
                continue
            groupMembers[e['id']]=members
        return groupMembers
    except Exception as e:
        print(f'error in retriveing groupMembers : {e}')

def getGroupEntitlements(resJson:json)->dict:
    try:
        groupEntitlements={}
        for e in resJson['Resources']:
            entms=[]
            try:
                for ent in e['entitlements']:
                    entms.append(ent['value'])
            except:
                pass
            groupEntitlements[e['id']]=entms
        return groupEntitlements
    except Exception as e:
        print(f'error in retriveing group entitlements : {e}')

def getGroupRoles(resJson:json)->dict:
    try:
        groupRoles={}
        for e in resJson['Resources']:
            entms=[]
            try:
                for ent in e['roles']:
                    entms.append(ent['value'])
            except:
                pass
            if len(entms)==0:continue
            groupRoles[e['id']]=entms
        return groupRoles
    except Exception as e:
        print(f'error in retriveing group roles : {e}')

def getACL(acls:dict)->list:
    aclList=[]
    for acl in acls:
        try:
            if acl['all_permissions'][0]['inherited']==True:continue
            aclList.append(list([acl['group_name'],acl['all_permissions'][0]['permission_level']]))
        except KeyError:
            continue
    return aclList


def getClusterACL(workspace_url:str)-> dict:
    try:

        resC=requests.get(f"{workspace_url}/api/2.0/clusters/list", headers=headers)
        resCJson=resC.json()
        clusterPerm={}
        for c in resCJson['clusters']:
            clusterId=c['cluster_id']
            resCPerm=requests.get(f"{workspace_url}/api/2.0/preview/permissions/clusters/{clusterId}", headers=headers)
            if resCPerm.status_code==404:
                print(f'cluster ACL not enabled for the cluster: {clusterId}')
                pass
            resCPermJson=resCPerm.json()            
            aclList=getACL(resCPermJson['access_control_list'])
            if len(aclList)==0:continue
            clusterPerm[clusterId]=aclList                
        return clusterPerm    
    except Exception as e:
        print(f'error in retriveing cluster permission: {e}')

def getClusterPolicyACL(workspace_url:str)-> dict:
    try:
        resCP=requests.get(f"{workspace_url}/api/2.0/policies/clusters/list", headers=headers)
        resCPJson=resCP.json()
        if resCPJson['total_count']==0:
            print('No cluster policies defined.')
            return {}
        clusterPolicyPerm={}
        for c in resCPJson['policies']:
            policyid=c['policy_id']
            resCPPerm=requests.get(f"{workspace_url}/api/2.0/preview/permissions/cluster-policies/{policyid}", headers=headers)
            if resCPPerm.status_code==404:
                print(f'cluster policy feature is not enabled for this tier.')
                pass
            resCPPermJson=resCPPerm.json()            
            aclList=getACL(resCPPermJson['access_control_list'])
            if len(aclList)==0:continue
            clusterPolicyPerm[policyid]=aclList                
        return clusterPolicyPerm
    except Exception as e:
        print(f'error in retriveing cluster policy permission: {e}')

def getWarehouseACL(workspace_url:str)-> dict:
    try:
        resW=requests.get(f"{workspace_url}/api/2.0/sql/warehouses", headers=headers)
        resWJson=resW.json()
        warehousePerm={}
        for c in resWJson['warehouses']:
            warehouseId=c['id']
            resWPerm=requests.get(f"{workspace_url}/api/2.0/preview/permissions/sql/warehouses/{warehouseId}", headers=headers)
            if resWPerm.status_code==404:
                print(f'feature not enabled for this tier')
                pass
            resWPermJson=resWPerm.json()            
            aclList=getACL(resWPermJson['access_control_list'])                   
            if len(aclList)==0:continue
            warehousePerm[warehouseId]=aclList               
        return warehousePerm
    except Exception as e:
        print(f'error in retriveing warehouse permission: {e}')

def getDashboardACL(workspace_url:str)-> dict:
    try:
        resD=requests.get(f"{workspace_url}/api/2.0/preview/sql/dashboards", headers=headers)
        resDJson=resD.json()
        pages=math.ceil(resDJson['count']/resDJson['page_size'])
        dashboardPerm={}
        for pg in range(1,pages+1):
            resD=requests.get(f"{workspace_url}/api/2.0/preview/sql/dashboards?page={str(pg)}", headers=headers)
            resDJson=resD.json()            
            for c in resDJson['results']:
                dashboardId=c['id']
                resDPerm=requests.get(f"{workspace_url}/api/2.0/preview/sql/permissions/dashboards/{dashboardId}", headers=headers)
                if resDPerm.status_code==404:
                    print(f'feature not enabled for this tier')
                    pass
                resDPermJson=resDPerm.json() 
                aclList=resDPermJson['access_control_list']        
                if len(aclList)==0:continue
                dashboardPerm[dashboardId]=aclList               
        return dashboardPerm
        
    except Exception as e:
        print(f'error in retriveing dashboard permission: {e}')
def getQueriesACL(workspace_url:str)-> dict:
    try:
        resQ=requests.get(f"{workspace_url}/api/2.0/preview/sql/queries", headers=headers)
        resQJson=resQ.json()
        queryPerm={}
        pages=math.ceil(resQJson['count']/resQJson['page_size'])
        for pg in range(1,pages+1):
            resQ=requests.get(f"{workspace_url}/api/2.0/preview/sql/queries?page={str(pg)}", headers=headers)
            resQJson=resQ.json()            
            for c in resQJson['results']:
                queryId=c['id']
                resQPerm=requests.get(f"{workspace_url}/api/2.0/preview/sql/permissions/queries/{queryId}", headers=headers)
                if resQPerm.status_code==404:
                    print(f'feature not enabled for this tier')
                    pass
                resQPermJson=resQPerm.json() 
                aclList=resQPermJson['access_control_list']                  
                if len(aclList)==0:continue
                queryPerm[queryId]=aclList               
        return queryPerm
        
    except Exception as e:
        print(f'error in retriveing query permission: {e}')
def getAlertsACL(workspace_url:str)-> dict:
    try:
        resA=requests.get(f"{workspace_url}/api/2.0/preview/sql/alerts", headers=headers)
        resAJson=resA.json()
        alertPerm={}
        for c in resAJson:
            alertId=c['id']
            resAPerm=requests.get(f"{workspace_url}/api/2.0/preview/sql/permissions/alerts/{alertId}", headers=headers)
            if resAPerm.status_code==404:
                print(f'feature not enabled for this tier')
                pass
            resAPermJson=resAPerm.json() 
            aclList=resAPermJson['access_control_list']                 
            if len(aclList)==0:continue
            alertPerm[alertId]=aclList               
        return alertPerm
        
    except Exception as e:
        print(f'error in retriveing alerts permission: {e}')

def getPasswordACL(workspace_url:str)-> dict:
    try:
        resP=requests.get(f"{workspace_url}/api/2.0/preview/permissions/authorization/passwords", headers=headers)
        resPJson=resP.json()
        if len(resPJson)==0:
            print('No password acls defined.')
            return {}
        passwordPerm={}
        passwordPerm['passwords']=getACL(resPJson['access_control_list'])            
        return passwordPerm
    except Exception as e:
        print(f'error in retriveing password  permission: {e}')

def getPoolACL(workspace_url:str)-> dict:
    try:
        resIP=requests.get(f"{workspace_url}/api/2.0/instance-pools/list", headers=headers)
        resIPJson=resIP.json()
        if len(resIPJson)==0:
            print('No Instance Pools defined.')
            return {}
        instancePoolPerm={}
        for c in resIPJson['instance_pools']:
            instancePID=c['instance_pool_id']
            resIPPerm=requests.get(f"{workspace_url}/api/2.0/preview/permissions/instance-pools/{instancePID}", headers=headers)
            if resIPPerm.status_code==404:
                print(f'feature not enabled for this tier')
                pass
            resIPPermJson=resIPPerm.json()   
            aclList=getACL(resIPPermJson['access_control_list'])            
            if len(aclList)==0:continue
            instancePoolPerm[instancePID]=aclList                
        return instancePoolPerm
    except Exception as e:
        print(f'error in retriveing Instance Pool permission: {e}')

def getJobACL(workspace_url:str)-> dict:
    try:
        jobPerm={}
        while True:
            resJob=requests.get(f"{workspace_url}/api/2.1/jobs/list", headers=headers)
            resJobJson=resJob.json()
            if resJob.text=="{\"has_more\":false}":
                print('No jobs available')
                return {}
            for c in resJobJson['jobs']:
                jobID=c['job_id']
                resJobPerm=requests.get(f"{workspace_url}/api/2.0/permissions/jobs/{jobID}", headers=headers)
                if resJobPerm.status_code==404:
                    print(f'feature not enabled for this tier')
                    pass
                resJobPermJson=resJobPerm.json()   
                aclList=getACL(resJobPermJson['access_control_list'])                
                if len(aclList)==0:continue
                jobPerm[jobID]=aclList    
            if resJobJson['has_more']==False:
                break    
        return jobPerm
    except Exception as e:
        print(f'error in retriveing job permission: {e}')
def getExperimentACL(workspace_url:str)-> dict:
    try:
        nextPageToken=''
        expPerm={}
        while True:
            data={}
            if nextPageToken!="":    
                data={'page_token':nextPageToken}
            resExp=requests.get(f"{workspace_url}/api/2.0/mlflow/experiments/list", headers=headers,data=json.dumps(data))
            resExpJson=resExp.json()
            if len(resExpJson)==0:
                print('No experiments available')
                return {}
            for c in resExpJson['experiments']:
                expID=c['experiment_id']
                resExpPerm=requests.get(f"{workspace_url}/api/2.0/permissions/experiments/{expID}", headers=headers)
                if resExpPerm.status_code==404:
                    print(f'feature not enabled for this tier')
                    pass
                resExpPermJson=resExpPerm.json()   
                aclList=getACL(resExpPermJson['access_control_list'])                
                if len(aclList)==0:continue
                expPerm[expID]=aclList  
            try:
                nextPageToken=resExpJson['next_page_token']
            except KeyError:
                break
        return expPerm
    except Exception as e:
        print(f'error in retriveing experiment permission: {e}')
def getModelACL(workspace_url:str)-> dict:
    try:
        nextPageToken=''
        expPerm={}
        while True:    
            data={}
            if nextPageToken!="":    
                data={'page_token':nextPageToken}    
            resModel=requests.get(f"{workspace_url}/api/2.0/mlflow/registered-models/list", headers=headers,data=json.dumps(data))
            resModelJson=resModel.json()
            if len(resModelJson)==0:
                print('No models available')
                return {}
            modelPerm={}
            for c in resModelJson['registered_models']:
                modelName=c['name']
                param={'name':modelName}
                modIDRes=requests.get(f"{workspace_url}/api/2.0/mlflow/databricks/registered-models/get", headers=headers, data=json.dumps(param))
                modelID=modIDRes.json()['registered_model_databricks']['id']
                resModelPerm=requests.get(f"{workspace_url}/api/2.0/permissions/registered-models/{modelID}", headers=headers)
                if resModelPerm.status_code==404:
                    print(f'feature not enabled for this tier')
                    pass
                resModelPermJson=resModelPerm.json()   
                aclList=getACL(resModelPermJson['access_control_list'])                
                if len(aclList)==0:continue
                modelPerm[modelID]=aclList  
            try:
                nextPageToken=resModelJson['next_page_token']
            except KeyError:
                break
        return modelPerm
    except Exception as e:
        print(f'error in retriveing model permission: {e}')
def getDLTACL(workspace_url:str)-> dict:
    try:
        nextPageToken=''
        dltPerm={}
        while True:
            data={}
            if nextPageToken!="":    
                data={'page_token':nextPageToken}
            resDlt=requests.get(f"{workspace_url}/api/2.0/pipelines", headers=headers,data=json.dumps(data))
            resDltJson=resDlt.json()
            if len(resDltJson)==0:
                print('No dlt pipelines available')
                return {}
            for c in resDltJson['statuses']:
                dltID=c['pipeline_id']
                resDltPerm=requests.get(f"{workspace_url}/api/2.0/permissions/pipelines/{dltID}", headers=headers)
                if resDltPerm.status_code==404:
                    print(f'feature not enabled for this tier')
                    pass
                resDltPermJson=resDltPerm.json()   
                aclList=getACL(resDltPermJson['access_control_list'])
                if len(aclList)==0:continue
                dltPerm[dltID]=aclList  
            try:
                nextPageToken=resDltJson['next_page_token']
            except KeyError:
                break

        return dltPerm
    except Exception as e:
        print(f'error in retriveing dlt pipelines permission: {e}')

def getFolderList(workspace_url:str, path:str)-> dict:
    try:
        data={'path':path}
        resFolder=requests.get(f"{workspace_url}/api/2.0/workspace/list", headers=headers, data=json.dumps(data))
        resFolderJson=resFolder.json()
        folderPerm={}
        if len(resFolderJson)==0:
            return
        for c in resFolderJson['objects']:
            if c['object_type']=="DIRECTORY":
                folderList[c['object_id']]=c['path']
                getFolderList(workspace_url,c['path'])
            elif c['object_type']=="NOTEBOOK":
                notebookList[c['object_id']]=c['path']
        
        return 
    except Exception as e:
        print(f'error in retriving folder details: {e}')


def getFoldersNotebookACL(workspace_url:str)-> list:
    try:
        getFolderList(workspace_url,"/Users")
        folderPerm={}
        notebookPerm={}
        #print(notebookList)
        for k,v in folderList.items():
            resFolderPerm=requests.get(f"{workspace_url}/api/2.0/permissions/directories/{k}", headers=headers)
            if resFolderPerm.status_code==404:
                print(f'feature not enabled for this tier')
                pass
            resFolderPermJson=resFolderPerm.json()   
            aclList=getACL(resFolderPermJson['access_control_list'])            
            if len(aclList)==0:continue
            folderPerm[k]=aclList  
        for k,v in notebookList.items():
            resNotebookPerm=requests.get(f"{workspace_url}/api/2.0/permissions/notebooks/{k}", headers=headers)
            if resNotebookPerm.status_code==404:
                print(f'feature not enabled for this tier')
                pass
            resNotebookPermJson=resNotebookPerm.json()   
            aclList=getACL(resNotebookPermJson['access_control_list'])
            if len(aclList)==0:continue
            notebookPerm[k]=aclList  
        return folderPerm, notebookPerm
    except Exception as e:
        print(f'error in retriveing folder permission: {e}')

def getRepoACL(workspace_url:str)-> dict:
    try:
        nextPageToken=''
        repoPerm={}
        while True:
            data={}
            if nextPageToken!="":    
                data={'page_token':nextPageToken}
            resRepo=requests.get(f"{workspace_url}/api/2.0/repos", headers=headers,data=json.dumps(data))
            resRepoJson=resRepo.json()
            if len(resRepoJson)==0:
                print('No repos available')
                return {}
            for c in resRepoJson['repos']:
                repoID=c['id']
                resRepoPerm=requests.get(f"{workspace_url}/api/2.0/permissions/repos/{repoID}", headers=headers)
                if resRepoPerm.status_code==404:
                    print(f'feature not enabled for this tier')
                    pass
                resRepoPermJson=resRepoPerm.json()   
                aclList=getACL(resRepoPermJson['access_control_list'])
                if len(aclList)==0:continue
                repoPerm[repoID]=aclList  
            try:
                nextPageToken=resRepoJson['next_page_token']
            except KeyError:
                break

        return repoPerm
    except Exception as e:
        print(f'error in retriveing repos permission: {e}')
def getTokenACL(workspace_url:str)-> dict:
    try:
        tokenPerm={}
        resTokenPerm=requests.get(f"{workspace_url}/api/2.0/preview/permissions/authorization/tokens", headers=headers)
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
        tokenPerm['tokens']=aclList  
        return tokenPerm
    except Exception as e:
        print(f'error in retriveing Token permission: {e}')
def getSecretScoppeACL(workspace_url:str)-> dict:
    try:

        resSScope=requests.get(f"{workspace_url}/api/2.0/secrets/scopes/list", headers=headers)
        resSScopeJson=resSScope.json()
        if len(resSScopeJson)==0:
            print('No secret scopes defined.')
            return {}
        secretScopePerm={}
        for c in resSScopeJson['scopes']:
            scopeName=c['name']
            data={'scope':scopeName}
            resSSPerm=requests.get(f"{workspace_url}/api/2.0/secrets/acls/list/", headers=headers, data=json.dumps(data))
            if resSSPerm.status_code==404:
                print(f'feature not enabled for this tier')
                pass
            resSSPermJson=resSSPerm.json()   
            aclList=[]
            groupsL=[a[0] for a in groupList]
            for acl in resSSPermJson['items']:
                try:
                    if acl['principal'] in groupsL:
                        aclList.append(list([acl['principal'],acl['permission']]))
                except KeyError:
                    continue
            secretScopePerm[scopeName]=aclList    

        return secretScopePerm
    except Exception as e:
        print(f'error in retriving Secret Scope permission: {e}')
#groupList=getGroupList(resJson)
#groupMembers=getGroupMembers(resJson)
#groupEntitlements=getGroupEntitlements(resJson)

def updateGroupEntitlements(groupEntitlements:dict):
    try:
        
        for group_id, etl in groupEntitlements.items():
            entitlementList=[]
            for e in etl:
                entitlementList.append({"value":e})
            entitlements = {
                            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                            "Operations": [{"op": "add",
                                        "path": "entitlements",
                                        "value": entitlementList}]
                        }
            resPatch=requests.patch(f'{workspace_url}/api/2.0/preview/scim/v2/Groups/{group_id}', headers=headers, data=json.dumps(entitlements))
    except Exception as e:
        print(f'error applying entitiement for group id: {group_id}.')

def updateGroupRoles(groupRoles:dict):
    try:
        
        for group_id, roles in groupRoles.items():
            roleList=[]
            for e in roles:
                roleList.append({"value":e})
            instanceProfileRoles = {
                            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                            "Operations": [{"op": "add",
                                        "path": "roles",
                                        "value": roleList}]
                        }
            resPatch=requests.patch(f'{workspace_url}/api/2.0/preview/scim/v2/Groups/{group_id}', headers=headers, data=json.dumps(instanceProfileRoles))
    except Exception as e:
        print(f'error applying role for group id: {group_id}.')

def updateGroupPermission(object:str, groupPermission : dict):
    try:
        for object_id,aclList in groupPermission.items(): 
            dataAcl=[]
            for  acl in aclList:
                dataAcl.append({"group_name":acl[0],"permission_level":acl[1]})
            data={"access_control_list":dataAcl}
            resAppPerm=requests.patch(f"{workspace_url}/api/2.0/preview/permissions/{object}/{object_id}", headers=headers, data=json.dumps(data))
    except Exception as e:
        print(f'Error setting permission for {object} {object_id}. {e} ')
def updateGroup2Permission(object:str, groupPermission : dict):
    try:
        for object_id,aclList in groupPermission.items(): 
            dataAcl=[]
            data={"access_control_list":aclList}
            resAppPerm=requests.post(f"{workspace_url}/api/2.0/preview/sql/permissions/{object}/{object_id}", headers=headers, data=json.dumps(data))
    except Exception as e:
        print(f'Error setting permission for {object} {object_id}. {e} ')
def updateSecretPermission(secretPermission : dict):
    try:
        for object_id,aclList in secretPermission.items(): 
            dataAcl=[]
            for  acl in aclList:
                data={"scope":object_id, "principal":acl[0], "permission":acl[1]}
                resAppPerm=requests.post(f"{workspace_url}/api/2.0/secrets/acls/put", headers=headers, data=json.dumps(data))
    except Exception as e:
        print(f'Error setting permission for scope {object_id}. {e} ')

#print(groupList)
#print(groupMembers)
#print(groupEntitlements)
#groupRoles=getGroupRoles(resJson)
#print(groupRoles)
#passwordPerm= getPasswordACL(workspace_url)
#print(passwordPerm)

#clusterPerm=getClusterACL(workspace_url)
#print(clusterPerm)
#clusterPolicyPerm=getClusterPolicyACL(workspace_url)
#print(clusterPolicyPerm)
#warehousePerm=getWarehouseACL(workspace_url)
#print(warehousePerm)
#dashboardPerm=getDashboardACL(workspace_url)
#print(dashboardPerm)
#queryPerm=getQueriesACL(workspace_url)
#print(queryPerm)
#alertPerm=getAlertsACL(workspace_url)
#print(alertPerm)
#instancePoolPerm=getPoolACL(workspace_url)
#print(instancePoolPerm)
#jobPerm=getJobACL(workspace_url)
#print(jobPerm)
#expPerm=getExperimentACL(workspace_url)
#print(expPerm)
#modelPerm=getModelACL(workspace_url)
#print(modelPerm)
#dltPerm=getDLTACL(workspace_url)
#print(dltPerm)
#folderPerm, notebookPerm=getFoldersNotebookACL(workspace_url)
#print(folderPerm)
#print('notebook')
#print(notebookPerm)
#repoPerm=getRepoACL(workspace_url)
#print(repoPerm)
#tokenPerm=getTokenACL(workspace_url)
#print(tokenPerm)
#secretScopePerm=getSecretScoppeACL(workspace_url)
#print(secretScopePerm)
#data={'scope':'TestA', 'principal':'BusinessAnalyst', 'permission':'READ'}
#resSSPerm=requests.post(f"{workspace_url}/api/2.0/secrets/acls/put", headers=headers, data=json.dumps(data))
#print(resSSPerm.text)
#updateGroupEntitlements(groupEntitlements)
#updateGroupPermission('clusters',clusterPerm)
#updateGroupPermission('cluster-policies',clusterPolicyPerm)
#updateGroupPermission('sql/warehouses',warehousePerm)
#updateGroupPermission('instance-pools',instancePoolPerm)
#updateGroupPermission('jobs',jobPerm)
#updateGroupPermission('experiments',expPerm)
#updateGroupPermission('registered-models',modelPerm)
#updateGroupPermission('pipelines',dltPerm)
#updateGroupPermission('directories',folderPerm)

#updateGroupPermission('notebooks',notebookPerm)

#updateGroupPermission('repos',repoPerm)
#updateGroupPermission('authorization',tokenPerm)
#updateSecretPermission(secretScopePerm)
#updateGroup2Permission('dashboards',dashboardPerm)
#updateGroupPermission('authorization',passwordPerm)