from os import getgrouplist
import requests
import json

workspace_url = 'https://adb-984752964297111.11.azuredatabricks.net'
workspace_url = 'https://adb-5932067186463130.10.azuredatabricks.net'
token='dapi5528d3a905ef8b2a6f9d186bb1ff8c52'
token='dapi61015f016e07c4f4628f0b3cabab22ca'
headers={'Authorization': 'Bearer %s' % token}
res=requests.get(f"{workspace_url}/api/2.0/preview/scim/v2/Groups", headers=headers)
#res=requests.get(f"{workspace_url}/api/2.0/clusters/list", headers=headers)
groupList=[]
groupMembers={}
groupEntitlements={}
#print(res.status_code)
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
            for mem in e['members']:
                members.append(list([mem['display'],mem['value']]))
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
        print(f'error in retriveing groupMembers : {e}')

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
            aclList=[]
            
            for acl in resCPermJson['access_control_list']:
                try:
                    aclList.append(list([acl['group_name'],acl['all_permissions'][0]['permission_level']]))
                except KeyError:
                    continue

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
            aclList=[]
            for acl in resCPPermJson['access_control_list']:
                try:
                    aclList.append(list([acl['group_name'],acl['all_permissions'][0]['permission_level']]))
                except KeyError:
                    continue

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
         
            aclList=[]
            
            for acl in resWPermJson['access_control_list']:
                try:
                    aclList.append(list([acl['group_name'],acl['all_permissions'][0]['permission_level']]))
                except KeyError:
                    continue

            warehousePerm[warehouseId]=aclList    
            
        return warehousePerm
    except Exception as e:
        print(f'error in retriveing warehouse permission: {e}')

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
            aclList=[]
            
            for acl in resIPPermJson['access_control_list']:
                try:
                    aclList.append(list([acl['group_name'],acl['all_permissions'][0]['permission_level']]))
                except KeyError:
                    continue

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
                aclList=[]
                
                for acl in resJobPermJson['access_control_list']:
                    try:
                        aclList.append(list([acl['group_name'],acl['all_permissions'][0]['permission_level']]))
                    except KeyError:
                        continue

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
                aclList=[]
                
                for acl in resExpPermJson['access_control_list']:
                    try:
                        aclList.append(list([acl['group_name'],acl['all_permissions'][0]['permission_level']]))
                    except KeyError:
                        continue
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
                aclList=[]
                
                for acl in resModelPermJson['access_control_list']:
                    try:
                        aclList.append(list([acl['group_name'],acl['all_permissions'][0]['permission_level']]))
                    except KeyError:
                        continue
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
                aclList=[]
                
                for acl in resDltPermJson['access_control_list']:
                    try:
                        aclList.append(list([acl['group_name'],acl['all_permissions'][0]['permission_level']]))
                    except KeyError:
                        continue
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
            aclList=[]
            for acl in resFolderPermJson['access_control_list']:
                try:
                    aclList.append(list([acl['group_name'],acl['all_permissions'][0]['permission_level'],acl['all_permissions'][0]['inherited']]))
                except KeyError:
                    continue
            folderPerm[k]=aclList  
        for k,v in notebookList.items():
            resNotebookPerm=requests.get(f"{workspace_url}/api/2.0/permissions/notebooks/{k}", headers=headers)
            if resNotebookPerm.status_code==404:
                print(f'feature not enabled for this tier')
                pass
            resNotebookPermJson=resNotebookPerm.json()   
            aclList=[]
            for acl in resNotebookPermJson['access_control_list']:
                try:
                    aclList.append(list([acl['group_name'],acl['all_permissions'][0]['permission_level'],acl['all_permissions'][0]['inherited']]))
                except KeyError:
                    continue
            notebookPerm[k]=aclList  
        return folderPerm, notebookPerm
    except Exception as e:
        print(f'error in retriveing folder permission: {e}')
#groupList=getGroupList(resJson)
#groupMembers=getGroupMembers(resJson)
#groupEntitlements=getGroupEntitlements(resJson)
#print(groupList)
#print(groupMembers)
#print(groupEntitlements)
#clusterPerm=getClusterACL(workspace_url)
#print(clusterPerm)
#clusterPolicyPerm=getClusterPolicyACL(workspace_url)
#print(clusterPolicyPerm)
#warehousePerm=getWarehouseACL(workspace_url)
#print(warehousePerm)
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
folderPerm, notebookPerm=getFoldersNotebookACL(workspace_url)
[print(v) for k,v in notebookPerm.items()]
