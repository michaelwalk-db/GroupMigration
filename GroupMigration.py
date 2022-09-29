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
        print(f'error in retriveing warehouse permission: {e}')
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