// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The resources which were recently accessed by a user.
* 
*/
export interface RecentlyAccessed {
    /**
    * List of recent resources that are global, meaning not contained to workspace example catalog and its subresoruces.
    */
    'globalLevelResources': Array<model.RecentResourceItem>;
    /**
    * List of recent resources that are contained in workspace, like clusters.
    */
    'workspaceLevelResources': Array<model.RecentResourceItem>;

}

export namespace RecentlyAccessed {



    export function getJsonObj(obj: RecentlyAccessed): object {
        const jsonObj = {...obj, ...{
            
                'globalLevelResources': obj.globalLevelResources ?
                
                obj.globalLevelResources.map((item)=>{return model.RecentResourceItem.getJsonObj(item)})
                
                 : undefined,
                'workspaceLevelResources': obj.workspaceLevelResources ?
                
                obj.workspaceLevelResources.map((item)=>{return model.RecentResourceItem.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RecentlyAccessed): object {
        const jsonObj = {...obj, ...{
            
                    'globalLevelResources': obj.globalLevelResources ?
                
                obj.globalLevelResources.map((item)=>{return model.RecentResourceItem.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'workspaceLevelResources': obj.workspaceLevelResources ?
                
                obj.workspaceLevelResources.map((item)=>{return model.RecentResourceItem.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
