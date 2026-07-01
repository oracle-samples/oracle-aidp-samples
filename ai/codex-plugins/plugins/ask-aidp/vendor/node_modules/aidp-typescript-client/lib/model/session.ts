// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Notebook Session describes the Session context for a running instance of a Notebook. Each opened Notebook has a separate Session,
* but different Notebook kernels can use same Session if user wants to share data across various opened Notebooks.
* 
*/
export interface Session {
    /**
    * UUID of the notebook session.
    */
    'id': string;
    /**
    * A user-friendly name for the notebook session.
    */
    'name': string;
    /**
    * Path to notebook session. For example, /data/test.ipynb
    */
    'path'?: string;
    /**
    * Notebook session type.
    */
    'type'?: Session.Type;
    /**
    * Cluster ID.
    */
    'clusterId'?: string;
    'kernel'?: model.Kernel;
    /**
    * Agent Flow Key of an agent flow.
    */
    'agentFlowKey'?: string;
    /**
    * lifecycleState of a Notebook Session.
    */
    'lifecycleState'?: Session.LifecycleState;

}

export namespace Session {




    export enum Type {
    
    Notebook = "notebook",
    File = "file",
    Agentflow = "agentflow",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}





    export enum LifecycleState {
    
    Creating = "CREATING",
    Active = "ACTIVE",
    Failed = "FAILED",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}


    export function getJsonObj(obj: Session): object {
        const jsonObj = {...obj, ...{
            




                'cluster_id': obj.clusterId,

                'kernel': obj.kernel ?
                
                
                model.Kernel.getJsonObj(obj.kernel) : undefined,


        }};

        delete (jsonObj as Partial<Session>).clusterId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Session): object {
        const jsonObj = {...obj, ...{
            




                'clusterId': (obj as any)["cluster_id"],

                    'kernel': obj.kernel ?
                
                
                model.Kernel.getDeserializedJsonObj(obj.kernel) : undefined,


         }};

        delete (jsonObj as any)["cluster_id"];
        
        return jsonObj;
    }
}
