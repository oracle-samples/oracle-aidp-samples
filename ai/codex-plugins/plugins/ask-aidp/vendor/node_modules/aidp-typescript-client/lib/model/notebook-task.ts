// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The information about the notebook task.
*/
export interface NotebookTask extends model.Task {
    /**
    * The path where the notebook exists.
    */
    'notebookPath': string;
    /**
    * Source selected for a task. Either workspace or Git provider.
    */
    'source'?: NotebookTask.Source;
    'gitConfig'?: model.GitConfig;
    'cluster': model.JobCluster;
    /**
    * An optional list of parameters.
    */
    'parameters'?: Array<model.Parameter>;
    /**
    * An optional value to indicate the max run duration of a job after which job will be timed out. The default is Zero indicating no timeout value. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'timeoutSeconds'?: number;
    /**
    * An optional property to enable or disable the streaming capability for a task.
    */
    'isStreaming'?: boolean;

   "type": string;
}

export namespace NotebookTask {


    export enum Source {
    
    Workspace = "WORKSPACE",
    GitProvider = "GIT_PROVIDER",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}







    export function getJsonObj(obj: NotebookTask, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Task.getJsonObj(obj) as NotebookTask, ...{
            


                'gitConfig': obj.gitConfig ?
                
                
                model.GitConfig.getJsonObj(obj.gitConfig) : undefined,
                'cluster': obj.cluster ?
                
                
                model.JobCluster.getJsonObj(obj.cluster) : undefined,
                'parameters': obj.parameters ?
                
                obj.parameters.map((item)=>{return model.Parameter.getJsonObj(item)})
                
                 : undefined,


        }};

        
        
        return jsonObj;
    }
    export const type = 'NOTEBOOK_TASK';
    export function getDeserializedJsonObj(obj: NotebookTask, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Task.getDeserializedJsonObj(obj) as NotebookTask, ...{
            


                    'gitConfig': obj.gitConfig ?
                
                
                model.GitConfig.getDeserializedJsonObj(obj.gitConfig) : undefined,
                    'cluster': obj.cluster ?
                
                
                model.JobCluster.getDeserializedJsonObj(obj.cluster) : undefined,
                    'parameters': obj.parameters ?
                
                obj.parameters.map((item)=>{return model.Parameter.getDeserializedJsonObj(item)})
                
                 : undefined,


         }};

        
        
        return jsonObj;
    }
}
