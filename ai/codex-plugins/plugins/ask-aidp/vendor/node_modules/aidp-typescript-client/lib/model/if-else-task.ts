// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The information about the If Else task.
*/
export interface IfElseTask extends model.Task {
    /**
    * List of expressions.
    */
    'expressions': Array<model.Expression>;
    /**
    * The condition string which binds expressions from expressions list using AND, OR or NOT operator. Expression key should be used to bind the expressions.
    */
    'condition': string;
    /**
    * An optional list of parameters.
    */
    'parameters'?: Array<model.Parameter>;

   "type": string;
}

export namespace IfElseTask {




    export function getJsonObj(obj: IfElseTask, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Task.getJsonObj(obj) as IfElseTask, ...{
            
                'expressions': obj.expressions ?
                
                obj.expressions.map((item)=>{return model.Expression.getJsonObj(item)})
                
                 : undefined,

                'parameters': obj.parameters ?
                
                obj.parameters.map((item)=>{return model.Parameter.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    export const type = 'IF_ELSE_TASK';
    export function getDeserializedJsonObj(obj: IfElseTask, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Task.getDeserializedJsonObj(obj) as IfElseTask, ...{
            
                    'expressions': obj.expressions ?
                
                obj.expressions.map((item)=>{return model.Expression.getDeserializedJsonObj(item)})
                
                 : undefined,

                    'parameters': obj.parameters ?
                
                obj.parameters.map((item)=>{return model.Parameter.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
