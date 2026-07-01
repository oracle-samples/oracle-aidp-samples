// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * @example Click {@link https://docs.oracle.com/en-us/iaas/tools/typescript-sdk-examples/latest/aidp/GetContent.ts.html |here} to see how to use GetContentRequest.
 */
export interface GetContentRequest extends common.BaseRequest {
/**
 * The [OCID]({{DOC_SERVER_URL}}/iaas/Content/General/Concepts/identifiers.htm) of the AI Data Platform (Data Lake) instance.
 */
 'aiDataPlatformId': string;
/**
 * The key of the Workspace
 */
 'workspaceKey': string;
/**
 * The path to the notebook file.
 */
 'contentPath': string;
/**
 * Unique Oracle-assigned identifier for the request. If you need to contact
* Oracle about a particular request, please provide the request ID.
* The only valid characters for request IDs are letters, numbers,
* underscore, and dash.
* 
 */
 'opcRequestId'?: string;
/**
 * Content type. Either file, directory, or notebook.
 */
 'type'?: GetContentRequest.Type;
/**
 * The format in which content should be returned. Either text, base64, or JSON.
 */
 'format'?: GetContentRequest.Format;
/**
 * Returns content based on param value. When set to 0, content is NOT returned. When set to 1, content is returned.
* 
 */
 'content'?: number;
/**
 * Returns hash hexdigest string of content and the hash algorithm. 0 for no hash, 1 for return hash. 0 is default. It may be ignored by the content manager.
* 
 */
 'hash'?: number;
/**
 * A flag to identify if the recent list should be updated.
 */
 'shouldUpdateRecent'?: boolean;
}

export namespace GetContentRequest {

  export enum Type {
    File = ("file"),
    Directory = ("directory"),
    Notebook = ("notebook")
  }

  export enum Format {
    Text = ("text"),
    Base64 = ("base64"),
    Json = ("json")
  }

}
