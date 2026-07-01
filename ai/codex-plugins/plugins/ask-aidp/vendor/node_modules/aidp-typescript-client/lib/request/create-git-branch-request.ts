// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * @example Click {@link https://docs.oracle.com/en-us/iaas/tools/typescript-sdk-examples/latest/aidp/CreateGitBranch.ts.html |here} to see how to use CreateGitBranchRequest.
 */
export interface CreateGitBranchRequest extends common.BaseRequest {
/**
 * The [OCID]({{DOC_SERVER_URL}}/iaas/Content/General/Concepts/identifiers.htm) of the AI Data Platform (Data Lake) instance.
 */
 'aiDataPlatformId': string;
/**
 * The key of the Workspace
 */
 'workspaceKey': string;
/**
 * The Git repository key.
 */
 'gitRepositoryKey': string;
/**
 * Details for the new Workspace Object.
 */
 'createGitBranchDetails':  model.CreateGitBranchDetails;
/**
 * A token that uniquely identifies a request so it can be retried in case of a timeout or
* server error without risk of running that same action again. Retry tokens expire after 24
* hours, but can be invalidated before then due to conflicting operations. For example, if a resource
* has been deleted and removed from the system, then a retry of the original creation request
* might be rejected.
* 
 */
 'opcRetryToken'?: string;
/**
 * Unique Oracle-assigned identifier for the request. If you need to contact
* Oracle about a particular request, please provide the request ID.
* The only valid characters for request IDs are letters, numbers,
* underscore, and dash.
* 
 */
 'opcRequestId'?: string;
/**
 * A flag to identify if the recent list should be updated.
 */
 'shouldUpdateRecent'?: boolean;
}

