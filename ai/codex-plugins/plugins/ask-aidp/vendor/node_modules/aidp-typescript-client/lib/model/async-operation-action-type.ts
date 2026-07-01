// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The action type of an async operation.
**/
export enum AsyncOperationActionType {
    Unknown = "UNKNOWN",
    CreateCatalog = "CREATE_CATALOG",
    UpdateCatalog = "UPDATE_CATALOG",
    DeleteCatalog = "DELETE_CATALOG",
    TestConnectionCatalog = "TEST_CONNECTION_CATALOG",
    CreateUserSchemaInCatalog = "CREATE_USER_SCHEMA_IN_CATALOG",
    CreateSchema = "CREATE_SCHEMA",
    DeleteSchema = "DELETE_SCHEMA",
    CreateTable = "CREATE_TABLE",
    UpdateTable = "UPDATE_TABLE",
    DeleteTable = "DELETE_TABLE",
    CreateVolume = "CREATE_VOLUME",
    DeleteVolume = "DELETE_VOLUME",
    CopyVolumeFile = "COPY_VOLUME_FILE",
    MoveVolumeFile = "MOVE_VOLUME_FILE",
    DeleteVolumeFile = "DELETE_VOLUME_FILE",
    DeleteVolumeFolder = "DELETE_VOLUME_FOLDER",
    CreateWorkspace = "CREATE_WORKSPACE",
    UpdateWorkspace = "UPDATE_WORKSPACE",
    DeleteWorkspace = "DELETE_WORKSPACE",
    CreateCluster = "CREATE_CLUSTER",
    UpdateCluster = "UPDATE_CLUSTER",
    StartCluster = "START_CLUSTER",
    StopCluster = "STOP_CLUSTER",
    RestartCluster = "RESTART_CLUSTER",
    PatchClusterLibraries = "PATCH_CLUSTER_LIBRARIES",
    DeleteCluster = "DELETE_CLUSTER",
    ManageExtractedEntities = "MANAGE_EXTRACTED_ENTITIES",
    RefreshEntity = "REFRESH_ENTITY",
    DownloadClusterLog = "DOWNLOAD_CLUSTER_LOG",
    MigrateExternalCatalog = "MIGRATE_EXTERNAL_CATALOG",
    UpdateKnowledgeBase = "UPDATE_KNOWLEDGE_BASE",
    DeleteKnowledgeBase = "DELETE_KNOWLEDGE_BASE",
    CreateKnowledgeBase = "CREATE_KNOWLEDGE_BASE",
    DeleteKnowledgeBaseJob = "DELETE_KNOWLEDGE_BASE_JOB",
    CreateKnowledgeBaseJob = "CREATE_KNOWLEDGE_BASE_JOB",
    CreateKnowledgeBaseJobRun = "CREATE_KNOWLEDGE_BASE_JOB_RUN",
    DeployAgentFlow = "DEPLOY_AGENT_FLOW",
    CreateGitFolder = "CREATE_GIT_FOLDER",
    UpdateGitRepository = "UPDATE_GIT_REPOSITORY",
    CreateBundle = "CREATE_BUNDLE",
    BundleDeploy = "BUNDLE_DEPLOY",
    BundlePurge = "BUNDLE_PURGE",
    BundleSync = "BUNDLE_SYNC",
    MarkAsBundle = "MARK_AS_BUNDLE",
    GitCommitPush = "GIT_COMMIT_PUSH",
    GitCreateBranch = "GIT_CREATE_BRANCH",
    GitCheckoutBranch = "GIT_CHECKOUT_BRANCH",
    GitOperationPull = "GIT_OPERATION_PULL",
    GitOperationMerge = "GIT_OPERATION_MERGE",
    GitOperationRebase = "GIT_OPERATION_REBASE",
    GitOperationReset = "GIT_OPERATION_RESET",
    GitOperationResetState = "GIT_OPERATION_RESET_STATE",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace AsyncOperationActionType {
    export function getJsonObj(obj: AsyncOperationActionType): AsyncOperationActionType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: AsyncOperationActionType): AsyncOperationActionType {
        return obj;
    }
}

