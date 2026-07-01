"use strict";
/*
 * Copyright (c) 2020, 2023, Oracle and/or its affiliates. All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.reInitialize = exports.useOnlyDeveloperToolConfigurationRegions = exports.doesDeveloperToolConfigurationFileExist = exports.isociAllowOnlyDeveloperToolConfigurationRegionsEnabled = exports.isServiceEnabled = exports.developerToolConfiguration = exports.getDeveloperToolConfigurationFilePath = void 0;
const path_1 = __importDefault(require("path"));
const region_1 = require("./region");
const fs_1 = __importDefault(require("fs"));
const realm_1 = require("./realm");
const config_file_reader_1 = require("./config-file-reader");
const log_1 = require("./log");
const OCI_DEVELOPER_TOOL_CONFIGURATION_FILE_PATH = "OCI_DEVELOPER_TOOL_CONFIGURATION_FILE_PATH";
const OCI_ALLOW_ONLY_DEVELOPER_TOOL_CONFIGURATION_REGIONS = "OCI_ALLOW_ONLY_DEVELOPER_TOOL_CONFIGURATION_REGIONS";
const DEFAULT_DEVELOPER_TOOL_CONFIGURATION_FILE_PATH = config_file_reader_1.ConfigFileReader.expandUserHome(path_1.default.join("~", ".oci", "developer-tool-configuration.json"));
let ociAllowOnlyDeveloperToolConfigurationvRegions;
let developerToolConfigurationProvider;
let ociEnabledServiceSet;
let developerToolConfigurationRegions;
function getParsedServiceName(serviceName) {
    return serviceName.toLowerCase().replace("/[^a-z]/", "");
}
function getDeveloperToolConfigurationFilePath() {
    var _a;
    return ((_a = process.env.OCI_DEVELOPER_TOOL_CONFIGURATION_FILE_PATH) !== null && _a !== void 0 ? _a : DEFAULT_DEVELOPER_TOOL_CONFIGURATION_FILE_PATH);
}
exports.getDeveloperToolConfigurationFilePath = getDeveloperToolConfigurationFilePath;
function initializedeveloperToolConfiguration() {
    var _a, _b;
    // Initialize the OciEnabledServiceSet
    ociEnabledServiceSet = new Set();
    developerToolConfigurationRegions = new Array();
    developerToolConfigurationProvider = "";
    ociAllowOnlyDeveloperToolConfigurationvRegions = false;
    if (!doesDeveloperToolConfigurationFileExist())
        return;
    let configFilePath = getDeveloperToolConfigurationFilePath();
    try {
        let content = fs_1.default.readFileSync(configFilePath, "utf8");
        if (!content) {
            return;
        }
        let developerToolConfig;
        try {
            developerToolConfig = JSON.parse(content);
        }
        catch (error) {
            if (log_1.LOG.logger)
                log_1.LOG.logger.error("Failure while parsing DeveloperToolConfiguration config file: " +
                    configFilePath +
                    " ex:" +
                    error);
        }
        if (developerToolConfig !== undefined) {
            // Add configured services to OciEnabledServiceSet
            (_a = developerToolConfig === null || developerToolConfig === void 0 ? void 0 : developerToolConfig.services) === null || _a === void 0 ? void 0 : _a.forEach((service) => {
                ociEnabledServiceSet.add(getParsedServiceName(service));
            });
            // Add configured Regions to developerToolConfigurationRegions
            (_b = developerToolConfig === null || developerToolConfig === void 0 ? void 0 : developerToolConfig.regions) === null || _b === void 0 ? void 0 : _b.forEach((region) => {
                developerToolConfigurationRegions.push(region);
            });
            // Initialize DeveloperToolConfiguration provider
            if (developerToolConfig.developerToolConfigurationProvider !== null) {
                developerToolConfigurationProvider = developerToolConfig.developerToolConfigurationProvider;
            }
            // Initialize ociAllowOnlyDeveloperToolConfigurationvRegions from DeveloperToolConfiguration config
            if (developerToolConfig.allowOnlyDeveloperToolConfigurationRegions !== null) {
                ociAllowOnlyDeveloperToolConfigurationvRegions = Boolean(developerToolConfig.allowOnlyDeveloperToolConfigurationRegions);
            }
        }
    }
    catch (error) {
        if (error.code === "ENOENT") {
            if (log_1.LOG.logger)
                log_1.LOG.logger.error("DeveloperToolConfiguration config file not found at " +
                    configFilePath +
                    ", enabling all OCI services as default");
            return;
        }
        else {
            if (log_1.LOG.logger)
                log_1.LOG.logger.error("Enabling all OCI services as failsafe. There was an exception while trying to read or de-serialize the DeveloperToolConfiguration config file at: " +
                    configFilePath +
                    " ex:" +
                    error);
        }
    }
}
function developerToolConfiguration() {
    initializedeveloperToolConfiguration();
}
exports.developerToolConfiguration = developerToolConfiguration;
function isServiceEnabled(service) {
    // Convert the service name to lower case to avoid match failure in list
    service = getParsedServiceName(service);
    if (ociEnabledServiceSet == null) {
        initializedeveloperToolConfiguration();
    }
    // If OciEnabledServiceSet is empty then we enable all services.
    if (ociEnabledServiceSet.size == 0) {
        if (log_1.LOG.logger)
            log_1.LOG.logger.debug("The OciEnabledServiceSet is empty, all OCI services are enabled");
        return true;
    }
    return ociEnabledServiceSet.has(service);
}
exports.isServiceEnabled = isServiceEnabled;
function isociAllowOnlyDeveloperToolConfigurationRegionsEnabled() {
    var _a;
    let ociAllowOnlyDeveloperToolConfigurationvRegionsFromEnvironmentVariable = Boolean((_a = process.env.OCI_ALLOW_ONLY_DEVELOPER_TOOL_CONFIGURATION_REGIONS) !== null && _a !== void 0 ? _a : false);
    if (ociAllowOnlyDeveloperToolConfigurationvRegionsFromEnvironmentVariable != null) {
        const result = Boolean(ociAllowOnlyDeveloperToolConfigurationvRegionsFromEnvironmentVariable);
        return result;
    }
    return ociAllowOnlyDeveloperToolConfigurationvRegions;
}
exports.isociAllowOnlyDeveloperToolConfigurationRegionsEnabled = isociAllowOnlyDeveloperToolConfigurationRegionsEnabled;
function doesDeveloperToolConfigurationFileExist() {
    return fs_1.default.existsSync(getDeveloperToolConfigurationFilePath());
}
exports.doesDeveloperToolConfigurationFileExist = doesDeveloperToolConfigurationFileExist;
function useOnlyDeveloperToolConfigurationRegions() {
    return (!isociAllowOnlyDeveloperToolConfigurationRegionsEnabled() &&
        (doesDeveloperToolConfigurationFileExist() || developerToolConfigurationRegions.length != 0));
}
exports.useOnlyDeveloperToolConfigurationRegions = useOnlyDeveloperToolConfigurationRegions;
function reInitialize() {
    region_1.Region.resetDeveloperToolConfiguration();
    realm_1.Realm.resetDeveloperToolConfiguration();
    initializedeveloperToolConfiguration();
}
exports.reInitialize = reInitialize;
//# sourceMappingURL=developertoolconfiguration.js.map