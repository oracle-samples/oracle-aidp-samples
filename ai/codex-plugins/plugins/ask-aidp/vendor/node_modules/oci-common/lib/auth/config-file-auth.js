"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.ConfigFileAuthenticationDetailsProvider = void 0;
const auth_1 = require("./auth");
const utils_1 = require("../utils");
const config_file_reader_1 = require("../config-file-reader");
const fs_1 = require("fs");
const region_1 = require("../region");
const realm_1 = require("../realm");
const CONFIG_FILE_AUTH_INFO = "For more info about config file and how to get required information, see https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm for more info on OCI configuration files.";
class ConfigFileAuthenticationDetailsProvider {
    /**
     * Creates a new instance using the config file at the default location,
     * see {@link ConfigFileReader#DEFAULT_FILE_PATH}.
     *
     * @param profile
     *            profile to load, optional
     * @param configurationFilePath
     *            config file path, optional
     * @throws Error
     *             if the configuration file could not be loaded
     */
    constructor(configurationFilePath, profile) {
        const pfl = profile || config_file_reader_1.ConfigFileReader.DEFAULT_PROFILE_NAME;
        if (configurationFilePath) {
            this.delegate = this.createConfigFileAuth(config_file_reader_1.ConfigFileReader.parseFileFromPath(configurationFilePath, pfl));
        }
        else {
            this.delegate = this.createConfigFileAuth(config_file_reader_1.ConfigFileReader.parseDefault(pfl));
        }
    }
    /**
     * Get a region based on regionId, if corresponding region is not found from regionId,
     * create a new region by registering regionId with Realm.OC1
     *
     * @param regionId: string
     * @param region: any
     * @return: Region
     */
    retrieveRegionFromRegionId(regionId) {
        let region;
        try {
            region = region_1.Region.fromRegionId(regionId);
            if (!region) {
                console.warn(`Found regionId ${regionId} in config file, but not supported by this version of the SDK`);
                const fallbackSecondLevelDomain = process.env["OCI_DEFAULT_REALM"];
                // Before defaulting to Realm.OC1, check if user defined a second level domain for unknown region fallback.
                // If so, create a dummy realm with the second level domain set from the env.OCI_DEFAULT_REALM. Else default to OC1 realm.
                if (fallbackSecondLevelDomain) {
                    console.warn(`Falling back to using second level domain: ${fallbackSecondLevelDomain}`);
                    const unknownRealm = realm_1.Realm.register("unknown", fallbackSecondLevelDomain);
                    region = region_1.Region.register(regionId, unknownRealm);
                }
                else {
                    // Proceed by assuming the region id in the config file belongs to OC1 realm.
                    console.warn(`Falling back to using OC1 realm.`);
                    region = region_1.Region.register(regionId, realm_1.Realm.OC1);
                }
            }
            return region;
        }
        catch (e) {
            throw new Error(`Error from retrying to retrieve region from regionId: ${e}. ${CONFIG_FILE_AUTH_INFO}`);
        }
    }
    createConfigFileAuth(file) {
        const authType = file.get("authentication_type");
        const tenantId = utils_1.checkNotNull(file.get("tenancy"), "missing tenancy in config");
        const delegationTokenPath = file.get("delegation_token_file");
        let delegationToken = "";
        let region = null;
        const regionEnvVar = process.env.OCI_REGION;
        let regionId = file.get("region") || regionEnvVar;
        if (regionId) {
            region = this.retrieveRegionFromRegionId(regionId);
        }
        else {
            throw Error("Region not specified in Config file or OCI_REGION env variable. Can not proceed without setting a region. " +
                CONFIG_FILE_AUTH_INFO);
        }
        if (delegationTokenPath) {
            delegationToken = fs_1.readFileSync(delegationTokenPath, "utf8").replace(/\n/g, "");
        }
        // If authType exist that means we are not doing file based authentication, return early.
        // This also assumes that a delegationTokenPath exists.
        if (authType) {
            return new auth_1.SimpleAuthenticationDetailsProvider(tenantId, "", "", "", "", region, authType, delegationToken);
        }
        const fingerprint = utils_1.checkNotNull(file.get("fingerprint"), "missing fingerprint in config");
        const pemFilePath = utils_1.checkNotNull(file.get("key_file"), "missing key_file in config");
        const passPhrase = file.get("pass_phrase");
        const privateKey = this.getPvtKey(config_file_reader_1.ConfigFileReader.expandUserHome(pemFilePath));
        const profileCredentials = file.profileCredentials;
        const sessionTokenPath = file.get("security_token_file");
        const sessionToken = sessionTokenPath
            ? this.getPvtKey(config_file_reader_1.ConfigFileReader.expandUserHome(sessionTokenPath))
            : undefined;
        const user = sessionToken ? "" : utils_1.checkNotNull(file.get("user"), "missing user in config");
        return new auth_1.SimpleAuthenticationDetailsProvider(tenantId, user, fingerprint, privateKey, passPhrase, region, undefined, undefined, profileCredentials, sessionToken);
    }
    getPvtKey(filePath) {
        try {
            const key = fs_1.readFileSync(filePath, "utf8");
            return key;
        }
        catch (e) {
            throw "Failed to read private key from file path. " + CONFIG_FILE_AUTH_INFO;
        }
    }
    /**
     * Get the key id to sign the http request.
     */
    getKeyId() {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.delegate.getKeyId();
        });
    }
    /**
     * Get the private key to sign the http request.
     */
    getPrivateKey() {
        return this.delegate.getPrivateKey();
    }
    /**
     * Get the passphrase of the private key to sign the http request.
     */
    getPassphrase() {
        return this.delegate.getPassphrase();
    }
    /**
     * Get the Tenancy
     */
    getTenantId() {
        return this.delegate.getTenantId();
    }
    /**
     * Get the user
     */
    getUser() {
        return this.delegate.getUser();
    }
    /**
     * Get the fingerprint
     */
    getFingerprint() {
        return this.delegate.getFingerprint();
    }
    /**
     * Get the region
     */
    getRegion() {
        return this.delegate.getRegion();
    }
    /**
     * Set the region
     */
    setRegion(regionId) {
        this.delegate.setRegion(this.retrieveRegionFromRegionId(regionId));
    }
    /**
     * Get the authType
     */
    getAuthType() {
        return this.delegate.getAuthType();
    }
    /**
     * Set the provider
     */
    setProvider(provider) {
        this.delegate.setProvider(provider);
    }
    /**
     * Get the provider
     */
    getProvider() {
        return this.delegate.getProvider();
    }
    getDelegationToken() {
        return this.delegate.getDelegationToken();
    }
    getProfileCredentials() {
        return this.delegate.getProfileCredentials();
    }
    get sessionToken() {
        return this.delegate.sessionToken;
    }
    set sessionToken(token) {
        this.delegate.sessionToken = token;
    }
}
exports.ConfigFileAuthenticationDetailsProvider = ConfigFileAuthenticationDetailsProvider;
//# sourceMappingURL=config-file-auth.js.map