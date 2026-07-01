/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
import { AuthenticationDetailsProvider, SimpleAuthenticationDetailsProvider, RegionProvider } from "./auth";
import { ConfigFile, ConfigAccumulator } from "../config-file-reader";
import { Region } from "../region";
export declare class ConfigFileAuthenticationDetailsProvider implements AuthenticationDetailsProvider, RegionProvider {
    private delegate;
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
    constructor(configurationFilePath?: string, profile?: string);
    /**
     * Get a region based on regionId, if corresponding region is not found from regionId,
     * create a new region by registering regionId with Realm.OC1
     *
     * @param regionId: string
     * @param region: any
     * @return: Region
     */
    retrieveRegionFromRegionId(regionId: string): Region;
    createConfigFileAuth(file: ConfigFile): SimpleAuthenticationDetailsProvider;
    getPvtKey(filePath: string): string;
    /**
     * Get the key id to sign the http request.
     */
    getKeyId(): Promise<string>;
    /**
     * Get the private key to sign the http request.
     */
    getPrivateKey(): string;
    /**
     * Get the passphrase of the private key to sign the http request.
     */
    getPassphrase(): string | null;
    /**
     * Get the Tenancy
     */
    getTenantId(): string;
    /**
     * Get the user
     */
    getUser(): string;
    /**
     * Get the fingerprint
     */
    getFingerprint(): string;
    /**
     * Get the region
     */
    getRegion(): Region;
    /**
     * Set the region
     */
    setRegion(regionId: string): void;
    /**
     * Get the authType
     */
    getAuthType(): string | undefined;
    /**
     * Set the provider
     */
    setProvider(provider: AuthenticationDetailsProvider): void;
    /**
     * Get the provider
     */
    getProvider(): AuthenticationDetailsProvider;
    getDelegationToken(): string | undefined;
    getProfileCredentials(): ConfigAccumulator | undefined;
    get sessionToken(): string | undefined;
    set sessionToken(token: string | undefined);
}
