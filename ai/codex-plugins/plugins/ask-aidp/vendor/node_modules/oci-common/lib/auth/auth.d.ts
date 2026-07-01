/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
import { Region } from "../region";
import AbstractAuthenticationDetailsProvider from "./models/abstract-authentication-details-provider";
import { ConfigAccumulator } from "../config-file-reader";
/**
 * An interface provides the authentication details.
 */
export interface AuthenticationDetailsProvider extends AbstractAuthenticationDetailsProvider {
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
     * Get the auth type of this authentication provider
     */
    getAuthType?(): string | undefined;
    /**
     * Get the true authentication provider
     */
    getProvider?(): AuthenticationDetailsProvider;
    /**
     * Set the provider
     */
    setProvider?(provider: AuthenticationDetailsProvider): void;
    /**
     * Get the delegation token
     */
    getDelegationToken?(): string | undefined;
}
export interface RegionProvider {
    /**
     * Returns the region.
     *
     * @return Region object.
     */
    getRegion(): Region;
}
export declare function isRegionProvider(arg: any): arg is RegionProvider;
/**
 * The API based authentication details provider which implements [[AuthenticationDetailsProvider]].
 */
export declare class SimpleAuthenticationDetailsProvider implements AuthenticationDetailsProvider, RegionProvider {
    private tenancy;
    private user;
    private fingerprint;
    private privateKey;
    private passphrase;
    private region?;
    private authType?;
    private delegationToken?;
    private profileCredentials?;
    private _sessionToken?;
    private provider;
    /**
     * Construct an instance of [[SimpleAuthenticationDetailsProvider]].
     * @param tenancy   tenancy id.
     * @param user  user id.
     * @param fingerprint   user's fingerprint.
     * @param privateKey    private key to sign the request.
     * @param passphrase    the passphrase of private key.
     */
    constructor(tenancy: string, user: string, fingerprint: string, privateKey: string, passphrase: string | null, region?: Region | undefined, authType?: string | undefined, delegationToken?: string | undefined, profileCredentials?: ConfigAccumulator | undefined, _sessionToken?: string | undefined);
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
    setRegion(region: Region): void;
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
    /**
     * Get the delegation token
     */
    getDelegationToken(): string | undefined;
    getProfileCredentials(): ConfigAccumulator | undefined;
    get sessionToken(): string | undefined;
    set sessionToken(token: string | undefined);
}
