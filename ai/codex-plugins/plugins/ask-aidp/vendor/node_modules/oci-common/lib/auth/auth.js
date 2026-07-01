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
exports.SimpleAuthenticationDetailsProvider = exports.isRegionProvider = void 0;
/*
 * User Defined type guard function to check if the object is of type RegionProvider
 * https://basarat.gitbooks.io/typescript/content/docs/types/typeGuard.html
 */
function isRegionProvider(arg) {
    return arg.getRegion !== undefined;
}
exports.isRegionProvider = isRegionProvider;
/**
 * The API based authentication details provider which implements [[AuthenticationDetailsProvider]].
 */
class SimpleAuthenticationDetailsProvider {
    /**
     * Construct an instance of [[SimpleAuthenticationDetailsProvider]].
     * @param tenancy   tenancy id.
     * @param user  user id.
     * @param fingerprint   user's fingerprint.
     * @param privateKey    private key to sign the request.
     * @param passphrase    the passphrase of private key.
     */
    constructor(tenancy, user, fingerprint, privateKey, passphrase, region, authType, delegationToken, profileCredentials, _sessionToken) {
        this.tenancy = tenancy;
        this.user = user;
        this.fingerprint = fingerprint;
        this.privateKey = privateKey;
        this.passphrase = passphrase;
        this.region = region;
        this.authType = authType;
        this.delegationToken = delegationToken;
        this.profileCredentials = profileCredentials;
        this._sessionToken = _sessionToken;
        // provider is a member variable that may or may not store the actual AuthenticationDetailsProvider.
        // In case of Cloud shell, provider would be the true AuthenticationDetailsProvider.
        this.provider = null;
    }
    /**
     * Get the key id to sign the http request.
     */
    getKeyId() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.tenancy + "/" + this.user + "/" + this.fingerprint;
        });
    }
    /**
     * Get the private key to sign the http request.
     */
    getPrivateKey() {
        return this.privateKey;
    }
    /**
     * Get the passphrase of the private key to sign the http request.
     */
    getPassphrase() {
        return this.passphrase;
    }
    /**
     * Get the Tenancy
     */
    getTenantId() {
        return this.tenancy;
    }
    /**
     * Get the user
     */
    getUser() {
        return this.user;
    }
    /**
     * Get the fingerprint
     */
    getFingerprint() {
        return this.fingerprint;
    }
    /**
     * Get the region
     */
    getRegion() {
        return this.region;
    }
    /**
     * Set the region
     */
    setRegion(region) {
        this.region = region;
    }
    /**
     * Get the authType
     */
    getAuthType() {
        return this.authType;
    }
    /**
     * Set the provider
     */
    setProvider(provider) {
        this.provider = provider;
    }
    /**
     * Get the provider
     */
    getProvider() {
        return this.provider;
    }
    /**
     * Get the delegation token
     */
    getDelegationToken() {
        return this.delegationToken;
    }
    getProfileCredentials() {
        return this.profileCredentials;
    }
    get sessionToken() {
        return this._sessionToken;
    }
    set sessionToken(token) {
        this._sessionToken = token;
    }
}
exports.SimpleAuthenticationDetailsProvider = SimpleAuthenticationDetailsProvider;
//# sourceMappingURL=auth.js.map