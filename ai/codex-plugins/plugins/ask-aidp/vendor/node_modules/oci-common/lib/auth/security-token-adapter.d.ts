/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
import SessionKeySupplier from "./models/session-key-supplier";
/**
 * Helper class to store security token and sessionKeySupplier
 * contain methods to check if token is expired and needs to be refreshed
 */
export default class SecurityTokenAdapter {
    private securityToken;
    private sessionKeySupplier;
    private jwt;
    constructor(securityToken: string, sessionKeySupplier: SessionKeySupplier);
    /**
     * Getter for securityToken
     * @returns securityToken
     */
    getSecurityToken(): string;
    /**
     * Helper method to verify token's secret matches publicKey string
     * then parse token string into an complete object (header, payload, signature)
     * @param token: string
     * @returns payload: object
     */
    parse(token: string): object;
    /**
     * Checks to see current token exists, and if so, check expiration time
     * @return true if valid
     */
    isValid(): boolean;
    /**
     * Return a claim from the token given a key if it exist, else return null
     * @param key: string
     * @return value of the claim
     */
    getStringClaim(key: string): string | null;
}
