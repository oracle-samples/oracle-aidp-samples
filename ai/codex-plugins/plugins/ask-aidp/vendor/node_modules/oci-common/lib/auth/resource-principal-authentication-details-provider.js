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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
var _a;
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * This constructs a default implementation of the {@link ResourcePrincipalAuthenticationDetailsProvider}, constructed
 * in accordance with the following environment variable settings:
 * <ul>
 *
 * <li>{@code OCI_RESOURCE_PRINCIPAL_VERSION}:
 * <p>permitted values are "2.2"</p>
 * </li>
 *
 * <li>{@code OCI_RESOURCE_PRINCIPAL_RPST}:
 * <p>If this is an absolute path, then the filesystem-supplied resource principal session token will be retrieved from
 *   that location. This mode supports token refresh (if the environment replaces the RPST in the filesystem).</p>
 * <p>Otherwise, the environment variable is taken to hold the raw value of an RPST.
 *   Under these circumstances, the RPST cannot be refreshed; consequently, this mode is only usable for short-lived
 *   executables.</p>
 * </li>
 * <li>{@code OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM}:
 * <p>If this is an absolute path, then the filesystem-supplied private key will be retrieved from that location. As
 *   with the OCI_RESOURCE_PRINCIPAL_RPST, this mode supports token refresh if the environment can update the file
 *   contents.</p>
 * <p>Otherwise, the value is interpreted as the direct injection of a private key. The same considerations as to the
 *   lifetime of this value apply when directly injecting a key.</p>
 * </li>
 * <li>{@code OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE}:
 * <p>This is optional. If set, it contains either the location (as an absolute path) or the value of the passphrase
 *   associated with the private key.</p>
 * </li>
 * <li>{@code OCI_RESOURCE_PRINCIPAL_REGION}:
 * <p>If set, this holds the canonical form of the local region. This is intended to enable executables to locate their
 *   "local" OCI service endpoints.</p>
 * </li>
 * </ul>
 */
const path_1 = __importDefault(require("path"));
const region_1 = require("../region");
const fixed_content_key_supplier_1 = __importDefault(require("./fixed-content-key-supplier"));
const file_based_key_supplier_1 = __importDefault(require("./file-based-key-supplier"));
const abstract_requesting_authentication_detail_provider_1 = __importDefault(require("./abstract-requesting-authentication-detail-provider"));
const file_based_resource_principal_federation_client_1 = __importDefault(require("./file-based-resource-principal-federation-client"));
const fixed_content_resource_principal_federation_client_1 = __importDefault(require("./fixed-content-resource-principal-federation-client"));
const RP_DEBUG_INFORMATION_LOG = "Resource principals authentication can only be used in certain OCI services. Please check that the OCI service you're running this code from supports Resource principals. See https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdk_authentication_methods.htm#sdk_authentication_methods_resource_principal for more info.";
class ResourcePrincipalAuthenticationDetailsProvider extends abstract_requesting_authentication_detail_provider_1.default {
    constructor(federationClient, sessionKeySupplier, region) {
        super(federationClient, sessionKeySupplier);
        this.federationClient = federationClient;
        this.sessionKeySupplier = sessionKeySupplier;
        this.region = region;
    }
    // Builder method to create ResourcePrincipalAuthenticationDetailsProviderBuilder which will build
    // ResourcePrincipalAuthenticationDetailsProvider
    static builder() {
        return new ResourcePrincipalAuthenticationDetailsProvider.ResourcePrincipalAuthenticationDetailsProviderBuilder().build();
    }
    /**
     * Session tokens carry JWT-like claims. Permit the retrieval of the value of those
     * claims from the token.
     * At the least, the token should carry claims for {@link ClaimKeys#COMPARTMENT_ID_CLAIM_KEY} and {@link ClaimKeys#TENANT_ID_CLAIM_KEY}
     * @param key the name of a claim in the session token
     * @return the claim value.
     */
    getStringClaim(key) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.federationClient.getStringClaim(key);
        });
    }
    /**
     * Refreshes the authentication data used by the provider
     * @return the refreshed authentication data
     */
    refresh() {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.federationClient.refreshAndGetSecurityToken();
        });
    }
    // region getter
    getRegion() {
        return this.region;
    }
}
exports.default = ResourcePrincipalAuthenticationDetailsProvider;
ResourcePrincipalAuthenticationDetailsProvider.OCI_RESOURCE_PRINCIPAL_VERSION = "OCI_RESOURCE_PRINCIPAL_VERSION";
ResourcePrincipalAuthenticationDetailsProvider.RP_VERSION_2_2 = "2.2";
ResourcePrincipalAuthenticationDetailsProvider.OCI_RESOURCE_PRINCIPAL_RPST = "OCI_RESOURCE_PRINCIPAL_RPST";
ResourcePrincipalAuthenticationDetailsProvider.OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM = "OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM";
ResourcePrincipalAuthenticationDetailsProvider.OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE = "OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE";
ResourcePrincipalAuthenticationDetailsProvider.OCI_RESOURCE_PRINCIPAL_REGION_ENV_VAR_NAME = "OCI_RESOURCE_PRINCIPAL_REGION";
ResourcePrincipalAuthenticationDetailsProvider.ClaimKeys = (_a = class ClaimsKey {
    },
    /**
     * COMPARTMENT_ID is the claim name that the RPST holds for the resource compartment.
     * This can be passed to {@link #getStringClaim} to retrieve the resource's compartment OCID.
     */
    _a.COMPARTMENT_ID_CLAIM_KEY = "res_compartment",
    /**
     * TENANT_ID_CLAIM_KEY is the claim name that the RPST holds for the resource tenancy.
     * This can be passed to {@link #getStringClaim} to retrieve the resource's tenancy OCID.
     */
    _a.TENANT_ID_CLAIM_KEY = "res_tenant",
    _a);
/**
 * Builder for ResourcePrincipalAuthenticationDetailsProvider that understands the V2.2 configuration
 */
ResourcePrincipalAuthenticationDetailsProvider.ResourcePrincipalAuthenticationDetailsProviderBuilder = class ResourcePrincipalAuthenticationDetailsProviderBuilder {
    constructor() { }
    build() {
        const OciResourcePrincipalVersion = process.env[ResourcePrincipalAuthenticationDetailsProvider.OCI_RESOURCE_PRINCIPAL_VERSION];
        if (!OciResourcePrincipalVersion) {
            throw Error("OCI_RESOURCE_PRINCIPAL_VERSION environment variable is missing " +
                RP_DEBUG_INFORMATION_LOG);
        }
        switch (OciResourcePrincipalVersion) {
            case ResourcePrincipalAuthenticationDetailsProvider.RP_VERSION_2_2:
                return ResourcePrincipalAuthenticationDetailsProviderBuilder.build_2_2();
            default:
                throw Error(`OCI_RESOURCE_PRINCIPAL_VERSION environment variable has an unknown value ${OciResourcePrincipalVersion}. ${RP_DEBUG_INFORMATION_LOG}`);
        }
    }
    /**
     * Helper method that interprets the runtime environment to build a v2.2-configured client
     * @return ResourcePrincipalAuthenticationDetailsProvider
     */
    static build_2_2() {
        let federationClient;
        let sessionKeySupplier;
        let region;
        const ociResourcePrincipalPrivateKey = process.env[ResourcePrincipalAuthenticationDetailsProvider.OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM];
        const ociResourcePrincipalPassphrase = process.env[ResourcePrincipalAuthenticationDetailsProvider
            .OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE];
        const ociResourcePrincipalRPST = process.env[ResourcePrincipalAuthenticationDetailsProvider.OCI_RESOURCE_PRINCIPAL_RPST];
        const ociResourcePrincipalRegion = process.env[ResourcePrincipalAuthenticationDetailsProvider.OCI_RESOURCE_PRINCIPAL_REGION_ENV_VAR_NAME];
        if (!ociResourcePrincipalPrivateKey) {
            throw Error("OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM environment variable missing. " +
                RP_DEBUG_INFORMATION_LOG);
        }
        if (!ociResourcePrincipalRPST) {
            throw Error("OCI_RESOURCE_PRINCIPAL_RPST environment variable is missing. " + RP_DEBUG_INFORMATION_LOG);
        }
        if (!ociResourcePrincipalRegion) {
            throw Error("OCI_RESOURCE_PRINCIPAL_REGION_ENV_VAR_NAME environment variable missing. " +
                RP_DEBUG_INFORMATION_LOG);
        }
        // Do a check to see if the file path of privateKey and passphrase are absolute path
        if (path_1.default.isAbsolute(ociResourcePrincipalPrivateKey)) {
            if (ociResourcePrincipalPassphrase && !path_1.default.isAbsolute(ociResourcePrincipalPassphrase)) {
                throw Error("cannot mix path and constant settings for \
          OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM and OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE");
            }
            sessionKeySupplier = new file_based_key_supplier_1.default(ociResourcePrincipalPrivateKey, ociResourcePrincipalPassphrase);
        }
        else {
            sessionKeySupplier = new fixed_content_key_supplier_1.default(ociResourcePrincipalPrivateKey, ociResourcePrincipalPassphrase);
        }
        if (path_1.default.isAbsolute(ociResourcePrincipalRPST)) {
            federationClient = new file_based_resource_principal_federation_client_1.default(sessionKeySupplier, ociResourcePrincipalRPST);
        }
        else {
            federationClient = new fixed_content_resource_principal_federation_client_1.default(ociResourcePrincipalRPST, sessionKeySupplier);
        }
        //  The region should be something like "us-phoenix-1" but if we get "phx" then convert it.
        const regionId = region_1.Region.getRegionIdFromShortCode(ociResourcePrincipalRegion);
        region = region_1.Region.fromRegionId(regionId);
        return new ResourcePrincipalAuthenticationDetailsProvider(federationClient, sessionKeySupplier, region);
    }
};
//# sourceMappingURL=resource-principal-authentication-details-provider.js.map