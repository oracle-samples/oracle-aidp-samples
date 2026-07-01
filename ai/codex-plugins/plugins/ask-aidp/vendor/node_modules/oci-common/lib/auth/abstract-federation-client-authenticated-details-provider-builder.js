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
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * Abstract builder base class for authentication details provider extending
 * @param <B> builder class
 * @param <P> provider class
 */
const endpoint_builder_1 = require("../endpoint-builder");
const http_1 = require("../http");
const region_1 = require("../region");
const realm_1 = require("../realm");
const auth_utils_1 = __importDefault(require("./helpers/auth-utils"));
const X509_federation_client_1 = __importDefault(require("./X509-federation-client"));
const session_key_supplier_1 = __importDefault(require("./session-key-supplier"));
const url_based_x509_certificate_supplier_1 = require("./url-based-x509-certificate-supplier");
const circuit_breaker_1 = __importDefault(require("../circuit-breaker"));
const log_1 = require("../log");
const INSTANCE_PRINCIPAL_GENERIC_ERROR = "Instance principals authentication can only be used on OCI compute instances. Please confirm this code is running on an OCI compute instance. See https://docs.oracle.com/en-us/iaas/Content/Identity/Tasks/callingservicesfrominstances.htm for more info.";
class AbstractFederationClientAuthenticationDetailsProviderBuilder {
    constructor() {
        // url template for auth service.
        this.template = "https://auth.{region}.{secondLevelDomain}";
        // Default base url of metadata service.
        this.METADATA_SERVICE_BASE_URL = "http://169.254.169.254/opc/v2/";
        // The Authorization header value to be sent for requests to the metadata service.
        this.METADATA_AUTH_HEADERS = "Bearer Oracle";
        this.AUTHORIZATION = "Authorization";
        // Base url of metadata service.
        this._metadataBaseUrl = this.METADATA_SERVICE_BASE_URL;
        // The federation endpoint url.
        this._federationEndpoint = "";
        //Tenancy OCI, or empty string if detecting from instance metadata.
        this._tenancyId = "";
        // Purpose: Specifies the non-default purpose of the session token to be requested. For internal use only.
        this._purpose = "DEFAULT";
        this._circuitBreaker = new circuit_breaker_1.default({
            timeout: 10000,
            errorThresholdPercentage: 50,
            resetTimeout: 30000 // After 30 seconds, try again.
        }).circuit;
        this._authCircuitBreaker = new circuit_breaker_1.default(circuit_breaker_1.default.defaultAuthConfiguration);
    }
    // metadataBaseUrl getter
    get metadataBaseUrl() {
        return this._metadataBaseUrl;
    }
    // federationEndpoint getter
    get federationEndpoint() {
        return this._federationEndpoint;
    }
    // LeafCertificateSupplier getter
    get leafCertificateSupplier() {
        return this._leafCertificateSupplier;
    }
    // tenancyId getter
    get tenancyId() {
        return this._tenancyId;
    }
    get circuitBreaker() {
        return this._circuitBreaker;
    }
    get authCircuitBreaker() {
        return this._authCircuitBreaker;
    }
    // region getter
    getRegion() {
        return this.region;
    }
    // Configure the metadata endpoint to use when retrieving the instance data and principal for federation.
    set metadataBaseUrl(metadataBaseUrl) {
        this._metadataBaseUrl = metadataBaseUrl;
        if (this._metadataBaseUrl.substr(-1) != "/") {
            this._metadataBaseUrl += "/";
        }
    }
    // Configures the custom federationEndpoint to use.
    set federationEndpoint(federationEndpoint) {
        this._federationEndpoint = federationEndpoint;
    }
    // Configures the custom leafCertificateSupplier to use.
    set leafCertificateSupplier(leafCertificateSupplier) {
        this._leafCertificateSupplier = leafCertificateSupplier;
    }
    // Configure the custom tenancyId to use.
    set tenancyId(tenancyId) {
        this._tenancyId = tenancyId;
    }
    // Configure the purpose to use.
    set purpose(purpose) {
        this._purpose = purpose;
    }
    // Configures the custom sessionKeySupplier to use.
    set sessionKeySupplier(sessionKeySupplier) {
        this._sessionKeySupplier = sessionKeySupplier;
    }
    // Configures the set of intermediate certificate suppliers to use, if any.
    set intermediateCertificateSuppliers(intermediateCertificateSuppliers) {
        this._intermediateCertificateSuppliers = intermediateCertificateSuppliers;
    }
    /**
     * Build a new AuthenticationDetailsProvider that uses the FederationClient.
     *
     * @return A new provider instance.
     */
    build() {
        return __awaiter(this, void 0, void 0, function* () {
            const sessionKeySupplierToUse = this._sessionKeySupplier || new session_key_supplier_1.default();
            this._sessionKeySupplier = sessionKeySupplierToUse;
            this._federationClient = this.createFederationClient(this._sessionKeySupplier);
            return this.buildProvider(this._sessionKeySupplier);
        });
    }
    /**
     * Build the Federation Client
     * @param sessionKeySupplier
     * @return Federation Client
     */
    createFederationClient(sessionKeySupplier) {
        return new X509_federation_client_1.default(this._federationEndpoint, this._tenancyId, this._leafCertificateSupplier, sessionKeySupplier, this._intermediateCertificateSuppliers, this._purpose, this.authCircuitBreaker);
    }
    /**
     * Auto-detect endpoint and certificate information using Instance metadata.
     */
    autoDetectUsingMetadataUrl() {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.autoDetectEndpointUsingMetadataUrl();
            yield this.autoDetectCertificatesUsingMetadataUrl();
        });
    }
    /**
     * Auto detects the endpoint that should be used when talking to OCI Auth, if no endpoint
     * has been configured already.
     * @return The auto-detected, or currently set, auth endpoint.
     */
    autoDetectEndpointUsingMetadataUrl() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this._federationEndpoint === "") {
                const url = this._metadataBaseUrl + "instance/region";
                let headers = new Headers();
                headers.append("accept", "text/plain");
                headers.append("Content-Type", "application/json");
                headers.append(this.AUTHORIZATION, this.METADATA_AUTH_HEADERS);
                const httpClient = new http_1.FetchHttpClient(null, this.circuitBreaker);
                const response = yield httpClient.send({
                    uri: url,
                    method: "GET",
                    headers: headers
                });
                // regionStr can be a shortCode or regionId
                const regionStr = yield response.text();
                const regionId = region_1.Region.getRegionIdFromShortCode(regionStr);
                try {
                    // Try to get region off regionId
                    this.region = region_1.Region.fromRegionId(regionId);
                }
                catch (e) {
                    if (log_1.LOG.logger)
                        log_1.LOG.logger.error(`
          failed reason: ${e},
          Region not supported by this version of the SDK, registering region ${regionId} under OC1
        `);
                    this.region = region_1.Region.register(regionId, realm_1.Realm.OC1);
                }
                try {
                    const endpoint = endpoint_builder_1.EndpointBuilder.createEndpointFromRegion(this.template, this.region //TODO: Find out why there is a type mismatch
                    );
                    this._federationEndpoint = endpoint;
                }
                catch (e) {
                    throw Error(`Endpoint for auth service is not known in region ${this.region}. ${INSTANCE_PRINCIPAL_GENERIC_ERROR}`);
                }
            }
            return this._federationEndpoint;
        });
    }
    /**
     * Auto detects and configures the certificates needed using Instance metadata.
     *
     */
    autoDetectCertificatesUsingMetadataUrl() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                if (!this._leafCertificateSupplier) {
                    this._leafCertificateSupplier = yield new url_based_x509_certificate_supplier_1.URLBasedX509CertificateSupplier(this.getMetaDataResourceDetail("identity/cert.pem", this.circuitBreaker), this.getMetaDataResourceDetail("identity/key.pem", this.circuitBreaker), null).refresh();
                }
                if (this._tenancyId === "") {
                    this._tenancyId = auth_utils_1.default.getTenantIdFromCertificate(this._leafCertificateSupplier.getCertificateAndKeyPair().getCertificate());
                }
                if (!this._intermediateCertificateSuppliers) {
                    this._intermediateCertificateSuppliers = [
                        yield new url_based_x509_certificate_supplier_1.URLBasedX509CertificateSupplier(this.getMetaDataResourceDetail("identity/intermediate.pem", this.circuitBreaker), null, null).refresh()
                    ];
                }
            }
            catch (e) {
                throw Error("Failed to autoDetectCertificatesUsingMetadataUrl. " + INSTANCE_PRINCIPAL_GENERIC_ERROR);
            }
        });
    }
    getMetaDataResourceDetail(path, circuitBreaker) {
        const url = this._metadataBaseUrl + path;
        let headers = new Headers();
        headers.append(this.AUTHORIZATION, this.METADATA_AUTH_HEADERS);
        return new url_based_x509_certificate_supplier_1.ResourceDetails(url, headers, circuitBreaker);
    }
}
exports.default = AbstractFederationClientAuthenticationDetailsProviderBuilder;
//# sourceMappingURL=abstract-federation-client-authenticated-details-provider-builder.js.map