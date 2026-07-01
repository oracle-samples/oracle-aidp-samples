(function webpackUniversalModuleDefinition(root, factory) {
	if(typeof exports === 'object' && typeof module === 'object')
		module.exports = factory();
	else if(typeof define === 'function' && define.amd)
		define([], factory);
	else if(typeof exports === 'object')
		exports["circuitBreaker"] = factory();
	else
		root["circuitBreaker"] = factory();
})(window, function() {
return /******/ (function(modules) { // webpackBootstrap
/******/ 	// The module cache
/******/ 	var installedModules = {};
/******/
/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {
/******/
/******/ 		// Check if module is in cache
/******/ 		if(installedModules[moduleId]) {
/******/ 			return installedModules[moduleId].exports;
/******/ 		}
/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = installedModules[moduleId] = {
/******/ 			i: moduleId,
/******/ 			l: false,
/******/ 			exports: {}
/******/ 		};
/******/
/******/ 		// Execute the module function
/******/ 		modules[moduleId].call(module.exports, module, module.exports, __webpack_require__);
/******/
/******/ 		// Flag the module as loaded
/******/ 		module.l = true;
/******/
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/
/******/
/******/ 	// expose the modules object (__webpack_modules__)
/******/ 	__webpack_require__.m = modules;
/******/
/******/ 	// expose the module cache
/******/ 	__webpack_require__.c = installedModules;
/******/
/******/ 	// define getter function for harmony exports
/******/ 	__webpack_require__.d = function(exports, name, getter) {
/******/ 		if(!__webpack_require__.o(exports, name)) {
/******/ 			Object.defineProperty(exports, name, { enumerable: true, get: getter });
/******/ 		}
/******/ 	};
/******/
/******/ 	// define __esModule on exports
/******/ 	__webpack_require__.r = function(exports) {
/******/ 		if(typeof Symbol !== 'undefined' && Symbol.toStringTag) {
/******/ 			Object.defineProperty(exports, Symbol.toStringTag, { value: 'Module' });
/******/ 		}
/******/ 		Object.defineProperty(exports, '__esModule', { value: true });
/******/ 	};
/******/
/******/ 	// create a fake namespace object
/******/ 	// mode & 1: value is a module id, require it
/******/ 	// mode & 2: merge all properties of value into the ns
/******/ 	// mode & 4: return value when already ns object
/******/ 	// mode & 8|1: behave like require
/******/ 	__webpack_require__.t = function(value, mode) {
/******/ 		if(mode & 1) value = __webpack_require__(value);
/******/ 		if(mode & 8) return value;
/******/ 		if((mode & 4) && typeof value === 'object' && value && value.__esModule) return value;
/******/ 		var ns = Object.create(null);
/******/ 		__webpack_require__.r(ns);
/******/ 		Object.defineProperty(ns, 'default', { enumerable: true, value: value });
/******/ 		if(mode & 2 && typeof value != 'string') for(var key in value) __webpack_require__.d(ns, key, function(key) { return value[key]; }.bind(null, key));
/******/ 		return ns;
/******/ 	};
/******/
/******/ 	// getDefaultExport function for compatibility with non-harmony modules
/******/ 	__webpack_require__.n = function(module) {
/******/ 		var getter = module && module.__esModule ?
/******/ 			function getDefault() { return module['default']; } :
/******/ 			function getModuleExports() { return module; };
/******/ 		__webpack_require__.d(getter, 'a', getter);
/******/ 		return getter;
/******/ 	};
/******/
/******/ 	// Object.prototype.hasOwnProperty.call
/******/ 	__webpack_require__.o = function(object, property) { return Object.prototype.hasOwnProperty.call(object, property); };
/******/
/******/ 	// __webpack_public_path__
/******/ 	__webpack_require__.p = "";
/******/
/******/
/******/ 	// Load entry module and return exports
/******/ 	return __webpack_require__(__webpack_require__.s = "./index.js");
/******/ })
/************************************************************************/
/******/ ({

/***/ "./index.js":
/*!******************!*\
  !*** ./index.js ***!
  \******************/
/*! no static exports found */
/***/ (function(module, exports, __webpack_require__) {

"use strict";


module.exports = exports = __webpack_require__(/*! ./lib/circuit */ "./lib/circuit.js");

/***/ }),

/***/ "./lib/circuit.js":
/*!************************!*\
  !*** ./lib/circuit.js ***!
  \************************/
/*! no static exports found */
/***/ (function(module, exports, __webpack_require__) {

"use strict";
/* WEBPACK VAR INJECTION */(function(console) {

function _toConsumableArray(arr) { return _arrayWithoutHoles(arr) || _iterableToArray(arr) || _unsupportedIterableToArray(arr) || _nonIterableSpread(); }

function _nonIterableSpread() { throw new TypeError("Invalid attempt to spread non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(n); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _iterableToArray(iter) { if (typeof Symbol !== "undefined" && Symbol.iterator in Object(iter)) return Array.from(iter); }

function _arrayWithoutHoles(arr) { if (Array.isArray(arr)) return _arrayLikeToArray(arr); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

function _createSuper(Derived) { return function () { var Super = _getPrototypeOf(Derived), result; if (_isNativeReflectConstruct()) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _possibleConstructorReturn(self, call) { if (call && (_typeof(call) === "object" || typeof call === "function")) { return call; } return _assertThisInitialized(self); }

function _assertThisInitialized(self) { if (self === void 0) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return self; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

function _getPrototypeOf(o) { _getPrototypeOf = Object.setPrototypeOf ? Object.getPrototypeOf : function _getPrototypeOf(o) { return o.__proto__ || Object.getPrototypeOf(o); }; return _getPrototypeOf(o); }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function"); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, writable: true, configurable: true } }); if (superClass) _setPrototypeOf(subClass, superClass); }

function _setPrototypeOf(o, p) { _setPrototypeOf = Object.setPrototypeOf || function _setPrototypeOf(o, p) { o.__proto__ = p; return o; }; return _setPrototypeOf(o, p); }

var EventEmitter = __webpack_require__(/*! events */ "./node_modules/events/events.js");

var Status = __webpack_require__(/*! ./status */ "./lib/status.js");

var Semaphore = __webpack_require__(/*! ./semaphore */ "./lib/semaphore.js");

var STATE = Symbol('state');
var OPEN = Symbol('open');
var CLOSED = Symbol('closed');
var HALF_OPEN = Symbol('half-open');
var PENDING_CLOSE = Symbol('pending-close');
var SHUTDOWN = Symbol('shutdown');
var FALLBACK_FUNCTION = Symbol('fallback');
var STATUS = Symbol('status');
var NAME = Symbol('name');
var GROUP = Symbol('group');
var CACHE = new WeakMap();
var ENABLED = Symbol('Enabled');
var WARMING_UP = Symbol('warming-up');
var VOLUME_THRESHOLD = Symbol('volume-threshold');
var OUR_ERROR = Symbol('our-error');
var RESET_TIMEOUT = Symbol('reset-timeout');
var WARMUP_TIMEOUT = Symbol('warmup-timeout');
var deprecation = "options.maxFailures is deprecated. Please use options.errorThresholdPercentage";
/**
 * Constructs a {@link CircuitBreaker}.
 *
 * @class CircuitBreaker
 * @extends EventEmitter
 * @param {Function} action The action to fire for this {@link CircuitBreaker}
 * @param {Object} options Options for the {@link CircuitBreaker}
 * @param {Number} options.timeout The time in milliseconds that action should
 * be allowed to execute before timing out. Timeout can be disabled by setting
 * this to `false`. Default 10000 (10 seconds)
 * @param {Number} options.maxFailures (Deprecated) The number of times the
 * circuit can fail before opening. Default 10.
 * @param {Number} options.resetTimeout The time in milliseconds to wait before
 * setting the breaker to `halfOpen` state, and trying the action again.
 * Default: 30000 (30 seconds)
 * @param {Number} options.rollingCountTimeout Sets the duration of the
 * statistical rolling window, in milliseconds. This is how long Opossum keeps
 * metrics for the circuit breaker to use and for publishing. Default: 10000
 * @param {Number} options.rollingCountBuckets Sets the number of buckets the
 * rolling statistical window is divided into. So, if
 * options.rollingCountTimeout is 10000, and options.rollingCountBuckets is 10,
 * then the statistical window will be 1000 1 second snapshots in the
 * statistical window. Default: 10
 * @param {String} options.name the circuit name to use when reporting stats.
 * Default: the name of the function this circuit controls.
 * @param {boolean} options.rollingPercentilesEnabled This property indicates
 * whether execution latencies should be tracked and calculated as percentiles.
 * If they are disabled, all summary statistics (mean, percentiles) are
 * returned as -1. Default: false
 * @param {Number} options.capacity the number of concurrent requests allowed.
 * If the number currently executing function calls is equal to
 * options.capacity, further calls to `fire()` are rejected until at least one
 * of the current requests completes. Default: `Number.MAX_SAFE_INTEGER`.
 * @param {Number} options.errorThresholdPercentage the error percentage at
 * which to open the circuit and start short-circuiting requests to fallback.
 * Default: 50
 * @param {boolean} options.enabled whether this circuit is enabled upon
 * construction. Default: true
 * @param {boolean} options.allowWarmUp determines whether to allow failures
 * without opening the circuit during a brief warmup period (this is the
 * `rollingCountTimeout` property). Default: false
 * allow before enabling the circuit. This can help in situations where no
 * matter what your `errorThresholdPercentage` is, if the first execution
 * times out or fails, the circuit immediately opens.
 * @param {Number} options.volumeThreshold the minimum number of requests within
 * the rolling statistical window that must exist before the circuit breaker
 * can open. This is similar to `options.allowWarmUp` in that no matter how many
 * failures there are, if the number of requests within the statistical window
 * does not exceed this threshold, the circuit will remain closed. Default: 0
 * @param {Function} options.errorFilter an optional function that will be
 * called when the circuit's function fails (returns a rejected Promise). If
 * this function returns truthy, the circuit's failPure statistics will not be
 * incremented. This is useful, for example, when you don't want HTTP 404 to
 * trip the circuit, but still want to handle it as a failure case.
 * @param {boolean} options.cache whether the return value of the first
 * successful execution of the circuit's function will be cached. Once a value
 * has been cached that value will be returned for every subsequent execution:
 * the cache can be cleared using `clearCache`. (The metrics `cacheHit` and
 * `cacheMiss` reflect cache activity.) Default: false
 *
 *
 * @fires CircuitBreaker#halfOpen
 * @fires CircuitBreaker#close
 * @fires CircuitBreaker#open
 * @fires CircuitBreaker#fire
 * @fires CircuitBreaker#cacheHit
 * @fires CircuitBreaker#cacheMiss
 * @fires CircuitBreaker#reject
 * @fires CircuitBreaker#timeout
 * @fires CircuitBreaker#success
 * @fires CircuitBreaker#semaphoreLocked
 * @fires CircuitBreaker#healthCheckFailed
 * @fires CircuitBreaker#fallback
 * @fires CircuitBreaker#failure
 */

var CircuitBreaker = /*#__PURE__*/function (_EventEmitter) {
  _inherits(CircuitBreaker, _EventEmitter);

  var _super = _createSuper(CircuitBreaker);

  _createClass(CircuitBreaker, null, [{
    key: "isOurError",

    /**
     * Returns true if the provided error was generated here. It will be false
     * if the error came from the action itself.
     * @param {Error} error The Error to check.
     * @returns {Boolean} true if the error was generated here
     */
    value: function isOurError(error) {
      return !!error[OUR_ERROR];
    }
  }]);

  function CircuitBreaker(action) {
    var _this;

    var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};

    _classCallCheck(this, CircuitBreaker);

    _this = _super.call(this);
    _this.options = options;
    _this.options.timeout = options.timeout === false ? false : options.timeout || 10000;
    _this.options.resetTimeout = options.resetTimeout || 30000;
    _this.options.errorThresholdPercentage = options.errorThresholdPercentage || 50;
    _this.options.rollingCountTimeout = options.rollingCountTimeout || 10000;
    _this.options.rollingCountBuckets = options.rollingCountBuckets || 10;
    _this.options.rollingPercentilesEnabled = options.rollingPercentilesEnabled !== false;
    _this.options.capacity = Number.isInteger(options.capacity) ? options.capacity : Number.MAX_SAFE_INTEGER;

    _this.options.errorFilter = options.errorFilter || function (_) {
      return false;
    };

    _this.semaphore = new Semaphore(_this.options.capacity);
    _this[VOLUME_THRESHOLD] = Number.isInteger(options.volumeThreshold) ? options.volumeThreshold : 0;
    _this[WARMING_UP] = options.allowWarmUp === true;
    _this[STATUS] = new Status(_this.options);
    _this[STATE] = CLOSED;
    _this[FALLBACK_FUNCTION] = null;
    _this[PENDING_CLOSE] = false;
    _this[NAME] = options.name || action.name || nextName();
    _this[GROUP] = options.group || _this[NAME];
    _this[ENABLED] = options.enabled !== false;

    if (_this[WARMING_UP]) {
      var timer = _this[WARMUP_TIMEOUT] = setTimeout(function (_) {
        return _this[WARMING_UP] = false;
      }, _this.options.rollingCountTimeout);

      if (typeof timer.unref === 'function') {
        timer.unref();
      }
    }

    if (typeof action !== 'function') {
      _this.action = function (_) {
        return Promise.resolve(action);
      };
    } else _this.action = action;

    if (options.maxFailures) console.error(deprecation);

    var increment = function increment(property) {
      return function (result, runTime) {
        return _this[STATUS].increment(property, runTime);
      };
    };

    _this.on('success', increment('successes'));

    _this.on('failure', increment('failures'));

    _this.on('fallback', increment('fallbacks'));

    _this.on('timeout', increment('timeouts'));

    _this.on('fire', increment('fires'));

    _this.on('reject', increment('rejects'));

    _this.on('cacheHit', increment('cacheHits'));

    _this.on('cacheMiss', increment('cacheMisses'));

    _this.on('open', function (_) {
      return _this[STATUS].open();
    });

    _this.on('close', function (_) {
      return _this[STATUS].close();
    });

    _this.on('semaphoreLocked', increment('semaphoreRejections'));
    /**
     * @param {CircuitBreaker} circuit This current circuit
     * @returns {function(): void} A bound reset callback
     * @private
     */


    function _startTimer(circuit) {
      return function (_) {
        var timer = circuit[RESET_TIMEOUT] = setTimeout(function () {
          circuit[STATE] = HALF_OPEN;
          circuit[PENDING_CLOSE] = true;
          /**
           * Emitted after `options.resetTimeout` has elapsed, allowing for
           * a single attempt to call the service again. If that attempt is
           * successful, the circuit will be closed. Otherwise it remains open.
           *
           * @event CircuitBreaker#halfOpen
           * @type {Number} how long the circuit remained open
           */

          circuit.emit('halfOpen', circuit.options.resetTimeout);
        }, circuit.options.resetTimeout);

        if (typeof timer.unref === 'function') {
          timer.unref();
        }
      };
    }

    _this.on('open', _startTimer(_assertThisInitialized(_this)));

    _this.on('success', function (_) {
      if (_this.halfOpen) {
        _this.close();
      }
    });

    if (_this.options.cache) {
      CACHE.set(_assertThisInitialized(_this), undefined);
    }

    return _this;
  }
  /**
   * Closes the breaker, allowing the action to execute again
   * @fires CircuitBreaker#close
   * @returns {void}
   */


  _createClass(CircuitBreaker, [{
    key: "close",
    value: function close() {
      if (this[STATE] !== CLOSED) {
        if (this[RESET_TIMEOUT]) {
          clearTimeout(this[RESET_TIMEOUT]);
        }

        this[STATE] = CLOSED;
        this[PENDING_CLOSE] = false;
        /**
         * Emitted when the breaker is reset allowing the action to execute again
         * @event CircuitBreaker#close
         */

        this.emit('close');
      }
    }
    /**
     * Opens the breaker. Each time the breaker is fired while the circuit is
     * opened, a failed Promise is returned, or if any fallback function
     * has been provided, it is invoked.
     *
     * If the breaker is already open this call does nothing.
     * @fires CircuitBreaker#open
     * @returns {void}
     */

  }, {
    key: "open",
    value: function open() {
      if (this[STATE] !== OPEN) {
        this[STATE] = OPEN;
        this[PENDING_CLOSE] = false;
        /**
         * Emitted when the breaker opens because the action has
         * failed more than `options.maxFailures` number of times.
         * @event CircuitBreaker#open
         */

        this.emit('open');
      }
    }
    /**
     * Shuts down this circuit breaker. All subsequent calls to the
     * circuit will fail, returning a rejected promise.
     * @returns {void}
     */

  }, {
    key: "shutdown",
    value: function shutdown() {
      this.disable();
      this.removeAllListeners();

      if (this[RESET_TIMEOUT]) {
        clearTimeout(this[RESET_TIMEOUT]);
      }

      if (this[WARMUP_TIMEOUT]) {
        clearTimeout(this[WARMUP_TIMEOUT]);
      }

      this.status.shutdown();
      this[STATE] = SHUTDOWN;
      /**
       * Emitted when the circuit breaker has been shut down.
       * @event CircuitBreaker#shutdown
       */

      this.emit('shutdown');
    }
    /**
     * Determines if the circuit has been shutdown.
     * @type {Boolean}
     */

  }, {
    key: "fallback",

    /**
     * Provide a fallback function for this {@link CircuitBreaker}. This
     * function will be executed when the circuit is `fire`d and fails.
     * It will always be preceded by a `failure` event, and `breaker.fire` returns
     * a rejected Promise.
     * @param {Function | CircuitBreaker} func the fallback function to execute
     * when the breaker has opened or when a timeout or error occurs.
     * @return {CircuitBreaker} this
     */
    value: function fallback(func) {
      var fb = func;

      if (func instanceof CircuitBreaker) {
        fb = function fb() {
          return func.fire.apply(func, arguments);
        };
      }

      this[FALLBACK_FUNCTION] = fb;
      return this;
    }
    /**
     * Execute the action for this circuit. If the action fails or times out, the
     * returned promise will be rejected. If the action succeeds, the promise will
     * resolve with the resolved value from action. If a fallback function was
     * provided, it will be invoked in the event of any failure or timeout.
     *
     * Any parameters passed to this function will be proxied to the circuit
     * function.
     *
     * @return {Promise<any>} promise resolves with the circuit function's return
     * value on success or is rejected on failure of the action. Use isOurError()
     * to determine if a rejection was a result of the circuit breaker or the
     * action.
     *
     * @fires CircuitBreaker#failure
     * @fires CircuitBreaker#fallback
     * @fires CircuitBreaker#fire
     * @fires CircuitBreaker#reject
     * @fires CircuitBreaker#success
     * @fires CircuitBreaker#timeout
     * @fires CircuitBreaker#semaphoreLocked
     */

  }, {
    key: "fire",
    value: function fire() {
      for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
        args[_key] = arguments[_key];
      }

      return this.call.apply(this, [this.action].concat(args));
    }
    /**
     * Execute the action for this circuit using `context` as `this`.
     * If the action fails or times out, the
     * returned promise will be rejected. If the action succeeds, the promise will
     * resolve with the resolved value from action. If a fallback function was
     * provided, it will be invoked in the event of any failure or timeout.
     *
     * Any parameters in addition to `context will be passed to the
     * circuit function.
     *
     * @param {any} context the `this` context used for function execution
     * @param {any} rest the arguments passed to the action
     *
     * @return {Promise<any>} promise resolves with the circuit function's return
     * value on success or is rejected on failure of the action.
     *
     * @fires CircuitBreaker#failure
     * @fires CircuitBreaker#fallback
     * @fires CircuitBreaker#fire
     * @fires CircuitBreaker#reject
     * @fires CircuitBreaker#success
     * @fires CircuitBreaker#timeout
     * @fires CircuitBreaker#semaphoreLocked
     */

  }, {
    key: "call",
    value: function call(context) {
      var _this2 = this;

      if (this.isShutdown) {
        var err = buildError('The circuit has been shutdown.', 'ESHUTDOWN');
        return Promise.reject(err);
      }

      for (var _len2 = arguments.length, rest = new Array(_len2 > 1 ? _len2 - 1 : 0), _key2 = 1; _key2 < _len2; _key2++) {
        rest[_key2 - 1] = arguments[_key2];
      }

      var args = Array.prototype.slice.call(rest);
      /**
       * Emitted when the circuit breaker action is executed
       * @event CircuitBreaker#fire
       * @type {any} the arguments passed to the fired function
       */

      this.emit('fire', args);

      if (CACHE.get(this) !== undefined) {
        /**
         * Emitted when the circuit breaker is using the cache
         * and finds a value.
         * @event CircuitBreaker#cacheHit
         */
        this.emit('cacheHit');
        return CACHE.get(this);
      } else if (this.options.cache) {
        /**
         * Emitted when the circuit breaker does not find a value in
         * the cache, but the cache option is enabled.
         * @event CircuitBreaker#cacheMiss
         */
        this.emit('cacheMiss');
      }

      if (!this[ENABLED]) {
        var result = this.action.apply(context, args);
        return typeof result.then === 'function' ? result : Promise.resolve(result);
      }

      if (!this.closed && !this.pendingClose) {
        /**
         * Emitted when the circuit breaker is open and failing fast
         * @event CircuitBreaker#reject
         * @type {Error}
         */
        var error = buildError('Breaker is open', 'EOPENBREAKER');
        this.emit('reject', error);
        return fallback(this, error, args) || Promise.reject(error);
      }

      this[PENDING_CLOSE] = false;
      var timeout;
      var timeoutError = false;
      return new Promise(function (resolve, reject) {
        var latencyStartTime = Date.now();

        if (_this2.semaphore.test()) {
          if (_this2.options.timeout) {
            timeout = setTimeout(function () {
              timeoutError = true;
              var error = buildError("Timed out after ".concat(_this2.options.timeout, "ms"), 'ETIMEDOUT');
              var latency = Date.now() - latencyStartTime;

              _this2.semaphore.release();
              /**
               * Emitted when the circuit breaker action takes longer than
               * `options.timeout`
               * @event CircuitBreaker#timeout
               * @type {Error}
               */


              _this2.emit('timeout', error, latency, args);

              handleError(error, _this2, timeout, args, latency, resolve, reject);
            }, _this2.options.timeout);
          }

          try {
            var _result = _this2.action.apply(context, args);

            var promise = typeof _result.then === 'function' ? _result : Promise.resolve(_result);
            promise.then(function (result) {
              if (!timeoutError) {
                clearTimeout(timeout);
                /**
                 * Emitted when the circuit breaker action succeeds
                 * @event CircuitBreaker#success
                 * @type {any} the return value from the circuit
                 */

                _this2.emit('success', result, Date.now() - latencyStartTime);

                _this2.semaphore.release();

                resolve(result);

                if (_this2.options.cache) {
                  CACHE.set(_this2, promise);
                }
              }
            })["catch"](function (error) {
              if (!timeoutError) {
                _this2.semaphore.release();

                var latencyEndTime = Date.now() - latencyStartTime;
                handleError(error, _this2, timeout, args, latencyEndTime, resolve, reject);
              }
            });
          } catch (error) {
            _this2.semaphore.release();

            var latency = Date.now() - latencyStartTime;
            handleError(error, _this2, timeout, args, latency, resolve, reject);
          }
        } else {
          var _latency = Date.now() - latencyStartTime;

          var _err = buildError('Semaphore locked', 'ESEMLOCKED');
          /**
           * Emitted when the rate limit has been reached and there
           * are no more locks to be obtained.
           * @event CircuitBreaker#semaphoreLocked
           * @type {Error}
           */


          _this2.emit('semaphoreLocked', _err, _latency);

          handleError(_err, _this2, timeout, args, _latency, resolve, reject);
        }
      });
    }
    /**
     * Clears the cache of this {@link CircuitBreaker}
     * @returns {void}
     */

  }, {
    key: "clearCache",
    value: function clearCache() {
      CACHE.set(this, undefined);
    }
    /**
     * Provide a health check function to be called periodically. The function
     * should return a Promise. If the promise is rejected the circuit will open.
     * This is in addition to the existing circuit behavior as defined by
     * `options.errorThresholdPercentage` in the constructor. For example, if the
     * health check function provided here always returns a resolved promise, the
     * circuit can still trip and open if there are failures exceeding the
     * configured threshold. The health check function is executed within the
     * circuit breaker's execution context, so `this` within the function is the
     * circuit breaker itself.
     *
     * @param {Function} func a health check function which returns a promise.
     * @param {Number} [interval] the amount of time between calls to the health
     * check function. Default: 5000 (5 seconds)
     *
     * @returns {void}
     *
     * @fires CircuitBreaker#healthCheckFailed
     * @throws {TypeError} if `interval` is supplied but not a number
     */

  }, {
    key: "healthCheck",
    value: function healthCheck(func, interval) {
      var _this3 = this;

      interval = interval || 5000;

      if (typeof func !== 'function') {
        throw new TypeError('Health check function must be a function');
      }

      if (isNaN(interval)) {
        throw new TypeError('Health check interval must be a number');
      }

      var check = function check(_) {
        func.apply(_this3)["catch"](function (e) {
          /**
           * Emitted with the user-supplied health check function
           * returns a rejected promise.
           * @event CircuitBreaker#healthCheckFailed
           * @type {Error}
           */
          _this3.emit('healthCheckFailed', e);

          _this3.open();
        });
      };

      var timer = setInterval(check, interval);

      if (typeof timer.unref === 'function') {
        timer.unref();
      }

      check();
    }
    /**
     * Enables this circuit. If the circuit is the  disabled
     * state, it will be re-enabled. If not, this is essentially
     * a noop.
     * @returns {void}
     */

  }, {
    key: "enable",
    value: function enable() {
      this[ENABLED] = true;
    }
    /**
     * Disables this circuit, causing all calls to the circuit's function
     * to be executed without circuit or fallback protection.
     * @returns {void}
     */

  }, {
    key: "disable",
    value: function disable() {
      this[ENABLED] = false;
    }
  }, {
    key: "isShutdown",
    get: function get() {
      return this[STATE] === SHUTDOWN;
    }
    /**
     * Gets the name of this circuit
     * @type {String}
     */

  }, {
    key: "name",
    get: function get() {
      return this[NAME];
    }
    /**
     * Gets the name of this circuit group
     * @type {String}
     */

  }, {
    key: "group",
    get: function get() {
      return this[GROUP];
    }
    /**
     * Gets whether this circuit is in the `pendingClosed` state
     * @type {Boolean}
     */

  }, {
    key: "pendingClose",
    get: function get() {
      return this[PENDING_CLOSE];
    }
    /**
     * True if the circuit is currently closed. False otherwise.
     * @type {Boolean}
     */

  }, {
    key: "closed",
    get: function get() {
      return this[STATE] === CLOSED;
    }
    /**
     * True if the circuit is currently opened. False otherwise.
     * @type {Boolean}
     */

  }, {
    key: "opened",
    get: function get() {
      return this[STATE] === OPEN;
    }
    /**
     * True if the circuit is currently half opened. False otherwise.
     * @type {Boolean}
     */

  }, {
    key: "halfOpen",
    get: function get() {
      return this[STATE] === HALF_OPEN;
    }
    /**
     * The current {@link Status} of this {@link CircuitBreaker}
     * @type {Status}
     */

  }, {
    key: "status",
    get: function get() {
      return this[STATUS];
    }
    /**
     * Get the current stats for the circuit.
     * @see Status#stats
     * @type {Object}
     */

  }, {
    key: "stats",
    get: function get() {
      return this[STATUS].stats;
    }
    /**
     * Gets whether the circuit is enabled or not
     * @type {Boolean}
     */

  }, {
    key: "enabled",
    get: function get() {
      return this[ENABLED];
    }
    /**
     * Gets whether the circuit is currently in warm up phase
     * @type {Boolean}
     */

  }, {
    key: "warmUp",
    get: function get() {
      return this[WARMING_UP];
    }
    /**
     * Gets the volume threshold for this circuit
     * @type {Boolean}
     */

  }, {
    key: "volumeThreshold",
    get: function get() {
      return this[VOLUME_THRESHOLD];
    }
  }]);

  return CircuitBreaker;
}(EventEmitter);

function handleError(error, circuit, timeout, args, latency, resolve, reject) {
  clearTimeout(timeout);

  if (circuit.options.errorFilter(error)) {
    circuit.emit('success', error, latency);
  } else {
    fail(circuit, error, args, latency);
  }

  var fb = fallback(circuit, error, args);
  if (fb) resolve(fb);else reject(error);
}

function fallback(circuit, err, args) {
  if (circuit[FALLBACK_FUNCTION]) {
    var result = circuit[FALLBACK_FUNCTION].apply(circuit[FALLBACK_FUNCTION], [].concat(_toConsumableArray(args), [err]));
    /**
     * Emitted when the circuit breaker executes a fallback function
     * @event CircuitBreaker#fallback
     * @type {any} the return value of the fallback function
     */

    circuit.emit('fallback', result, err);
    if (result instanceof Promise) return result;
    return Promise.resolve(result);
  }
}

function fail(circuit, err, args, latency) {
  /**
   * Emitted when the circuit breaker action fails
   * @event CircuitBreaker#failure
   * @type {Error}
   */
  circuit.emit('failure', err, latency, args);
  if (circuit.warmUp) return; // check stats to see if the circuit should be opened

  var stats = circuit.stats;
  if (stats.fires < circuit.volumeThreshold && !circuit.halfOpen) return;
  var errorRate = stats.failures / stats.fires * 100;

  if (errorRate > circuit.options.errorThresholdPercentage || stats.failures >= circuit.options.maxFailures || circuit.halfOpen) {
    circuit.open();
  }
}

function buildError(msg, code) {
  var error = new Error(msg);
  error.code = code;
  error[OUR_ERROR] = true;
  return error;
} // http://stackoverflow.com/a/2117523


var nextName = function nextName() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    var r = Math.random() * 16 | 0;
    var v = c === 'x' ? r : r & 0x3 | 0x8;
    return v.toString(16);
  });
};

module.exports = exports = CircuitBreaker;
/* WEBPACK VAR INJECTION */}.call(this, __webpack_require__(/*! ./../node_modules/console-browserify/index.js */ "./node_modules/console-browserify/index.js")))

/***/ }),

/***/ "./lib/semaphore.js":
/*!**************************!*\
  !*** ./lib/semaphore.js ***!
  \**************************/
/*! no static exports found */
/***/ (function(module, exports, __webpack_require__) {

"use strict";


module.exports = exports = semaphore;

function semaphore(count) {
  var resolvers = [];
  var counter = count;
  var sem = {
    take: take,
    release: release,
    test: test
  };
  Object.defineProperty(sem, 'count', {
    get: function get(_) {
      return counter;
    },
    enumerable: true
  });
  return sem;

  function take(timeout) {
    if (counter > 0) {
      --counter;
      return Promise.resolve(release);
    }

    return new Promise(function (resolve, reject) {
      resolvers.push(function (_) {
        --counter;
        resolve(release);
      });

      if (timeout) {
        setTimeout(function (_) {
          resolvers.shift();
          var err = new Error("Timed out after ".concat(timeout, "ms"));
          err.code = 'ETIMEDOUT';
          reject(err);
        }, timeout);
      }
    });
  }

  function release() {
    counter++;

    if (resolvers.length > 0) {
      resolvers.shift()();
    }
  }

  function test() {
    if (counter < 1) return false;
    return take() && true;
  }
}

/***/ }),

/***/ "./lib/status.js":
/*!***********************!*\
  !*** ./lib/status.js ***!
  \***********************/
/*! no static exports found */
/***/ (function(module, exports, __webpack_require__) {

"use strict";


function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

function _createSuper(Derived) { return function () { var Super = _getPrototypeOf(Derived), result; if (_isNativeReflectConstruct()) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _possibleConstructorReturn(self, call) { if (call && (_typeof(call) === "object" || typeof call === "function")) { return call; } return _assertThisInitialized(self); }

function _assertThisInitialized(self) { if (self === void 0) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return self; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

function _getPrototypeOf(o) { _getPrototypeOf = Object.setPrototypeOf ? Object.getPrototypeOf : function _getPrototypeOf(o) { return o.__proto__ || Object.getPrototypeOf(o); }; return _getPrototypeOf(o); }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function"); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, writable: true, configurable: true } }); if (superClass) _setPrototypeOf(subClass, superClass); }

function _setPrototypeOf(o, p) { _setPrototypeOf = Object.setPrototypeOf || function _setPrototypeOf(o, p) { o.__proto__ = p; return o; }; return _setPrototypeOf(o, p); }

var WINDOW = Symbol('window');
var BUCKETS = Symbol('buckets');
var TIMEOUT = Symbol('timeout');
var PERCENTILES = Symbol('percentiles');
var BUCKET_INTERVAL = Symbol('bucket-interval');
var SNAPSHOT_INTERVAL = Symbol('snapshot-interval');

var EventEmitter = __webpack_require__(/*! events */ "./node_modules/events/events.js").EventEmitter;
/**
 * Tracks execution status for a given {@link CircuitBreaker}.
 * A Status instance is created for every {@link CircuitBreaker}
 * and does not typically need to be created by a user.
 *
 * A Status instance will listen for all events on the {@link CircuitBreaker}
 * and track them in a rolling statistical window. The window duration is
 * determined by the `rollingCountTimeout` option provided to the
 * {@link CircuitBreaker}. The window consists of an array of Objects,
 * each representing the counts for a {@link CircuitBreaker}'s events.
 *
 * The array's length is determined by the {@link CircuitBreaker}'s
 * `rollingCountBuckets` option. The duration of each slice of the window
 * is determined by dividing the `rollingCountTimeout` by
 * `rollingCountBuckets`.
 *
 * @class Status
 * @extends EventEmitter
 * @param {Object} options for the status window
 * @param {Number} options.rollingCountBuckets number of buckets in the window
 * @param {Number} options.rollingCountTimeout the duration of the window
 * @param {Boolean} options.rollingPercentilesEnabled whether to calculate
 * percentiles
 * @example
 * // Creates a 1 second window consisting of ten time slices,
 * // each 100ms long.
 * const circuit = circuitBreaker(fs.readFile,
 *  { rollingCountBuckets: 10, rollingCountTimeout: 1000});
 *
 * // get the cumulative statistics for the last second
 * circuit.status.stats;
 *
 * // get the array of 10, 1 second time slices for the last second
 * circuit.status.window;
 * @fires Status#snapshot
 * @see CircuitBreaker#status
 */


var Status = /*#__PURE__*/function (_EventEmitter) {
  _inherits(Status, _EventEmitter);

  var _super = _createSuper(Status);

  function Status(options) {
    var _this;

    _classCallCheck(this, Status);

    _this = _super.call(this); // Set up our statistical rolling window

    _this[BUCKETS] = options.rollingCountBuckets;
    _this[TIMEOUT] = options.rollingCountTimeout;
    _this[WINDOW] = new Array(_this[BUCKETS]);
    _this[PERCENTILES] = [0.0, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.995, 1]; // Default this value to true

    _this.rollingPercentilesEnabled = options.rollingPercentilesEnabled; // prime the window with buckets

    for (var i = 0; i < _this[BUCKETS]; i++) {
      _this[WINDOW][i] = bucket();
    } // rotate the buckets periodically


    var bucketInterval = Math.floor(_this[TIMEOUT] / _this[BUCKETS]);
    _this[BUCKET_INTERVAL] = setInterval(nextBucket(_this[WINDOW]), bucketInterval); // No unref() in the browser

    if (typeof _this[BUCKET_INTERVAL].unref === 'function') {
      _this[BUCKET_INTERVAL].unref();
    }
    /**
     * Emitted at each time-slice. Listeners for this
     * event will receive a cumulative snapshot of the current status window.
     * @event Status#snapshot
     * @type {Object}
     */


    _this[SNAPSHOT_INTERVAL] = setInterval(function (_) {
      return _this.emit('snapshot', _this.stats);
    }, bucketInterval);

    if (typeof _this[SNAPSHOT_INTERVAL].unref === 'function') {
      _this[SNAPSHOT_INTERVAL].unref();
    }

    return _this;
  }
  /**
   * Get the cumulative stats for the current window
   * @type {Object}
   */


  _createClass(Status, [{
    key: "increment",
    value: function increment(property, latencyRunTime) {
      this[WINDOW][0][property]++;

      if (property === 'successes' || property === 'failures' || property === 'timeouts') {
        this[WINDOW][0].latencyTimes.push(latencyRunTime || 0);
      }
    }
  }, {
    key: "open",
    value: function open() {
      this[WINDOW][0].isCircuitBreakerOpen = true;
    }
  }, {
    key: "close",
    value: function close() {
      this[WINDOW][0].isCircuitBreakerOpen = false;
    }
  }, {
    key: "shutdown",
    value: function shutdown() {
      this.removeAllListeners();
      clearInterval(this[BUCKET_INTERVAL]);
      clearInterval(this[SNAPSHOT_INTERVAL]);
    }
  }, {
    key: "stats",
    get: function get() {
      var _this2 = this;

      var totals = this[WINDOW].reduce(function (acc, val) {
        if (!val) {
          return acc;
        }

        Object.keys(acc).forEach(function (key) {
          if (key !== 'latencyTimes' && key !== 'percentiles') {
            acc[key] += val[key] || 0;
          }
        });

        if (_this2.rollingPercentilesEnabled) {
          acc.latencyTimes.push.apply(acc.latencyTimes, val.latencyTimes || []);
        }

        return acc;
      }, bucket());

      if (this.rollingPercentilesEnabled) {
        // Sort the latencyTimes
        totals.latencyTimes.sort(function (a, b) {
          return a - b;
        }); // Get the mean latency
        // Mean = sum of all values in the array/length of array

        if (totals.latencyTimes.length) {
          totals.latencyMean = totals.latencyTimes.reduce(function (a, b) {
            return a + b;
          }, 0) / totals.latencyTimes.length;
        } else {
          totals.latencyMean = 0;
        } // Calculate Percentiles


        this[PERCENTILES].forEach(function (percentile) {
          totals.percentiles[percentile] = calculatePercentile(percentile, totals.latencyTimes);
        });
      } else {
        totals.latencyMean = -1;
        this[PERCENTILES].forEach(function (percentile) {
          totals.percentiles[percentile] = -1;
        });
      }

      return totals;
    }
    /**
     * Gets the stats window as an array of time-sliced objects.
     * @type {Array}
     */

  }, {
    key: "window",
    get: function get() {
      return this[WINDOW].slice();
    }
  }]);

  return Status;
}(EventEmitter);

var nextBucket = function nextBucket(window) {
  return function (_) {
    window.pop();
    window.unshift(bucket());
  };
};

var bucket = function bucket(_) {
  return {
    failures: 0,
    fallbacks: 0,
    successes: 0,
    rejects: 0,
    fires: 0,
    timeouts: 0,
    cacheHits: 0,
    cacheMisses: 0,
    semaphoreRejections: 0,
    percentiles: {},
    latencyTimes: []
  };
};

function calculatePercentile(percentile, arr) {
  if (percentile === 0) {
    return arr[0] || 0;
  }

  var idx = Math.ceil(percentile * arr.length);
  return arr[idx - 1] || 0;
}

module.exports = exports = Status;

/***/ }),

/***/ "./node_modules/assert/assert.js":
/*!***************************************!*\
  !*** ./node_modules/assert/assert.js ***!
  \***************************************/
/*! no static exports found */
/***/ (function(module, exports, __webpack_require__) {

"use strict";
/* WEBPACK VAR INJECTION */(function(global) {

var objectAssign = __webpack_require__(/*! object-assign */ "./node_modules/object-assign/index.js");

// compare and isBuffer taken from https://github.com/feross/buffer/blob/680e9e5e488f22aac27599a57dc844a6315928dd/index.js
// original notice:

/*!
 * The buffer module from node.js, for the browser.
 *
 * @author   Feross Aboukhadijeh <feross@feross.org> <http://feross.org>
 * @license  MIT
 */
function compare(a, b) {
  if (a === b) {
    return 0;
  }

  var x = a.length;
  var y = b.length;

  for (var i = 0, len = Math.min(x, y); i < len; ++i) {
    if (a[i] !== b[i]) {
      x = a[i];
      y = b[i];
      break;
    }
  }

  if (x < y) {
    return -1;
  }
  if (y < x) {
    return 1;
  }
  return 0;
}
function isBuffer(b) {
  if (global.Buffer && typeof global.Buffer.isBuffer === 'function') {
    return global.Buffer.isBuffer(b);
  }
  return !!(b != null && b._isBuffer);
}

// based on node assert, original notice:
// NB: The URL to the CommonJS spec is kept just for tradition.
//     node-assert has evolved a lot since then, both in API and behavior.

// http://wiki.commonjs.org/wiki/Unit_Testing/1.0
//
// THIS IS NOT TESTED NOR LIKELY TO WORK OUTSIDE V8!
//
// Originally from narwhal.js (http://narwhaljs.org)
// Copyright (c) 2009 Thomas Robinson <280north.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the 'Software'), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
// ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

var util = __webpack_require__(/*! util/ */ "./node_modules/util/util.js");
var hasOwn = Object.prototype.hasOwnProperty;
var pSlice = Array.prototype.slice;
var functionsHaveNames = (function () {
  return function foo() {}.name === 'foo';
}());
function pToString (obj) {
  return Object.prototype.toString.call(obj);
}
function isView(arrbuf) {
  if (isBuffer(arrbuf)) {
    return false;
  }
  if (typeof global.ArrayBuffer !== 'function') {
    return false;
  }
  if (typeof ArrayBuffer.isView === 'function') {
    return ArrayBuffer.isView(arrbuf);
  }
  if (!arrbuf) {
    return false;
  }
  if (arrbuf instanceof DataView) {
    return true;
  }
  if (arrbuf.buffer && arrbuf.buffer instanceof ArrayBuffer) {
    return true;
  }
  return false;
}
// 1. The assert module provides functions that throw
// AssertionError's when particular conditions are not met. The
// assert module must conform to the following interface.

var assert = module.exports = ok;

// 2. The AssertionError is defined in assert.
// new assert.AssertionError({ message: message,
//                             actual: actual,
//                             expected: expected })

var regex = /\s*function\s+([^\(\s]*)\s*/;
// based on https://github.com/ljharb/function.prototype.name/blob/adeeeec8bfcc6068b187d7d9fb3d5bb1d3a30899/implementation.js
function getName(func) {
  if (!util.isFunction(func)) {
    return;
  }
  if (functionsHaveNames) {
    return func.name;
  }
  var str = func.toString();
  var match = str.match(regex);
  return match && match[1];
}
assert.AssertionError = function AssertionError(options) {
  this.name = 'AssertionError';
  this.actual = options.actual;
  this.expected = options.expected;
  this.operator = options.operator;
  if (options.message) {
    this.message = options.message;
    this.generatedMessage = false;
  } else {
    this.message = getMessage(this);
    this.generatedMessage = true;
  }
  var stackStartFunction = options.stackStartFunction || fail;
  if (Error.captureStackTrace) {
    Error.captureStackTrace(this, stackStartFunction);
  } else {
    // non v8 browsers so we can have a stacktrace
    var err = new Error();
    if (err.stack) {
      var out = err.stack;

      // try to strip useless frames
      var fn_name = getName(stackStartFunction);
      var idx = out.indexOf('\n' + fn_name);
      if (idx >= 0) {
        // once we have located the function frame
        // we need to strip out everything before it (and its line)
        var next_line = out.indexOf('\n', idx + 1);
        out = out.substring(next_line + 1);
      }

      this.stack = out;
    }
  }
};

// assert.AssertionError instanceof Error
util.inherits(assert.AssertionError, Error);

function truncate(s, n) {
  if (typeof s === 'string') {
    return s.length < n ? s : s.slice(0, n);
  } else {
    return s;
  }
}
function inspect(something) {
  if (functionsHaveNames || !util.isFunction(something)) {
    return util.inspect(something);
  }
  var rawname = getName(something);
  var name = rawname ? ': ' + rawname : '';
  return '[Function' +  name + ']';
}
function getMessage(self) {
  return truncate(inspect(self.actual), 128) + ' ' +
         self.operator + ' ' +
         truncate(inspect(self.expected), 128);
}

// At present only the three keys mentioned above are used and
// understood by the spec. Implementations or sub modules can pass
// other keys to the AssertionError's constructor - they will be
// ignored.

// 3. All of the following functions must throw an AssertionError
// when a corresponding condition is not met, with a message that
// may be undefined if not provided.  All assertion methods provide
// both the actual and expected values to the assertion error for
// display purposes.

function fail(actual, expected, message, operator, stackStartFunction) {
  throw new assert.AssertionError({
    message: message,
    actual: actual,
    expected: expected,
    operator: operator,
    stackStartFunction: stackStartFunction
  });
}

// EXTENSION! allows for well behaved errors defined elsewhere.
assert.fail = fail;

// 4. Pure assertion tests whether a value is truthy, as determined
// by !!guard.
// assert.ok(guard, message_opt);
// This statement is equivalent to assert.equal(true, !!guard,
// message_opt);. To test strictly for the value true, use
// assert.strictEqual(true, guard, message_opt);.

function ok(value, message) {
  if (!value) fail(value, true, message, '==', assert.ok);
}
assert.ok = ok;

// 5. The equality assertion tests shallow, coercive equality with
// ==.
// assert.equal(actual, expected, message_opt);

assert.equal = function equal(actual, expected, message) {
  if (actual != expected) fail(actual, expected, message, '==', assert.equal);
};

// 6. The non-equality assertion tests for whether two objects are not equal
// with != assert.notEqual(actual, expected, message_opt);

assert.notEqual = function notEqual(actual, expected, message) {
  if (actual == expected) {
    fail(actual, expected, message, '!=', assert.notEqual);
  }
};

// 7. The equivalence assertion tests a deep equality relation.
// assert.deepEqual(actual, expected, message_opt);

assert.deepEqual = function deepEqual(actual, expected, message) {
  if (!_deepEqual(actual, expected, false)) {
    fail(actual, expected, message, 'deepEqual', assert.deepEqual);
  }
};

assert.deepStrictEqual = function deepStrictEqual(actual, expected, message) {
  if (!_deepEqual(actual, expected, true)) {
    fail(actual, expected, message, 'deepStrictEqual', assert.deepStrictEqual);
  }
};

function _deepEqual(actual, expected, strict, memos) {
  // 7.1. All identical values are equivalent, as determined by ===.
  if (actual === expected) {
    return true;
  } else if (isBuffer(actual) && isBuffer(expected)) {
    return compare(actual, expected) === 0;

  // 7.2. If the expected value is a Date object, the actual value is
  // equivalent if it is also a Date object that refers to the same time.
  } else if (util.isDate(actual) && util.isDate(expected)) {
    return actual.getTime() === expected.getTime();

  // 7.3 If the expected value is a RegExp object, the actual value is
  // equivalent if it is also a RegExp object with the same source and
  // properties (`global`, `multiline`, `lastIndex`, `ignoreCase`).
  } else if (util.isRegExp(actual) && util.isRegExp(expected)) {
    return actual.source === expected.source &&
           actual.global === expected.global &&
           actual.multiline === expected.multiline &&
           actual.lastIndex === expected.lastIndex &&
           actual.ignoreCase === expected.ignoreCase;

  // 7.4. Other pairs that do not both pass typeof value == 'object',
  // equivalence is determined by ==.
  } else if ((actual === null || typeof actual !== 'object') &&
             (expected === null || typeof expected !== 'object')) {
    return strict ? actual === expected : actual == expected;

  // If both values are instances of typed arrays, wrap their underlying
  // ArrayBuffers in a Buffer each to increase performance
  // This optimization requires the arrays to have the same type as checked by
  // Object.prototype.toString (aka pToString). Never perform binary
  // comparisons for Float*Arrays, though, since e.g. +0 === -0 but their
  // bit patterns are not identical.
  } else if (isView(actual) && isView(expected) &&
             pToString(actual) === pToString(expected) &&
             !(actual instanceof Float32Array ||
               actual instanceof Float64Array)) {
    return compare(new Uint8Array(actual.buffer),
                   new Uint8Array(expected.buffer)) === 0;

  // 7.5 For all other Object pairs, including Array objects, equivalence is
  // determined by having the same number of owned properties (as verified
  // with Object.prototype.hasOwnProperty.call), the same set of keys
  // (although not necessarily the same order), equivalent values for every
  // corresponding key, and an identical 'prototype' property. Note: this
  // accounts for both named and indexed properties on Arrays.
  } else if (isBuffer(actual) !== isBuffer(expected)) {
    return false;
  } else {
    memos = memos || {actual: [], expected: []};

    var actualIndex = memos.actual.indexOf(actual);
    if (actualIndex !== -1) {
      if (actualIndex === memos.expected.indexOf(expected)) {
        return true;
      }
    }

    memos.actual.push(actual);
    memos.expected.push(expected);

    return objEquiv(actual, expected, strict, memos);
  }
}

function isArguments(object) {
  return Object.prototype.toString.call(object) == '[object Arguments]';
}

function objEquiv(a, b, strict, actualVisitedObjects) {
  if (a === null || a === undefined || b === null || b === undefined)
    return false;
  // if one is a primitive, the other must be same
  if (util.isPrimitive(a) || util.isPrimitive(b))
    return a === b;
  if (strict && Object.getPrototypeOf(a) !== Object.getPrototypeOf(b))
    return false;
  var aIsArgs = isArguments(a);
  var bIsArgs = isArguments(b);
  if ((aIsArgs && !bIsArgs) || (!aIsArgs && bIsArgs))
    return false;
  if (aIsArgs) {
    a = pSlice.call(a);
    b = pSlice.call(b);
    return _deepEqual(a, b, strict);
  }
  var ka = objectKeys(a);
  var kb = objectKeys(b);
  var key, i;
  // having the same number of owned properties (keys incorporates
  // hasOwnProperty)
  if (ka.length !== kb.length)
    return false;
  //the same set of keys (although not necessarily the same order),
  ka.sort();
  kb.sort();
  //~~~cheap key test
  for (i = ka.length - 1; i >= 0; i--) {
    if (ka[i] !== kb[i])
      return false;
  }
  //equivalent values for every corresponding key, and
  //~~~possibly expensive deep test
  for (i = ka.length - 1; i >= 0; i--) {
    key = ka[i];
    if (!_deepEqual(a[key], b[key], strict, actualVisitedObjects))
      return false;
  }
  return true;
}

// 8. The non-equivalence assertion tests for any deep inequality.
// assert.notDeepEqual(actual, expected, message_opt);

assert.notDeepEqual = function notDeepEqual(actual, expected, message) {
  if (_deepEqual(actual, expected, false)) {
    fail(actual, expected, message, 'notDeepEqual', assert.notDeepEqual);
  }
};

assert.notDeepStrictEqual = notDeepStrictEqual;
function notDeepStrictEqual(actual, expected, message) {
  if (_deepEqual(actual, expected, true)) {
    fail(actual, expected, message, 'notDeepStrictEqual', notDeepStrictEqual);
  }
}


// 9. The strict equality assertion tests strict equality, as determined by ===.
// assert.strictEqual(actual, expected, message_opt);

assert.strictEqual = function strictEqual(actual, expected, message) {
  if (actual !== expected) {
    fail(actual, expected, message, '===', assert.strictEqual);
  }
};

// 10. The strict non-equality assertion tests for strict inequality, as
// determined by !==.  assert.notStrictEqual(actual, expected, message_opt);

assert.notStrictEqual = function notStrictEqual(actual, expected, message) {
  if (actual === expected) {
    fail(actual, expected, message, '!==', assert.notStrictEqual);
  }
};

function expectedException(actual, expected) {
  if (!actual || !expected) {
    return false;
  }

  if (Object.prototype.toString.call(expected) == '[object RegExp]') {
    return expected.test(actual);
  }

  try {
    if (actual instanceof expected) {
      return true;
    }
  } catch (e) {
    // Ignore.  The instanceof check doesn't work for arrow functions.
  }

  if (Error.isPrototypeOf(expected)) {
    return false;
  }

  return expected.call({}, actual) === true;
}

function _tryBlock(block) {
  var error;
  try {
    block();
  } catch (e) {
    error = e;
  }
  return error;
}

function _throws(shouldThrow, block, expected, message) {
  var actual;

  if (typeof block !== 'function') {
    throw new TypeError('"block" argument must be a function');
  }

  if (typeof expected === 'string') {
    message = expected;
    expected = null;
  }

  actual = _tryBlock(block);

  message = (expected && expected.name ? ' (' + expected.name + ').' : '.') +
            (message ? ' ' + message : '.');

  if (shouldThrow && !actual) {
    fail(actual, expected, 'Missing expected exception' + message);
  }

  var userProvidedMessage = typeof message === 'string';
  var isUnwantedException = !shouldThrow && util.isError(actual);
  var isUnexpectedException = !shouldThrow && actual && !expected;

  if ((isUnwantedException &&
      userProvidedMessage &&
      expectedException(actual, expected)) ||
      isUnexpectedException) {
    fail(actual, expected, 'Got unwanted exception' + message);
  }

  if ((shouldThrow && actual && expected &&
      !expectedException(actual, expected)) || (!shouldThrow && actual)) {
    throw actual;
  }
}

// 11. Expected to throw an error:
// assert.throws(block, Error_opt, message_opt);

assert.throws = function(block, /*optional*/error, /*optional*/message) {
  _throws(true, block, error, message);
};

// EXTENSION! This is annoying to write outside this module.
assert.doesNotThrow = function(block, /*optional*/error, /*optional*/message) {
  _throws(false, block, error, message);
};

assert.ifError = function(err) { if (err) throw err; };

// Expose a strict only variant of assert
function strict(value, message) {
  if (!value) fail(value, true, message, '==', strict);
}
assert.strict = objectAssign(strict, assert, {
  equal: assert.strictEqual,
  deepEqual: assert.deepStrictEqual,
  notEqual: assert.notStrictEqual,
  notDeepEqual: assert.notDeepStrictEqual
});
assert.strict.strict = assert.strict;

var objectKeys = Object.keys || function (obj) {
  var keys = [];
  for (var key in obj) {
    if (hasOwn.call(obj, key)) keys.push(key);
  }
  return keys;
};

/* WEBPACK VAR INJECTION */}.call(this, __webpack_require__(/*! ./../webpack/buildin/global.js */ "./node_modules/webpack/buildin/global.js")))

/***/ }),

/***/ "./node_modules/console-browserify/index.js":
/*!**************************************************!*\
  !*** ./node_modules/console-browserify/index.js ***!
  \**************************************************/
/*! no static exports found */
/***/ (function(module, exports, __webpack_require__) {

/* WEBPACK VAR INJECTION */(function(global) {/*global window, global*/
var util = __webpack_require__(/*! util */ "./node_modules/util/util.js")
var assert = __webpack_require__(/*! assert */ "./node_modules/assert/assert.js")
function now() { return new Date().getTime() }

var slice = Array.prototype.slice
var console
var times = {}

if (typeof global !== "undefined" && global.console) {
    console = global.console
} else if (typeof window !== "undefined" && window.console) {
    console = window.console
} else {
    console = {}
}

var functions = [
    [log, "log"],
    [info, "info"],
    [warn, "warn"],
    [error, "error"],
    [time, "time"],
    [timeEnd, "timeEnd"],
    [trace, "trace"],
    [dir, "dir"],
    [consoleAssert, "assert"]
]

for (var i = 0; i < functions.length; i++) {
    var tuple = functions[i]
    var f = tuple[0]
    var name = tuple[1]

    if (!console[name]) {
        console[name] = f
    }
}

module.exports = console

function log() {}

function info() {
    console.log.apply(console, arguments)
}

function warn() {
    console.log.apply(console, arguments)
}

function error() {
    console.warn.apply(console, arguments)
}

function time(label) {
    times[label] = now()
}

function timeEnd(label) {
    var time = times[label]
    if (!time) {
        throw new Error("No such label: " + label)
    }

    delete times[label]
    var duration = now() - time
    console.log(label + ": " + duration + "ms")
}

function trace() {
    var err = new Error()
    err.name = "Trace"
    err.message = util.format.apply(null, arguments)
    console.error(err.stack)
}

function dir(object) {
    console.log(util.inspect(object) + "\n")
}

function consoleAssert(expression) {
    if (!expression) {
        var arr = slice.call(arguments, 1)
        assert.ok(false, util.format.apply(null, arr))
    }
}

/* WEBPACK VAR INJECTION */}.call(this, __webpack_require__(/*! ./../webpack/buildin/global.js */ "./node_modules/webpack/buildin/global.js")))

/***/ }),

/***/ "./node_modules/events/events.js":
/*!***************************************!*\
  !*** ./node_modules/events/events.js ***!
  \***************************************/
/*! no static exports found */
/***/ (function(module, exports, __webpack_require__) {

"use strict";
/* WEBPACK VAR INJECTION */(function(console) {// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.



var R = typeof Reflect === 'object' ? Reflect : null
var ReflectApply = R && typeof R.apply === 'function'
  ? R.apply
  : function ReflectApply(target, receiver, args) {
    return Function.prototype.apply.call(target, receiver, args);
  }

var ReflectOwnKeys
if (R && typeof R.ownKeys === 'function') {
  ReflectOwnKeys = R.ownKeys
} else if (Object.getOwnPropertySymbols) {
  ReflectOwnKeys = function ReflectOwnKeys(target) {
    return Object.getOwnPropertyNames(target)
      .concat(Object.getOwnPropertySymbols(target));
  };
} else {
  ReflectOwnKeys = function ReflectOwnKeys(target) {
    return Object.getOwnPropertyNames(target);
  };
}

function ProcessEmitWarning(warning) {
  if (console && console.warn) console.warn(warning);
}

var NumberIsNaN = Number.isNaN || function NumberIsNaN(value) {
  return value !== value;
}

function EventEmitter() {
  EventEmitter.init.call(this);
}
module.exports = EventEmitter;

// Backwards-compat with node 0.10.x
EventEmitter.EventEmitter = EventEmitter;

EventEmitter.prototype._events = undefined;
EventEmitter.prototype._eventsCount = 0;
EventEmitter.prototype._maxListeners = undefined;

// By default EventEmitters will print a warning if more than 10 listeners are
// added to it. This is a useful default which helps finding memory leaks.
var defaultMaxListeners = 10;

function checkListener(listener) {
  if (typeof listener !== 'function') {
    throw new TypeError('The "listener" argument must be of type Function. Received type ' + typeof listener);
  }
}

Object.defineProperty(EventEmitter, 'defaultMaxListeners', {
  enumerable: true,
  get: function() {
    return defaultMaxListeners;
  },
  set: function(arg) {
    if (typeof arg !== 'number' || arg < 0 || NumberIsNaN(arg)) {
      throw new RangeError('The value of "defaultMaxListeners" is out of range. It must be a non-negative number. Received ' + arg + '.');
    }
    defaultMaxListeners = arg;
  }
});

EventEmitter.init = function() {

  if (this._events === undefined ||
      this._events === Object.getPrototypeOf(this)._events) {
    this._events = Object.create(null);
    this._eventsCount = 0;
  }

  this._maxListeners = this._maxListeners || undefined;
};

// Obviously not all Emitters should be limited to 10. This function allows
// that to be increased. Set to zero for unlimited.
EventEmitter.prototype.setMaxListeners = function setMaxListeners(n) {
  if (typeof n !== 'number' || n < 0 || NumberIsNaN(n)) {
    throw new RangeError('The value of "n" is out of range. It must be a non-negative number. Received ' + n + '.');
  }
  this._maxListeners = n;
  return this;
};

function _getMaxListeners(that) {
  if (that._maxListeners === undefined)
    return EventEmitter.defaultMaxListeners;
  return that._maxListeners;
}

EventEmitter.prototype.getMaxListeners = function getMaxListeners() {
  return _getMaxListeners(this);
};

EventEmitter.prototype.emit = function emit(type) {
  var args = [];
  for (var i = 1; i < arguments.length; i++) args.push(arguments[i]);
  var doError = (type === 'error');

  var events = this._events;
  if (events !== undefined)
    doError = (doError && events.error === undefined);
  else if (!doError)
    return false;

  // If there is no 'error' event listener then throw.
  if (doError) {
    var er;
    if (args.length > 0)
      er = args[0];
    if (er instanceof Error) {
      // Note: The comments on the `throw` lines are intentional, they show
      // up in Node's output if this results in an unhandled exception.
      throw er; // Unhandled 'error' event
    }
    // At least give some kind of context to the user
    var err = new Error('Unhandled error.' + (er ? ' (' + er.message + ')' : ''));
    err.context = er;
    throw err; // Unhandled 'error' event
  }

  var handler = events[type];

  if (handler === undefined)
    return false;

  if (typeof handler === 'function') {
    ReflectApply(handler, this, args);
  } else {
    var len = handler.length;
    var listeners = arrayClone(handler, len);
    for (var i = 0; i < len; ++i)
      ReflectApply(listeners[i], this, args);
  }

  return true;
};

function _addListener(target, type, listener, prepend) {
  var m;
  var events;
  var existing;

  checkListener(listener);

  events = target._events;
  if (events === undefined) {
    events = target._events = Object.create(null);
    target._eventsCount = 0;
  } else {
    // To avoid recursion in the case that type === "newListener"! Before
    // adding it to the listeners, first emit "newListener".
    if (events.newListener !== undefined) {
      target.emit('newListener', type,
                  listener.listener ? listener.listener : listener);

      // Re-assign `events` because a newListener handler could have caused the
      // this._events to be assigned to a new object
      events = target._events;
    }
    existing = events[type];
  }

  if (existing === undefined) {
    // Optimize the case of one listener. Don't need the extra array object.
    existing = events[type] = listener;
    ++target._eventsCount;
  } else {
    if (typeof existing === 'function') {
      // Adding the second element, need to change to array.
      existing = events[type] =
        prepend ? [listener, existing] : [existing, listener];
      // If we've already got an array, just append.
    } else if (prepend) {
      existing.unshift(listener);
    } else {
      existing.push(listener);
    }

    // Check for listener leak
    m = _getMaxListeners(target);
    if (m > 0 && existing.length > m && !existing.warned) {
      existing.warned = true;
      // No error code for this since it is a Warning
      // eslint-disable-next-line no-restricted-syntax
      var w = new Error('Possible EventEmitter memory leak detected. ' +
                          existing.length + ' ' + String(type) + ' listeners ' +
                          'added. Use emitter.setMaxListeners() to ' +
                          'increase limit');
      w.name = 'MaxListenersExceededWarning';
      w.emitter = target;
      w.type = type;
      w.count = existing.length;
      ProcessEmitWarning(w);
    }
  }

  return target;
}

EventEmitter.prototype.addListener = function addListener(type, listener) {
  return _addListener(this, type, listener, false);
};

EventEmitter.prototype.on = EventEmitter.prototype.addListener;

EventEmitter.prototype.prependListener =
    function prependListener(type, listener) {
      return _addListener(this, type, listener, true);
    };

function onceWrapper() {
  if (!this.fired) {
    this.target.removeListener(this.type, this.wrapFn);
    this.fired = true;
    if (arguments.length === 0)
      return this.listener.call(this.target);
    return this.listener.apply(this.target, arguments);
  }
}

function _onceWrap(target, type, listener) {
  var state = { fired: false, wrapFn: undefined, target: target, type: type, listener: listener };
  var wrapped = onceWrapper.bind(state);
  wrapped.listener = listener;
  state.wrapFn = wrapped;
  return wrapped;
}

EventEmitter.prototype.once = function once(type, listener) {
  checkListener(listener);
  this.on(type, _onceWrap(this, type, listener));
  return this;
};

EventEmitter.prototype.prependOnceListener =
    function prependOnceListener(type, listener) {
      checkListener(listener);
      this.prependListener(type, _onceWrap(this, type, listener));
      return this;
    };

// Emits a 'removeListener' event if and only if the listener was removed.
EventEmitter.prototype.removeListener =
    function removeListener(type, listener) {
      var list, events, position, i, originalListener;

      checkListener(listener);

      events = this._events;
      if (events === undefined)
        return this;

      list = events[type];
      if (list === undefined)
        return this;

      if (list === listener || list.listener === listener) {
        if (--this._eventsCount === 0)
          this._events = Object.create(null);
        else {
          delete events[type];
          if (events.removeListener)
            this.emit('removeListener', type, list.listener || listener);
        }
      } else if (typeof list !== 'function') {
        position = -1;

        for (i = list.length - 1; i >= 0; i--) {
          if (list[i] === listener || list[i].listener === listener) {
            originalListener = list[i].listener;
            position = i;
            break;
          }
        }

        if (position < 0)
          return this;

        if (position === 0)
          list.shift();
        else {
          spliceOne(list, position);
        }

        if (list.length === 1)
          events[type] = list[0];

        if (events.removeListener !== undefined)
          this.emit('removeListener', type, originalListener || listener);
      }

      return this;
    };

EventEmitter.prototype.off = EventEmitter.prototype.removeListener;

EventEmitter.prototype.removeAllListeners =
    function removeAllListeners(type) {
      var listeners, events, i;

      events = this._events;
      if (events === undefined)
        return this;

      // not listening for removeListener, no need to emit
      if (events.removeListener === undefined) {
        if (arguments.length === 0) {
          this._events = Object.create(null);
          this._eventsCount = 0;
        } else if (events[type] !== undefined) {
          if (--this._eventsCount === 0)
            this._events = Object.create(null);
          else
            delete events[type];
        }
        return this;
      }

      // emit removeListener for all listeners on all events
      if (arguments.length === 0) {
        var keys = Object.keys(events);
        var key;
        for (i = 0; i < keys.length; ++i) {
          key = keys[i];
          if (key === 'removeListener') continue;
          this.removeAllListeners(key);
        }
        this.removeAllListeners('removeListener');
        this._events = Object.create(null);
        this._eventsCount = 0;
        return this;
      }

      listeners = events[type];

      if (typeof listeners === 'function') {
        this.removeListener(type, listeners);
      } else if (listeners !== undefined) {
        // LIFO order
        for (i = listeners.length - 1; i >= 0; i--) {
          this.removeListener(type, listeners[i]);
        }
      }

      return this;
    };

function _listeners(target, type, unwrap) {
  var events = target._events;

  if (events === undefined)
    return [];

  var evlistener = events[type];
  if (evlistener === undefined)
    return [];

  if (typeof evlistener === 'function')
    return unwrap ? [evlistener.listener || evlistener] : [evlistener];

  return unwrap ?
    unwrapListeners(evlistener) : arrayClone(evlistener, evlistener.length);
}

EventEmitter.prototype.listeners = function listeners(type) {
  return _listeners(this, type, true);
};

EventEmitter.prototype.rawListeners = function rawListeners(type) {
  return _listeners(this, type, false);
};

EventEmitter.listenerCount = function(emitter, type) {
  if (typeof emitter.listenerCount === 'function') {
    return emitter.listenerCount(type);
  } else {
    return listenerCount.call(emitter, type);
  }
};

EventEmitter.prototype.listenerCount = listenerCount;
function listenerCount(type) {
  var events = this._events;

  if (events !== undefined) {
    var evlistener = events[type];

    if (typeof evlistener === 'function') {
      return 1;
    } else if (evlistener !== undefined) {
      return evlistener.length;
    }
  }

  return 0;
}

EventEmitter.prototype.eventNames = function eventNames() {
  return this._eventsCount > 0 ? ReflectOwnKeys(this._events) : [];
};

function arrayClone(arr, n) {
  var copy = new Array(n);
  for (var i = 0; i < n; ++i)
    copy[i] = arr[i];
  return copy;
}

function spliceOne(list, index) {
  for (; index + 1 < list.length; index++)
    list[index] = list[index + 1];
  list.pop();
}

function unwrapListeners(arr) {
  var ret = new Array(arr.length);
  for (var i = 0; i < ret.length; ++i) {
    ret[i] = arr[i].listener || arr[i];
  }
  return ret;
}

/* WEBPACK VAR INJECTION */}.call(this, __webpack_require__(/*! ./../console-browserify/index.js */ "./node_modules/console-browserify/index.js")))

/***/ }),

/***/ "./node_modules/object-assign/index.js":
/*!*********************************************!*\
  !*** ./node_modules/object-assign/index.js ***!
  \*********************************************/
/*! no static exports found */
/***/ (function(module, exports, __webpack_require__) {

"use strict";
/*
object-assign
(c) Sindre Sorhus
@license MIT
*/


/* eslint-disable no-unused-vars */
var getOwnPropertySymbols = Object.getOwnPropertySymbols;
var hasOwnProperty = Object.prototype.hasOwnProperty;
var propIsEnumerable = Object.prototype.propertyIsEnumerable;

function toObject(val) {
	if (val === null || val === undefined) {
		throw new TypeError('Object.assign cannot be called with null or undefined');
	}

	return Object(val);
}

function shouldUseNative() {
	try {
		if (!Object.assign) {
			return false;
		}

		// Detect buggy property enumeration order in older V8 versions.

		// https://bugs.chromium.org/p/v8/issues/detail?id=4118
		var test1 = new String('abc');  // eslint-disable-line no-new-wrappers
		test1[5] = 'de';
		if (Object.getOwnPropertyNames(test1)[0] === '5') {
			return false;
		}

		// https://bugs.chromium.org/p/v8/issues/detail?id=3056
		var test2 = {};
		for (var i = 0; i < 10; i++) {
			test2['_' + String.fromCharCode(i)] = i;
		}
		var order2 = Object.getOwnPropertyNames(test2).map(function (n) {
			return test2[n];
		});
		if (order2.join('') !== '0123456789') {
			return false;
		}

		// https://bugs.chromium.org/p/v8/issues/detail?id=3056
		var test3 = {};
		'abcdefghijklmnopqrst'.split('').forEach(function (letter) {
			test3[letter] = letter;
		});
		if (Object.keys(Object.assign({}, test3)).join('') !==
				'abcdefghijklmnopqrst') {
			return false;
		}

		return true;
	} catch (err) {
		// We don't expect any of the above to throw, but better to be safe.
		return false;
	}
}

module.exports = shouldUseNative() ? Object.assign : function (target, source) {
	var from;
	var to = toObject(target);
	var symbols;

	for (var s = 1; s < arguments.length; s++) {
		from = Object(arguments[s]);

		for (var key in from) {
			if (hasOwnProperty.call(from, key)) {
				to[key] = from[key];
			}
		}

		if (getOwnPropertySymbols) {
			symbols = getOwnPropertySymbols(from);
			for (var i = 0; i < symbols.length; i++) {
				if (propIsEnumerable.call(from, symbols[i])) {
					to[symbols[i]] = from[symbols[i]];
				}
			}
		}
	}

	return to;
};


/***/ }),

/***/ "./node_modules/process/browser.js":
/*!*****************************************!*\
  !*** ./node_modules/process/browser.js ***!
  \*****************************************/
/*! no static exports found */
/***/ (function(module, exports) {

// shim for using process in browser
var process = module.exports = {};

// cached from whatever global is present so that test runners that stub it
// don't break things.  But we need to wrap it in a try catch in case it is
// wrapped in strict mode code which doesn't define any globals.  It's inside a
// function because try/catches deoptimize in certain engines.

var cachedSetTimeout;
var cachedClearTimeout;

function defaultSetTimout() {
    throw new Error('setTimeout has not been defined');
}
function defaultClearTimeout () {
    throw new Error('clearTimeout has not been defined');
}
(function () {
    try {
        if (typeof setTimeout === 'function') {
            cachedSetTimeout = setTimeout;
        } else {
            cachedSetTimeout = defaultSetTimout;
        }
    } catch (e) {
        cachedSetTimeout = defaultSetTimout;
    }
    try {
        if (typeof clearTimeout === 'function') {
            cachedClearTimeout = clearTimeout;
        } else {
            cachedClearTimeout = defaultClearTimeout;
        }
    } catch (e) {
        cachedClearTimeout = defaultClearTimeout;
    }
} ())
function runTimeout(fun) {
    if (cachedSetTimeout === setTimeout) {
        //normal enviroments in sane situations
        return setTimeout(fun, 0);
    }
    // if setTimeout wasn't available but was latter defined
    if ((cachedSetTimeout === defaultSetTimout || !cachedSetTimeout) && setTimeout) {
        cachedSetTimeout = setTimeout;
        return setTimeout(fun, 0);
    }
    try {
        // when when somebody has screwed with setTimeout but no I.E. maddness
        return cachedSetTimeout(fun, 0);
    } catch(e){
        try {
            // When we are in I.E. but the script has been evaled so I.E. doesn't trust the global object when called normally
            return cachedSetTimeout.call(null, fun, 0);
        } catch(e){
            // same as above but when it's a version of I.E. that must have the global object for 'this', hopfully our context correct otherwise it will throw a global error
            return cachedSetTimeout.call(this, fun, 0);
        }
    }


}
function runClearTimeout(marker) {
    if (cachedClearTimeout === clearTimeout) {
        //normal enviroments in sane situations
        return clearTimeout(marker);
    }
    // if clearTimeout wasn't available but was latter defined
    if ((cachedClearTimeout === defaultClearTimeout || !cachedClearTimeout) && clearTimeout) {
        cachedClearTimeout = clearTimeout;
        return clearTimeout(marker);
    }
    try {
        // when when somebody has screwed with setTimeout but no I.E. maddness
        return cachedClearTimeout(marker);
    } catch (e){
        try {
            // When we are in I.E. but the script has been evaled so I.E. doesn't  trust the global object when called normally
            return cachedClearTimeout.call(null, marker);
        } catch (e){
            // same as above but when it's a version of I.E. that must have the global object for 'this', hopfully our context correct otherwise it will throw a global error.
            // Some versions of I.E. have different rules for clearTimeout vs setTimeout
            return cachedClearTimeout.call(this, marker);
        }
    }



}
var queue = [];
var draining = false;
var currentQueue;
var queueIndex = -1;

function cleanUpNextTick() {
    if (!draining || !currentQueue) {
        return;
    }
    draining = false;
    if (currentQueue.length) {
        queue = currentQueue.concat(queue);
    } else {
        queueIndex = -1;
    }
    if (queue.length) {
        drainQueue();
    }
}

function drainQueue() {
    if (draining) {
        return;
    }
    var timeout = runTimeout(cleanUpNextTick);
    draining = true;

    var len = queue.length;
    while(len) {
        currentQueue = queue;
        queue = [];
        while (++queueIndex < len) {
            if (currentQueue) {
                currentQueue[queueIndex].run();
            }
        }
        queueIndex = -1;
        len = queue.length;
    }
    currentQueue = null;
    draining = false;
    runClearTimeout(timeout);
}

process.nextTick = function (fun) {
    var args = new Array(arguments.length - 1);
    if (arguments.length > 1) {
        for (var i = 1; i < arguments.length; i++) {
            args[i - 1] = arguments[i];
        }
    }
    queue.push(new Item(fun, args));
    if (queue.length === 1 && !draining) {
        runTimeout(drainQueue);
    }
};

// v8 likes predictible objects
function Item(fun, array) {
    this.fun = fun;
    this.array = array;
}
Item.prototype.run = function () {
    this.fun.apply(null, this.array);
};
process.title = 'browser';
process.browser = true;
process.env = {};
process.argv = [];
process.version = ''; // empty string to avoid regexp issues
process.versions = {};

function noop() {}

process.on = noop;
process.addListener = noop;
process.once = noop;
process.off = noop;
process.removeListener = noop;
process.removeAllListeners = noop;
process.emit = noop;
process.prependListener = noop;
process.prependOnceListener = noop;

process.listeners = function (name) { return [] }

process.binding = function (name) {
    throw new Error('process.binding is not supported');
};

process.cwd = function () { return '/' };
process.chdir = function (dir) {
    throw new Error('process.chdir is not supported');
};
process.umask = function() { return 0; };


/***/ }),

/***/ "./node_modules/util/node_modules/inherits/inherits_browser.js":
/*!*********************************************************************!*\
  !*** ./node_modules/util/node_modules/inherits/inherits_browser.js ***!
  \*********************************************************************/
/*! no static exports found */
/***/ (function(module, exports) {

if (typeof Object.create === 'function') {
  // implementation from standard node.js 'util' module
  module.exports = function inherits(ctor, superCtor) {
    ctor.super_ = superCtor
    ctor.prototype = Object.create(superCtor.prototype, {
      constructor: {
        value: ctor,
        enumerable: false,
        writable: true,
        configurable: true
      }
    });
  };
} else {
  // old school shim for old browsers
  module.exports = function inherits(ctor, superCtor) {
    ctor.super_ = superCtor
    var TempCtor = function () {}
    TempCtor.prototype = superCtor.prototype
    ctor.prototype = new TempCtor()
    ctor.prototype.constructor = ctor
  }
}


/***/ }),

/***/ "./node_modules/util/support/isBufferBrowser.js":
/*!******************************************************!*\
  !*** ./node_modules/util/support/isBufferBrowser.js ***!
  \******************************************************/
/*! no static exports found */
/***/ (function(module, exports) {

module.exports = function isBuffer(arg) {
  return arg && typeof arg === 'object'
    && typeof arg.copy === 'function'
    && typeof arg.fill === 'function'
    && typeof arg.readUInt8 === 'function';
}

/***/ }),

/***/ "./node_modules/util/util.js":
/*!***********************************!*\
  !*** ./node_modules/util/util.js ***!
  \***********************************/
/*! no static exports found */
/***/ (function(module, exports, __webpack_require__) {

/* WEBPACK VAR INJECTION */(function(process, console) {// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

var getOwnPropertyDescriptors = Object.getOwnPropertyDescriptors ||
  function getOwnPropertyDescriptors(obj) {
    var keys = Object.keys(obj);
    var descriptors = {};
    for (var i = 0; i < keys.length; i++) {
      descriptors[keys[i]] = Object.getOwnPropertyDescriptor(obj, keys[i]);
    }
    return descriptors;
  };

var formatRegExp = /%[sdj%]/g;
exports.format = function(f) {
  if (!isString(f)) {
    var objects = [];
    for (var i = 0; i < arguments.length; i++) {
      objects.push(inspect(arguments[i]));
    }
    return objects.join(' ');
  }

  var i = 1;
  var args = arguments;
  var len = args.length;
  var str = String(f).replace(formatRegExp, function(x) {
    if (x === '%%') return '%';
    if (i >= len) return x;
    switch (x) {
      case '%s': return String(args[i++]);
      case '%d': return Number(args[i++]);
      case '%j':
        try {
          return JSON.stringify(args[i++]);
        } catch (_) {
          return '[Circular]';
        }
      default:
        return x;
    }
  });
  for (var x = args[i]; i < len; x = args[++i]) {
    if (isNull(x) || !isObject(x)) {
      str += ' ' + x;
    } else {
      str += ' ' + inspect(x);
    }
  }
  return str;
};


// Mark that a method should not be used.
// Returns a modified function which warns once by default.
// If --no-deprecation is set, then it is a no-op.
exports.deprecate = function(fn, msg) {
  if (typeof process !== 'undefined' && process.noDeprecation === true) {
    return fn;
  }

  // Allow for deprecating things in the process of starting up.
  if (typeof process === 'undefined') {
    return function() {
      return exports.deprecate(fn, msg).apply(this, arguments);
    };
  }

  var warned = false;
  function deprecated() {
    if (!warned) {
      if (process.throwDeprecation) {
        throw new Error(msg);
      } else if (process.traceDeprecation) {
        console.trace(msg);
      } else {
        console.error(msg);
      }
      warned = true;
    }
    return fn.apply(this, arguments);
  }

  return deprecated;
};


var debugs = {};
var debugEnviron;
exports.debuglog = function(set) {
  if (isUndefined(debugEnviron))
    debugEnviron = Object({"WEB":"web"}).NODE_DEBUG || '';
  set = set.toUpperCase();
  if (!debugs[set]) {
    if (new RegExp('\\b' + set + '\\b', 'i').test(debugEnviron)) {
      var pid = process.pid;
      debugs[set] = function() {
        var msg = exports.format.apply(exports, arguments);
        console.error('%s %d: %s', set, pid, msg);
      };
    } else {
      debugs[set] = function() {};
    }
  }
  return debugs[set];
};


/**
 * Echos the value of a value. Trys to print the value out
 * in the best way possible given the different types.
 *
 * @param {Object} obj The object to print out.
 * @param {Object} opts Optional options object that alters the output.
 */
/* legacy: obj, showHidden, depth, colors*/
function inspect(obj, opts) {
  // default options
  var ctx = {
    seen: [],
    stylize: stylizeNoColor
  };
  // legacy...
  if (arguments.length >= 3) ctx.depth = arguments[2];
  if (arguments.length >= 4) ctx.colors = arguments[3];
  if (isBoolean(opts)) {
    // legacy...
    ctx.showHidden = opts;
  } else if (opts) {
    // got an "options" object
    exports._extend(ctx, opts);
  }
  // set default options
  if (isUndefined(ctx.showHidden)) ctx.showHidden = false;
  if (isUndefined(ctx.depth)) ctx.depth = 2;
  if (isUndefined(ctx.colors)) ctx.colors = false;
  if (isUndefined(ctx.customInspect)) ctx.customInspect = true;
  if (ctx.colors) ctx.stylize = stylizeWithColor;
  return formatValue(ctx, obj, ctx.depth);
}
exports.inspect = inspect;


// http://en.wikipedia.org/wiki/ANSI_escape_code#graphics
inspect.colors = {
  'bold' : [1, 22],
  'italic' : [3, 23],
  'underline' : [4, 24],
  'inverse' : [7, 27],
  'white' : [37, 39],
  'grey' : [90, 39],
  'black' : [30, 39],
  'blue' : [34, 39],
  'cyan' : [36, 39],
  'green' : [32, 39],
  'magenta' : [35, 39],
  'red' : [31, 39],
  'yellow' : [33, 39]
};

// Don't use 'blue' not visible on cmd.exe
inspect.styles = {
  'special': 'cyan',
  'number': 'yellow',
  'boolean': 'yellow',
  'undefined': 'grey',
  'null': 'bold',
  'string': 'green',
  'date': 'magenta',
  // "name": intentionally not styling
  'regexp': 'red'
};


function stylizeWithColor(str, styleType) {
  var style = inspect.styles[styleType];

  if (style) {
    return '\u001b[' + inspect.colors[style][0] + 'm' + str +
           '\u001b[' + inspect.colors[style][1] + 'm';
  } else {
    return str;
  }
}


function stylizeNoColor(str, styleType) {
  return str;
}


function arrayToHash(array) {
  var hash = {};

  array.forEach(function(val, idx) {
    hash[val] = true;
  });

  return hash;
}


function formatValue(ctx, value, recurseTimes) {
  // Provide a hook for user-specified inspect functions.
  // Check that value is an object with an inspect function on it
  if (ctx.customInspect &&
      value &&
      isFunction(value.inspect) &&
      // Filter out the util module, it's inspect function is special
      value.inspect !== exports.inspect &&
      // Also filter out any prototype objects using the circular check.
      !(value.constructor && value.constructor.prototype === value)) {
    var ret = value.inspect(recurseTimes, ctx);
    if (!isString(ret)) {
      ret = formatValue(ctx, ret, recurseTimes);
    }
    return ret;
  }

  // Primitive types cannot have properties
  var primitive = formatPrimitive(ctx, value);
  if (primitive) {
    return primitive;
  }

  // Look up the keys of the object.
  var keys = Object.keys(value);
  var visibleKeys = arrayToHash(keys);

  if (ctx.showHidden) {
    keys = Object.getOwnPropertyNames(value);
  }

  // IE doesn't make error fields non-enumerable
  // http://msdn.microsoft.com/en-us/library/ie/dww52sbt(v=vs.94).aspx
  if (isError(value)
      && (keys.indexOf('message') >= 0 || keys.indexOf('description') >= 0)) {
    return formatError(value);
  }

  // Some type of object without properties can be shortcutted.
  if (keys.length === 0) {
    if (isFunction(value)) {
      var name = value.name ? ': ' + value.name : '';
      return ctx.stylize('[Function' + name + ']', 'special');
    }
    if (isRegExp(value)) {
      return ctx.stylize(RegExp.prototype.toString.call(value), 'regexp');
    }
    if (isDate(value)) {
      return ctx.stylize(Date.prototype.toString.call(value), 'date');
    }
    if (isError(value)) {
      return formatError(value);
    }
  }

  var base = '', array = false, braces = ['{', '}'];

  // Make Array say that they are Array
  if (isArray(value)) {
    array = true;
    braces = ['[', ']'];
  }

  // Make functions say that they are functions
  if (isFunction(value)) {
    var n = value.name ? ': ' + value.name : '';
    base = ' [Function' + n + ']';
  }

  // Make RegExps say that they are RegExps
  if (isRegExp(value)) {
    base = ' ' + RegExp.prototype.toString.call(value);
  }

  // Make dates with properties first say the date
  if (isDate(value)) {
    base = ' ' + Date.prototype.toUTCString.call(value);
  }

  // Make error with message first say the error
  if (isError(value)) {
    base = ' ' + formatError(value);
  }

  if (keys.length === 0 && (!array || value.length == 0)) {
    return braces[0] + base + braces[1];
  }

  if (recurseTimes < 0) {
    if (isRegExp(value)) {
      return ctx.stylize(RegExp.prototype.toString.call(value), 'regexp');
    } else {
      return ctx.stylize('[Object]', 'special');
    }
  }

  ctx.seen.push(value);

  var output;
  if (array) {
    output = formatArray(ctx, value, recurseTimes, visibleKeys, keys);
  } else {
    output = keys.map(function(key) {
      return formatProperty(ctx, value, recurseTimes, visibleKeys, key, array);
    });
  }

  ctx.seen.pop();

  return reduceToSingleString(output, base, braces);
}


function formatPrimitive(ctx, value) {
  if (isUndefined(value))
    return ctx.stylize('undefined', 'undefined');
  if (isString(value)) {
    var simple = '\'' + JSON.stringify(value).replace(/^"|"$/g, '')
                                             .replace(/'/g, "\\'")
                                             .replace(/\\"/g, '"') + '\'';
    return ctx.stylize(simple, 'string');
  }
  if (isNumber(value))
    return ctx.stylize('' + value, 'number');
  if (isBoolean(value))
    return ctx.stylize('' + value, 'boolean');
  // For some reason typeof null is "object", so special case here.
  if (isNull(value))
    return ctx.stylize('null', 'null');
}


function formatError(value) {
  return '[' + Error.prototype.toString.call(value) + ']';
}


function formatArray(ctx, value, recurseTimes, visibleKeys, keys) {
  var output = [];
  for (var i = 0, l = value.length; i < l; ++i) {
    if (hasOwnProperty(value, String(i))) {
      output.push(formatProperty(ctx, value, recurseTimes, visibleKeys,
          String(i), true));
    } else {
      output.push('');
    }
  }
  keys.forEach(function(key) {
    if (!key.match(/^\d+$/)) {
      output.push(formatProperty(ctx, value, recurseTimes, visibleKeys,
          key, true));
    }
  });
  return output;
}


function formatProperty(ctx, value, recurseTimes, visibleKeys, key, array) {
  var name, str, desc;
  desc = Object.getOwnPropertyDescriptor(value, key) || { value: value[key] };
  if (desc.get) {
    if (desc.set) {
      str = ctx.stylize('[Getter/Setter]', 'special');
    } else {
      str = ctx.stylize('[Getter]', 'special');
    }
  } else {
    if (desc.set) {
      str = ctx.stylize('[Setter]', 'special');
    }
  }
  if (!hasOwnProperty(visibleKeys, key)) {
    name = '[' + key + ']';
  }
  if (!str) {
    if (ctx.seen.indexOf(desc.value) < 0) {
      if (isNull(recurseTimes)) {
        str = formatValue(ctx, desc.value, null);
      } else {
        str = formatValue(ctx, desc.value, recurseTimes - 1);
      }
      if (str.indexOf('\n') > -1) {
        if (array) {
          str = str.split('\n').map(function(line) {
            return '  ' + line;
          }).join('\n').substr(2);
        } else {
          str = '\n' + str.split('\n').map(function(line) {
            return '   ' + line;
          }).join('\n');
        }
      }
    } else {
      str = ctx.stylize('[Circular]', 'special');
    }
  }
  if (isUndefined(name)) {
    if (array && key.match(/^\d+$/)) {
      return str;
    }
    name = JSON.stringify('' + key);
    if (name.match(/^"([a-zA-Z_][a-zA-Z_0-9]*)"$/)) {
      name = name.substr(1, name.length - 2);
      name = ctx.stylize(name, 'name');
    } else {
      name = name.replace(/'/g, "\\'")
                 .replace(/\\"/g, '"')
                 .replace(/(^"|"$)/g, "'");
      name = ctx.stylize(name, 'string');
    }
  }

  return name + ': ' + str;
}


function reduceToSingleString(output, base, braces) {
  var numLinesEst = 0;
  var length = output.reduce(function(prev, cur) {
    numLinesEst++;
    if (cur.indexOf('\n') >= 0) numLinesEst++;
    return prev + cur.replace(/\u001b\[\d\d?m/g, '').length + 1;
  }, 0);

  if (length > 60) {
    return braces[0] +
           (base === '' ? '' : base + '\n ') +
           ' ' +
           output.join(',\n  ') +
           ' ' +
           braces[1];
  }

  return braces[0] + base + ' ' + output.join(', ') + ' ' + braces[1];
}


// NOTE: These type checking functions intentionally don't use `instanceof`
// because it is fragile and can be easily faked with `Object.create()`.
function isArray(ar) {
  return Array.isArray(ar);
}
exports.isArray = isArray;

function isBoolean(arg) {
  return typeof arg === 'boolean';
}
exports.isBoolean = isBoolean;

function isNull(arg) {
  return arg === null;
}
exports.isNull = isNull;

function isNullOrUndefined(arg) {
  return arg == null;
}
exports.isNullOrUndefined = isNullOrUndefined;

function isNumber(arg) {
  return typeof arg === 'number';
}
exports.isNumber = isNumber;

function isString(arg) {
  return typeof arg === 'string';
}
exports.isString = isString;

function isSymbol(arg) {
  return typeof arg === 'symbol';
}
exports.isSymbol = isSymbol;

function isUndefined(arg) {
  return arg === void 0;
}
exports.isUndefined = isUndefined;

function isRegExp(re) {
  return isObject(re) && objectToString(re) === '[object RegExp]';
}
exports.isRegExp = isRegExp;

function isObject(arg) {
  return typeof arg === 'object' && arg !== null;
}
exports.isObject = isObject;

function isDate(d) {
  return isObject(d) && objectToString(d) === '[object Date]';
}
exports.isDate = isDate;

function isError(e) {
  return isObject(e) &&
      (objectToString(e) === '[object Error]' || e instanceof Error);
}
exports.isError = isError;

function isFunction(arg) {
  return typeof arg === 'function';
}
exports.isFunction = isFunction;

function isPrimitive(arg) {
  return arg === null ||
         typeof arg === 'boolean' ||
         typeof arg === 'number' ||
         typeof arg === 'string' ||
         typeof arg === 'symbol' ||  // ES6 symbol
         typeof arg === 'undefined';
}
exports.isPrimitive = isPrimitive;

exports.isBuffer = __webpack_require__(/*! ./support/isBuffer */ "./node_modules/util/support/isBufferBrowser.js");

function objectToString(o) {
  return Object.prototype.toString.call(o);
}


function pad(n) {
  return n < 10 ? '0' + n.toString(10) : n.toString(10);
}


var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep',
              'Oct', 'Nov', 'Dec'];

// 26 Feb 16:19:34
function timestamp() {
  var d = new Date();
  var time = [pad(d.getHours()),
              pad(d.getMinutes()),
              pad(d.getSeconds())].join(':');
  return [d.getDate(), months[d.getMonth()], time].join(' ');
}


// log is just a thin wrapper to console.log that prepends a timestamp
exports.log = function() {
  console.log('%s - %s', timestamp(), exports.format.apply(exports, arguments));
};


/**
 * Inherit the prototype methods from one constructor into another.
 *
 * The Function.prototype.inherits from lang.js rewritten as a standalone
 * function (not on Function.prototype). NOTE: If this file is to be loaded
 * during bootstrapping this function needs to be rewritten using some native
 * functions as prototype setup using normal JavaScript does not work as
 * expected during bootstrapping (see mirror.js in r114903).
 *
 * @param {function} ctor Constructor function which needs to inherit the
 *     prototype.
 * @param {function} superCtor Constructor function to inherit prototype from.
 */
exports.inherits = __webpack_require__(/*! inherits */ "./node_modules/util/node_modules/inherits/inherits_browser.js");

exports._extend = function(origin, add) {
  // Don't do anything if add isn't an object
  if (!add || !isObject(add)) return origin;

  var keys = Object.keys(add);
  var i = keys.length;
  while (i--) {
    origin[keys[i]] = add[keys[i]];
  }
  return origin;
};

function hasOwnProperty(obj, prop) {
  return Object.prototype.hasOwnProperty.call(obj, prop);
}

var kCustomPromisifiedSymbol = typeof Symbol !== 'undefined' ? Symbol('util.promisify.custom') : undefined;

exports.promisify = function promisify(original) {
  if (typeof original !== 'function')
    throw new TypeError('The "original" argument must be of type Function');

  if (kCustomPromisifiedSymbol && original[kCustomPromisifiedSymbol]) {
    var fn = original[kCustomPromisifiedSymbol];
    if (typeof fn !== 'function') {
      throw new TypeError('The "util.promisify.custom" argument must be of type Function');
    }
    Object.defineProperty(fn, kCustomPromisifiedSymbol, {
      value: fn, enumerable: false, writable: false, configurable: true
    });
    return fn;
  }

  function fn() {
    var promiseResolve, promiseReject;
    var promise = new Promise(function (resolve, reject) {
      promiseResolve = resolve;
      promiseReject = reject;
    });

    var args = [];
    for (var i = 0; i < arguments.length; i++) {
      args.push(arguments[i]);
    }
    args.push(function (err, value) {
      if (err) {
        promiseReject(err);
      } else {
        promiseResolve(value);
      }
    });

    try {
      original.apply(this, args);
    } catch (err) {
      promiseReject(err);
    }

    return promise;
  }

  Object.setPrototypeOf(fn, Object.getPrototypeOf(original));

  if (kCustomPromisifiedSymbol) Object.defineProperty(fn, kCustomPromisifiedSymbol, {
    value: fn, enumerable: false, writable: false, configurable: true
  });
  return Object.defineProperties(
    fn,
    getOwnPropertyDescriptors(original)
  );
}

exports.promisify.custom = kCustomPromisifiedSymbol

function callbackifyOnRejected(reason, cb) {
  // `!reason` guard inspired by bluebird (Ref: https://goo.gl/t5IS6M).
  // Because `null` is a special error value in callbacks which means "no error
  // occurred", we error-wrap so the callback consumer can distinguish between
  // "the promise rejected with null" or "the promise fulfilled with undefined".
  if (!reason) {
    var newReason = new Error('Promise was rejected with a falsy value');
    newReason.reason = reason;
    reason = newReason;
  }
  return cb(reason);
}

function callbackify(original) {
  if (typeof original !== 'function') {
    throw new TypeError('The "original" argument must be of type Function');
  }

  // We DO NOT return the promise as it gives the user a false sense that
  // the promise is actually somehow related to the callback's execution
  // and that the callback throwing will reject the promise.
  function callbackified() {
    var args = [];
    for (var i = 0; i < arguments.length; i++) {
      args.push(arguments[i]);
    }

    var maybeCb = args.pop();
    if (typeof maybeCb !== 'function') {
      throw new TypeError('The last argument must be of type Function');
    }
    var self = this;
    var cb = function() {
      return maybeCb.apply(self, arguments);
    };
    // In true node style we process the callback on `nextTick` with all the
    // implications (stack, `uncaughtException`, `async_hooks`)
    original.apply(this, args)
      .then(function(ret) { process.nextTick(cb, null, ret) },
            function(rej) { process.nextTick(callbackifyOnRejected, rej, cb) });
  }

  Object.setPrototypeOf(callbackified, Object.getPrototypeOf(original));
  Object.defineProperties(callbackified,
                          getOwnPropertyDescriptors(original));
  return callbackified;
}
exports.callbackify = callbackify;

/* WEBPACK VAR INJECTION */}.call(this, __webpack_require__(/*! ./../process/browser.js */ "./node_modules/process/browser.js"), __webpack_require__(/*! ./../console-browserify/index.js */ "./node_modules/console-browserify/index.js")))

/***/ }),

/***/ "./node_modules/webpack/buildin/global.js":
/*!***********************************!*\
  !*** (webpack)/buildin/global.js ***!
  \***********************************/
/*! no static exports found */
/***/ (function(module, exports) {

var g;

// This works in non-strict mode
g = (function() {
	return this;
})();

try {
	// This works if eval is allowed (see CSP)
	g = g || new Function("return this")();
} catch (e) {
	// This works if the window reference is available
	if (typeof window === "object") g = window;
}

// g can still be undefined, but nothing to do about it...
// We return undefined, instead of nothing here, so it's
// easier to handle this case. if(!global) { ...}

module.exports = g;


/***/ })

/******/ });
});
//# sourceMappingURL=opossum.map