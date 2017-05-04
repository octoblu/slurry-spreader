_             = require 'lodash'
async         = require 'async'
EventEmitter2 = require 'eventemitter2'
moment        = require 'moment'
Redis         = require 'ioredis'
Encryption    = require 'meshblu-encryption'
RedisNS       = require '@octoblu/redis-ns'
Redlock       = require 'redlock'
UUID          = require 'uuid'
debug         = require('debug')('slurry-spreader:spreader')
GenericPool   = require 'generic-pool'
When          = require 'when'

class SlurrySpreader extends EventEmitter2
  constructor: ({@redisUri, @namespace, @lockTimeout, privateKey}, dependencies={}) ->
    {@UUID} = dependencies

    throw new Error('SlurrySpreader: @redisUri is required') unless @redisUri?
    throw new Error('SlurrySpreader: @namespace is required') unless @namespace?
    throw new Error('SlurrySpreader: @privateKey is required') unless privateKey?

    @encryption = Encryption.fromJustGuess privateKey
    @UUID ?= UUID
    @lockTimeout ?= 60 * 1000
    @minConnections = 1
    @maxConnections = 10
    @idleTimeoutMillis = 60000
    @_queuePool = @_createRedisPool { @maxConnections, @minConnections, @idleTimeoutMillis, @namespace, @redisUri }
    @_commandPool = @_createRedisPool { @maxConnections, @minConnections, @idleTimeoutMillis, @namespace, @redisUri }

  add: (slurry, callback) =>
    slurry = _.cloneDeep slurry
    {uuid} = slurry
    debug 'add', uuid

    slurry.nonce = @UUID.v4()

    @_encrypt JSON.stringify(slurry), (error, encrypted) =>
      return callback error if error?

      @_commandPool.acquire().then (redisClient) =>
        tasks = [
          async.apply redisClient.set, "data:#{uuid}", encrypted
          async.apply redisClient.lrem, 'slurries', 0, uuid
          async.apply redisClient.rpush, 'slurries', uuid
        ]

        async.series tasks, (error) =>
          @_commandPool.release redisClient
          callback error

    return # promises

  close: ({uuid}, callback) =>
    debug 'close', uuid
    @_releaseLockAndUnsubscribe uuid, callback

  connect: (callback) =>
    @slurries = {}

    @_queuePool.acquire().then (redlockClient) =>
      @redlock = new Redlock [redlockClient], retryCount: 0
      @redlock.on 'clientError', (error) =>
        debug 'A redis error has occurred:', error
        @stop _.noop

      return callback()
    .catch callback
    return # promises

  delay: ({ uuid, timeout }, callback) =>
    debug 'delay', uuid, timeout
    timeout ?= 60 * 1000
    timeout = Math.round(timeout/1000)
    return callback new Error "Not subscribed to this slurry: #{uuid}" unless @_isSubscribed uuid
    @_commandPool.acquire().then (redisClient) =>
      redisClient.setex "delay:#{uuid}", timeout, Date.now(), callback
      @_commandPool.release redisClient
    return # stupid promises

  processQueue: (cb) =>
    callback = (error) =>
      return cb error if error?
      @emit 'processedQueue'
      return _.delay cb, 100

    @_queuePool.acquire().then (queueClient) =>
      queueClient.brpoplpush 'slurries', 'slurries', 30, (error, uuid) =>
        @_queuePool.release queueClient
        return callback error if error?
        return callback() unless uuid?

        @_isEncrypted uuid, (error, isEncrypted) =>
          return callback error if error?
          return @_encryptAndRelease uuid, callback unless isEncrypted

          @_isDelayed uuid, (error, delayed) =>
            return callback error if error?
            return callback() if delayed == true
            return callback() if @_isSubscribed(uuid)
            return @_acquireLock uuid, callback
    .catch callback
    return # stupid promises

  processQueueForever: =>
    async.until @_isStopped, @processQueue, (error) =>
      throw error if error?

  extendLocksForever: =>
    debug 'gonna extend the locks FOREVER'
    @_extendLockInterval =  setInterval @_extendLockOnInterval, Math.floor(@lockTimeout / 2)

  _extendLockOnInterval: =>
    return clearInterval @_extendLockInterval if @_isStopped()
    locksToExtend = _.keys @slurries
    debug "about to extend a bunch of locks. Probably, like, #{locksToExtend.length} or something."
    async.each locksToExtend, @_extendOrReleaseLock, (error) =>
      debug "extended #{locksToExtend.length} locks. Error: #{error?.stack}"

  _emitOnlineUntil: (slurry) =>
    onlineUntil = moment().utc().add(@lockTimeout, 'ms')
    @emit 'onlineUntil', {slurry, onlineUntil}

  remove: ({ uuid }, callback) =>
    debug 'remove', uuid
    @_commandPool.acquire().then (redisClient) =>
      redisClient.del "data:#{uuid}", callback
      @_commandPool.release redisClient
    .catch callback
    return # stupid promises

  start: (callback) =>
    @connect (error) =>
      return callback error if error?
      @processQueueForever()
      @extendLocksForever()
      callback()

  stop: (callback) =>
    debug 'stop'
    @stopped = true
    async.eachSeries _.keys(@slurries), @_releaseLockAndUnsubscribe, callback

  _acquireLock: (uuid, callback) =>
    debug '_acquireLock', uuid
    @redlock.lock "locks:#{uuid}", @lockTimeout, (error, lock) =>
      return callback() if error?
      return callback() unless lock?
      debug 'acquiredLock', "locks:#{uuid}", @lockTimeout
      @_createSlurry {uuid, lock}, callback

  _createSlurry: ({uuid, lock}, callback) =>
    debug '_createSlurry', uuid
    return callback() if @_isSubscribed uuid
    @_getSlurry uuid, (error, slurry) =>
      return callback error if error?
      @slurries[uuid] = {lock, nonce: slurry?.nonce}
      debug "created slurry: #{uuid}"
      return @_releaseLockAndRemoveFromQueue uuid, callback unless slurry?
      @emit 'create', slurry
      callback()

  _decrypt: (encrypted, callback) =>
    try
      callback null, @encryption.decrypt encrypted
    catch error
      callback error

  _encrypt: (data, callback) =>
    try
      callback null, @encryption.encrypt data
    catch error
      callback error

  _encryptAndRelease: (uuid, callback) =>
    debug '_encryptAndRelease', uuid
    @_commandPool.acquire().then (redisClient) =>
      redisClient.get "data:#{uuid}", (error, decrypted) =>
        @_commandPool.release redisClient
        return callback error if error?
        @_encrypt decrypted, (error, encrypted) =>
          return callback error if error?
          @_commandPool.acquire().then (redisClient) =>
            redisClient.set "data:#{uuid}", encrypted, (error) =>
              @_commandPool.release redisClient
              return callback error if error?
              @_releaseLockAndUnsubscribe uuid, callback

  _extendLock: (uuid, callback) =>
    debug '_extendLock', uuid
    return callback() unless @_isSubscribed uuid

    slurry = @slurries[uuid]
    slurry.lock.extend @lockTimeout, callback

  _extendOrReleaseLock: (uuid, callback) =>
    debug "_extendOrReleaseLock #{uuid}: #{@slurries[uuid].nonce}"

    @_getSlurry uuid, (error, slurryData) =>
      if error?
        @_unsubscribe uuid, _.noop
        return callback error
      return callback() unless @_isSubscribed uuid # Might no longer be subscribed
      return @_releaseLockAndRemoveFromQueue uuid, callback if _.isEmpty slurryData
      return @_unsubscribe uuid, callback                   if @_isLockExpired uuid
      return @_releaseLockAndUnsubscribe uuid, callback     unless slurryData.nonce == @slurries[uuid].nonce

      @_emitOnlineUntil slurryData
      return @_extendLock uuid, callback


  _getSlurry: (uuid, callback) =>
    @_commandPool.acquire().then (redisClient) =>
      redisClient.get "data:#{uuid}", (error, encrypted) =>
        @_commandPool.release redisClient
        return callback error if error?

        @_decrypt encrypted, (error, decrypted) =>
          return callback error if error?

          @_jsonParse decrypted, callback

  _isDelayed: (uuid, callback) =>
    @_commandPool.acquire().then (redisClient) =>
      redisClient.exists "delay:#{uuid}", (error, delayed) =>
        @_commandPool.release redisClient
        debug "isDelayed: #{uuid} = #{delayed}"
        return callback error, (delayed==1)

  _isEncrypted: (uuid, callback) =>
    @_commandPool.acquire().then (redisClient) =>
      redisClient.get "data:#{uuid}", (error, encrypted) =>
        @_commandPool.release redisClient
        return callback error if error?
        @_decrypt encrypted, (error) =>
          return callback null, false if error? # If we can't decrypt it, it ain't encrypted
          return callback null, true

  _isLockExpired: (uuid) =>
    expiration = @slurries[uuid].lock.expiration
    now = Date.now()
    debug '_isLockExpired', uuid, expiration, now, expiration < now
    return expiration < now

  _isStopped: =>
    @stopped

  _isSubscribed: (uuid) =>
    @slurries[uuid]?

  _jsonParse: (data, callback) =>
    return callback() if _.isEmpty data
    try
      return callback null, JSON.parse(data)
    catch error
      return callback error

  _releaseLock: (uuid, callback) =>
    debug '_releaseLock', uuid
    return callback() unless @_isSubscribed uuid
    @emit 'destroy', {uuid}
    slurry = @slurries[uuid]
    slurry.lock.unlock callback

  _releaseLockAndUnsubscribe: (uuid, callback) =>
    debug '_releaseLockAndUnsubscribe', uuid
    return callback() unless @_isSubscribed uuid

    @emit 'destroy', {uuid}
    @_unsubscribe uuid, (error, slurry) =>
      slurry.lock.unlock callback

  _releaseLockAndRemoveFromQueue: (uuid, callback) =>
    debug '_releaseLockAndRemoveFromQueue', uuid
    @_commandPool.acquire().then (redisClient) =>
      async.series [
        async.apply redisClient.lrem, 'slurries', 0, uuid
        async.apply @_releaseLockAndUnsubscribe, uuid
      ], (error) =>
        @_commandPool.release redisClient
        callback error

  _unsubscribe: (uuid, callback) =>
    debug '_unsubscribe', uuid
    slurry = @slurries[uuid]
    delete @slurries[uuid]
    callback null, slurry

  _createRedisPool: ({ maxConnections, minConnections, idleTimeoutMillis, evictionRunIntervalMillis, acquireTimeoutMillis, namespace, redisUri }) =>
    factory =
      create: =>
        return When.promise (resolve, reject) =>
          conx = new Redis redisUri, dropBufferSupport: true
          client = new RedisNS namespace, conx
          rejectError = (error) =>
            return reject error

          client.once 'error', rejectError
          client.once 'ready', =>
            client.removeListener 'error', rejectError
            resolve client

      destroy: (client) =>
        return When.promise (resolve, reject) =>
          @_closeClient client, (error) =>
            return reject error if error?
            resolve()

      validate: (client) =>
        return When.promise (resolve) =>
          client.ping (error) =>
            return resolve false if error?
            resolve true

    options = {
      max: maxConnections
      min: minConnections
      testOnBorrow: true
      idleTimeoutMillis
      evictionRunIntervalMillis
      acquireTimeoutMillis
    }

    pool = GenericPool.createPool factory, options

    pool.on 'factoryCreateError', (error) =>
      @emit 'factoryCreateError', error

    return pool

module.exports = SlurrySpreader
