async         = require 'async'
EventEmitter2 = require 'eventemitter2'
redis         = require 'ioredis'
_             = require 'lodash'
Encryption    = require 'meshblu-encryption'
RedisNS       = require '@octoblu/redis-ns'
Redlock       = require 'redlock'
UUID          = require 'uuid'
debug         = require('debug')('slurry-spreader:spreader')

class SlurrySpreader extends EventEmitter2
  constructor: ({@redisUri, @namespace, @lockTimeout, privateKey}, dependencies={}) ->
    {@UUID} = dependencies

    throw new Error('SlurrySpreader: @redisUri is required') unless @redisUri?
    throw new Error('SlurrySpreader: @namespace is required') unless @namespace?
    throw new Error('SlurrySpreader: @privateKey is required') unless privateKey?

    @encryption = Encryption.fromJustGuess privateKey
    @UUID ?= UUID
    @lockTimeout ?= 60 * 1000

  add: (slurry, callback) =>
    slurry = _.cloneDeep slurry
    {uuid} = slurry
    debug 'add', uuid

    slurry.nonce = @UUID.v4()

    @_encrypt JSON.stringify(slurry), (error, encrypted) =>
      return callback error if error?

      tasks = [
        async.apply @redisClient.set, "data:#{uuid}", encrypted
        async.apply @redisClient.lrem, 'slurries', 0, uuid
        async.apply @redisClient.rpush, 'slurries', uuid
      ]

      async.series tasks, callback

  close: ({uuid}, callback) =>
    debug 'close', uuid
    @_releaseLockAndUnsubscribe uuid, callback

  connect: (callback) =>
    @slurries = {}
    @redisClient = new RedisNS @namespace, redis.createClient(@redisUri, dropBufferSupport: true)
    @queueClient = new RedisNS @namespace, redis.createClient(@redisUri, dropBufferSupport: true)

    @redlock = new Redlock [@queueClient], retryCount: 0
    @redlock.on 'clientError', (error) =>
      debug 'A redis error has occurred:', error

    callback()

  delay: ({ uuid, timeout }, callback) =>
    debug 'delay', uuid, timeout
    timeout ?= 60 * 1000
    timeout = Math.round(timeout/1000)
    return callback new Error "Not subscribed to this slurry: #{uuid}" unless @_isSubscribed uuid
    @redisClient.setex "delay:#{uuid}", timeout, Date.now(), callback
    return # stupid promises

  processQueue: (cb) =>
    callback = (error) =>
      return cb error if error?
      return _.delay cb, 100

    @queueClient.brpoplpush 'slurries', 'slurries', 30, (error, uuid) =>
      return callback error if error?
      return callback() unless uuid?

      @_isEncrypted uuid, (error, isEncrypted) =>
        return callback error if error?
        return @_encryptAndRelease uuid, callback unless isEncrypted

        @_isDelayed uuid, (error, delayed) =>
          return callback error if error?
          return callback() if delayed == true
          return @_extendOrReleaseLock uuid, callback if @_isSubscribed(uuid)
          return @_acquireLock uuid, callback

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
    async.each locksToExtend, @_extendLock, (error) =>
      debug "extended #{locksToExtend.length} locks. Error: #{error?.stack}"

  remove: ({ uuid }, callback) =>
    debug 'remove', uuid
    @redisClient.del "data:#{uuid}", callback
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
    @redisClient.get "data:#{uuid}", (error, decrypted) =>
      return callback error if error?
      @_encrypt decrypted, (error, encrypted) =>
        return callback error if error?
        @redisClient.set "data:#{uuid}", encrypted, (error) =>
          return callback error if error?
          @_releaseLockAndUnsubscribe uuid, callback

  _extendLock: (uuid, callback) =>
    debug '_extendLock', uuid
    return callback() unless @_isSubscribed uuid

    slurry = @slurries[uuid]
    slurry.lock.extend @lockTimeout, callback

  _extendOrReleaseLock: (uuid, callback) =>
    return callback() unless @_isSubscribed uuid

    @_getSlurry uuid, (error, slurryData) =>
      return callback error if error?
      return callback() unless @_isSubscribed uuid # Might no longer be subscribed
      return @_releaseLockAndRemoveFromQueue uuid, callback if _.isEmpty slurryData
      return @_unsubscribe uuid, callback                   if @_isLockExpired uuid
      return @_extendLock uuid, callback                    if slurryData.nonce == @slurries[uuid].nonce
      return @_releaseLockAndUnsubscribe uuid, callback

  _getSlurry: (uuid, callback) =>
    @redisClient.get "data:#{uuid}", (error, encrypted) =>
      return callback error if error?

      @_decrypt encrypted, (error, decrypted) =>
        return callback error if error?

        @_jsonParse decrypted, callback

  _isDelayed: (uuid, callback) =>
    @redisClient.exists "delay:#{uuid}", (error, delayed) =>
      debug "isDelayed: #{uuid} = #{delayed}"
      return callback error, (delayed==1)

  _isEncrypted: (uuid, callback) =>
    @redisClient.get "data:#{uuid}", (error, encrypted) =>
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
    async.series [
      async.apply @redisClient.lrem, 'slurries', 0, uuid
      async.apply @_releaseLockAndUnsubscribe, uuid
    ], callback

  _unsubscribe: (uuid, callback) =>
    debug '_unsubscribe', uuid
    slurry = @slurries[uuid]
    delete @slurries[uuid]
    callback null, slurry

module.exports = SlurrySpreader
