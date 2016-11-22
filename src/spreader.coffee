_                   = require 'lodash'
EventEmitter2       = require 'eventemitter2'
debug               = require('debug')('slurry-spreader:spreader')
RedisNS             = require '@octoblu/redis-ns'
redis               = require 'ioredis'
Redlock             = require 'redlock'
async               = require 'async'
UUID                = require 'uuid'

class SlurrySpreader extends EventEmitter2
  constructor: ({@redisUri, @namespace, @lockTimeout}, dependencies={}) ->
    {@UUID} = dependencies

    throw new Error('SlurrySpreader: @redisUri is required') unless @redisUri?
    throw new Error('SlurrySpreader: @namespace is required') unless @namespace?

    @UUID ?= UUID
    @lockTimeout ?= 60 * 1000

  add: (slurry, callback) =>
    slurry = _.cloneDeep slurry
    {uuid} = slurry
    debug 'add', uuid

    slurry.nonce = @UUID.v4()

    tasks = [
      async.apply @redisClient.set, "data:#{uuid}", JSON.stringify(slurry)
      async.apply @redisClient.lrem, 'slurries', 0, uuid
      async.apply @redisClient.rpush, 'slurries', uuid
    ]

    async.series tasks, callback

  close: ({uuid}, callback) =>
    @_releaseLock uuid, callback

  connect: (callback) =>
    @slurries = {}
    @redisClient = new RedisNS @namespace, redis.createClient(@redisUri, dropBufferSupport: true)
    @queueClient = new RedisNS @namespace, redis.createClient(@redisUri, dropBufferSupport: true)

    @redlock = new Redlock [@queueClient], retryCount: 0
    callback()

  delay: ({ uuid, timeout }, callback) =>
    debug 'delay', uuid, timeout
    timeout ?= 60 * 1000
    return callback new Error "Not subscribed to this slurry: #{uuid}" unless @_isSubscribed uuid

    slurry = @slurries[uuid]
    slurry.lock.extend timeout, (error) =>
      return callback error if error?
      slurry.delayedUntil = Date.now() + timeout
      callback()
    return # stupid promises

  processQueue: (cb) =>
    callback = (error) =>
      return cb error if error?
      return _.delay cb, 100

    @queueClient.brpoplpush 'slurries', 'slurries', 30, (error, uuid) =>
      return callback error if error?
      return callback() unless uuid?
      return callback() if @_isDelayed uuid

      return @_extendOrReleaseLock uuid, callback if @_isSubscribed uuid
      return @_acquireLock uuid, callback
    return # stupid promises

  processQueueForever: =>
    async.until @_isStopped, @processQueue, (error) =>
      throw error if error?

  remove: ({ uuid }, callback) =>
    debug 'remove', uuid

    tasks = [
      async.apply @redisClient.del, "data:#{uuid}"
    ]

    async.series tasks, callback

  start: (callback) =>
    @connect (error) =>
      return callback error if error?
      @processQueueForever()
      callback()

  stop: (callback) =>
    @stopped = true
    async.eachSeries _.keys(@slurries), @_releaseLock, callback

  _acquireLock: (uuid, callback) =>
    debug '_acquireLock', "locks:#{uuid}", @lockTimeout
    @redlock.lock "locks:#{uuid}", @lockTimeout, (error, lock) =>
      return callback() if error?
      return callback() unless lock?
      @_createSlurry {uuid, lock}, callback

  _checkClaimableSlurry: (uuid, callback) =>
    @redisClient.exists "claim:#{uuid}", (error, exists) =>
      return callback error if error?
      return callback null, true if exists == 0
      return callback null, @_isSubscribed(uuid)

  _checkNonce: (slurry, callback) =>
    return callback null, false

  _claimSlurry: (uuid, callback) =>
    @redisClient.setex "claim:#{uuid}", 60, Date.now(), callback

  _createSlurry: ({uuid, lock}, callback) =>
    return callback() if @_isSubscribed uuid
    @_getSlurry uuid, (error, slurry) =>
      return callback error if error?
      @slurries[uuid] = {lock, nonce: slurry?.nonce}
      return @_releaseLockAndDelete uuid, callback unless slurry?
      @emit 'create', slurry
      callback()

  _extendLock: (uuid, callback) =>
    return callback() unless @_isSubscribed uuid

    slurry = @slurries[uuid]
    slurry.lock.extend @lockTimeout, callback

  _extendOrReleaseLock: (uuid, callback) =>
    return unless @_isSubscribed uuid

    @_getSlurry uuid, (error, slurryData) =>
      return callback error if error?
      return @_releaseLockAndDelete uuid, callback unless slurryData?
      return @_extendLock uuid, callback if slurryData?.nonce == @slurries[uuid].nonce
      return @_releaseLock uuid, callback

  _getSlurry: (uuid, callback) =>
    @redisClient.get "data:#{uuid}", (error, data) =>
      return callback error if error?
      @_jsonParse data, callback

  _isDelayed: (uuid) =>
    return false unless @_isSubscribed uuid
    return @slurries[uuid].delayedUntil > Date.now()

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

  _releaseLockAndDelete: (uuid, callback) =>
    debug '_releaseLockAndDelete'
    async.series [
      async.apply @redisClient.lrem, 'slurries', 0, uuid
      async.apply @_releaseLock, uuid
    ], callback

module.exports = SlurrySpreader
