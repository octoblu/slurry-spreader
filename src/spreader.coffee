_                   = require 'lodash'
EventEmitter2       = require 'eventemitter2'
debug               = require('debug')('slurry-spreader:spreader')
RedisNS             = require '@octoblu/redis-ns'
redis               = require 'ioredis'
Redlock             = require 'redlock'
async               = require 'async'
UUID                = require 'uuid'

class SlurrySpreader extends EventEmitter2
  constructor: (options, dependencies) ->
    {
      @redisUri
      @namespace
    } = options
    {
      @UUID
    } = dependencies
    @UUID ?= UUID
    throw new Error('SlurrySpreader: @redisUri is required') unless @redisUri?
    throw new Error('SlurrySpreader: @namespace is required') unless @namespace?

  connect: (callback) =>
    @slurries = {}
    @redisClient = new RedisNS @namespace, redis.createClient(@redisUri, dropBufferSupport: true)
    @queueClient = new RedisNS @namespace, redis.createClient(@redisUri, dropBufferSupport: true)

    redlockOptions =
      retryCount: 100
      retryDelay: 100
    @redlock = new Redlock [@queueClient], redlockOptions
    callback()

  start: (callback) =>
    @connect (error) =>
      return callback error if error?
      @_processQueueForever()
      callback()

  stop: (callback) =>
    @stopped = true
    callback()

  _processQueueForever: =>
    async.forever @_processQueue, (error) =>
      throw error if error? && !@stopped

  add: (slurry, callback) =>
    {
      uuid
    } = slurry

    nonce = @UUID.v4()
    slurry.nonce = nonce

    tasks = [
      async.apply @redisClient.set, "data:#{uuid}", JSON.stringify(slurry)
      async.apply @redisClient.rpush, 'slurries', uuid
    ]

    async.series tasks, callback

  remove: (slurry, callback) =>
    {
      uuid
    } = slurry

    tasks = [
      async.apply @redisClient.del, "data:#{uuid}"
      async.apply @redisClient.lrem, 'slurries', 1, uuid
    ]

    async.series tasks, callback

  _processQueue: (callback) =>
    @queueClient.brpoplpush 'slurries', 'slurries', 30, (error, uuid) =>
      return callback new Error('stopping') if @stopped
      return callback error if error?
      return callback() unless uuid?

      @_acquireLock uuid, (error) =>
        return callback error if error?
        setTimeout callback, 100

  _acquireLock: (uuid, callback) =>
    @redlock.lock "locks:#{uuid}", 60*1000, (error, lock) =>
      return callback error if error?
      return callback() unless lock?
      @_handleSlurry uuid, (error) =>
        lock.unlock()
        callback error

  _handleSlurry: (uuid, callback) =>
    @_checkClaimableSlurry uuid, (error, claimable) =>
      return callback error if error?
      return callback() unless claimable
      async.series [
        async.apply @_claimSlurry, uuid
        async.apply @_createSlurry, uuid
        async.apply @_destroySlurry, uuid
      ], callback

  _checkClaimableSlurry: (uuid, callback) =>
    @redisClient.exists "claim:#{uuid}", (error, exists) =>
      return callback error if error?
      return callback null, true if exists == 0
      return callback null, @_isSubscribed(uuid)

  _claimSlurry: (uuid, callback) =>
    @redisClient.setex "claim:#{uuid}", Date.now(), 60, callback

  _getSlurry: (uuid, callback) =>
    @redisClient.get "data:#{uuid}", (error, data) =>
      return callback error if error?
      try
        slurry = JSON.parse(data)
      catch error
        return callback error

      callback null, slurry

  _checkNonce: (slurry, callback) =>
    return callback null, false

  _createSlurry: (uuid, callback) =>
    return callback() if @_isSubscribed uuid
    @_getSlurry uuid, (error, slurry) =>
      return callback error if error?
      @slurries[uuid] = slurry.nonce
      @emit 'create', slurry
      callback()

  _destroySlurry: (uuid, callback) =>
    return callback() unless @_isSubscribed uuid
    @_getSlurry uuid, (error, slurry) =>
      return callback error if error?
      return callback() if @slurries[uuid] == slurry.nonce
      delete @slurries[uuid]
      @emit 'destroy', slurry
      callback()

  _isSubscribed: (uuid) =>
    @slurries[uuid]?

module.exports = SlurrySpreader
