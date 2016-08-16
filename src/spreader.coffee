_                   = require 'lodash'
EventEmitter2       = require 'eventemitter2'
debug               = require('debug')('slurry-spreader:spreader')
RedisNS             = require '@octoblu/redis-ns'
redis               = require 'ioredis'
Redlock             = require 'redlock'
async               = require 'async'

class SlurrySpreader extends EventEmitter2
  constructor: (options) ->
    {
      @redisUri
      @namespace
    } = options
    throw new Error('SlurrySpreader: @redisUri is required') unless @redisUri?
    throw new Error('SlurrySpreader: @namespace is required') unless @namespace?

  connect: (callback) =>
    @slurries = {}
    @slurryLookup = {}
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

    tasks = [
      async.apply @redisClient.rpush, 'slurries', uuid
      async.apply @redisClient.set, "data:#{uuid}", JSON.stringify(slurry)
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
        async.apply @_updateSlurries, uuid
        async.apply @_emitSlurry, uuid
      ], callback

  _checkClaimableSlurry: (uuid, callback) =>
    @redisClient.exists uuid, (error, exists) =>
      return callback error if error?
      claimable = !@_isSubscribed(uuid) or !exists
      return callback null, claimable

  _claimSlurry: (uuid, callback) =>
    @redisClient.setex uuid, Date.now(), 60, (error) =>
      return callback error if error?
      callback()

  _emitSlurry: (uuid, callback) =>
      @emit 'create', slurry
      callback()

  _updateSlurries: (uuid, callback) =>
    return callback() if @_isSubscribed uuid
    @redisClient.get "data:#{uuid}", (error, data) =>
      return callback error if error?
      try
        slurry = JSON.parse(data)
      catch error
        return callback error

      @slurryLookup[uuid] = slurry
      @slurries[uuid] = true
      callback()

  _isSubscribed: (uuid) =>
    @slurries[uuid]?

module.exports = SlurrySpreader
