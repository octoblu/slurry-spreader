{afterEach, beforeEach, describe, it} = global
{expect} = require 'chai'
sinon    = require 'sinon'

async      = require 'async'
fs         = require 'fs'
redis      = require 'ioredis'
_          = require 'lodash'
Encryption = require 'meshblu-encryption'
RedisNS    = require '@octoblu/redis-ns'
path       = require 'path'
Redlock    = require 'redlock'

PRIVATE_KEY = fs.readFileSync path.join(__dirname, 'fixtures/private-key.pem')
SlurrySpreader = require '../src/spreader'

xdescribe 'connect slurry stream', ->
  @timeout 10 * 60 * 1000

  beforeEach (done) ->
    @encryption = Encryption.fromJustGuess PRIVATE_KEY

    rawClient = redis.createClient(dropBufferSupport: true)
    @redisClient = new RedisNS 'test:slurry:spreader', rawClient
    @redisClient.on 'ready', =>
      @redisClient.keys '*', (error, keys) =>
        return done error if error?
        return done() if _.isEmpty keys
        rawClient.del keys..., done

  beforeEach (done) ->
    rawClient = redis.createClient(dropBufferSupport: true)
    redisClient = new RedisNS 'test:slurry:spreader', rawClient
    @redlock = new Redlock [redisClient], retryCount: 0
    redisClient.on 'ready', => done()

  beforeEach 'sut', (done) ->
    @sut = new SlurrySpreader {
      redisUri: 'redis://localhost:6379'
      namespace: 'test:slurry:spreader'
      lockTimeout: 1000
      privateKey: PRIVATE_KEY
    }

    @sut.connect (error) =>
      done error if error?
      @sut.start done

  afterEach (done) ->
    @sut.stop done

  beforeEach 'sut2', (done) ->
    @sut2 = new SlurrySpreader {
      redisUri: 'redis://localhost:6379'
      namespace: 'test:slurry:spreader'
      lockTimeout: 1
      privateKey: PRIVATE_KEY
    }

    @sut2.connect (error) =>
      done error if error?
      @sut2.start done

  afterEach (done) ->
    @sut2.stop done

  describe 'when we have 1 uuid', ->
    beforeEach ->
      slurry =
        uuid: 'user-device-uuid'
        auth:
          uuid: 'cred-uuid'
          token: 'cred-token'

      @sut.add slurry, => console.log 'added slurry1'
      @sut2.add slurry, => console.log 'added slurry2'

      @_checkBothUuidArrays = =>
        sutSlurries = _.keys(@sut.slurries)
        sut2Slurries = _.keys(@sut2.slurries)
        console.log '-----------'
        console.log 'sut', sutSlurries
        console.log 'sut2', sut2Slurries
        console.log '-----------\n'
        # async.each sutSlurries, @sut._releaseLockAndUnsubscribe, =>
        #   async.each sut2Slurries, @sut2._releaseLockAndUnsubscribe, =>

    it 'should not be in both suts', (done) ->
      setInterval @_checkBothUuidArrays, 1000
