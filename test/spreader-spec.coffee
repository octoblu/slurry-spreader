_              = require 'lodash'
async          = require 'async'
SlurrySpreader = require '../src/spreader'
RedisNS        = require '@octoblu/redis-ns'
redis          = require 'ioredis'

describe 'connect slurry stream', ->
  beforeEach (done) ->
    @redisClient = new RedisNS 'test:slurry:spreader', redis.createClient(dropBufferSupport: true)
    @redisClient.on 'ready', =>
      @redisClient.del 'subscriptions', 'user-device-uuid', done

  beforeEach (done) ->
    @spreader = new SlurrySpreader
      redisUri: 'redis://localhost:6379'
      namespace: 'test:slurry:spreader'

    @spreader.start done

  afterEach (done) ->
    @spreader.stop done

  describe '-> add', ->
    beforeEach (done) ->
      slurry =
        uuid: 'user-device-uuid'
        auth:
          uuid: 'cred-uuid'
          token: 'cred-token'
      @spreader.add slurry, done

    it 'should insert the uuid into the slurries queue in redis', (done) ->
      @members = []
      checkList = (callback) =>
        @redisClient.lrange 'slurries', 0, -1, (error, @members) =>
          return callback error if error?
          callback()

      async.until (=> _.includes @members, 'user-device-uuid'), checkList, (error) =>
        return done error if error?
        expect(@members).to.include 'user-device-uuid'
        done()

    it 'should save the metadata in redis', (done) ->
      @redisClient.get 'data:user-device-uuid', (error, data) =>
        return done error if error?
        expectedData =
          uuid: 'user-device-uuid'
          auth:
            uuid: 'cred-uuid'
            token: 'cred-token'

        expect(JSON.parse data).to.deep.equal expectedData
        done()

    describe 'emit: create', ->
      beforeEach (done) ->
        @spreader.once 'create', (@slurry) =>
          done()

      it 'should be a slurry', ->
        expectedData =
          uuid: 'user-device-uuid'
          auth:
            uuid: 'cred-uuid'
            token: 'cred-token'

        expect(@slurry).to.deep.equal expectedData
