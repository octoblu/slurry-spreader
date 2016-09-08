_              = require 'lodash'
async          = require 'async'
SlurrySpreader = require '../src/spreader'
RedisNS        = require '@octoblu/redis-ns'
redis          = require 'ioredis'

describe 'connect slurry stream', ->
  beforeEach (done) ->
    rawClient = redis.createClient(dropBufferSupport: true)
    @redisClient = new RedisNS 'test:slurry:spreader', rawClient
    @redisClient.on 'ready', =>
      @redisClient.keys '*', (error, keys) =>
        return done error if error?
        return done() if _.isEmpty keys
        rawClient.del keys..., done

  beforeEach (done) ->
    @UUID = v4: sinon.stub()
    @spreader = new SlurrySpreader {
      redisUri: 'redis://localhost:6379'
      namespace: 'test:slurry:spreader'
    }, {@UUID}

    @spreader.start done

  afterEach (done) ->
    @spreader.stop done

  describe '-> add', ->
    beforeEach (done) ->
      @UUID.v4.returns 'this-is-a-nonce'
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
          nonce: 'this-is-a-nonce'
          auth:
            uuid: 'cred-uuid'
            token: 'cred-token'

        expect(JSON.parse data).to.deep.equal expectedData
        done()
      return # stupid promises

  describe '-> remove', ->
    beforeEach (done) ->
      slurry =
        uuid: 'user-device-uuid'
        auth:
          uuid: 'cred-uuid'
          token: 'cred-token'
      @spreader.remove slurry, done

    it 'should remove the uuid from the slurries list', (done) ->
      @members = []
      checkList = (callback) =>
        @redisClient.lrange 'slurries', 0, -1, (error, @members) =>
          return callback error if error?
          callback()

      async.until (=> !_.includes @members, 'user-device-uuid'), checkList, (error) =>
        return done error if error?
        expect(@members).not.to.include 'user-device-uuid'
        done()

    it 'should remove the metadata in redis', (done) ->
      @redisClient.exists 'data:user-device-uuid', (error, exists) =>
        return done error if error?
        expect(exists).to.equal 0
        done()
      return # stupid promises

  describe '-> end', ->
    beforeEach (done) ->
      slurry =
        uuid: 'user-device-uuid'
        auth:
          uuid: 'cred-uuid'
          token: 'cred-token'
      @spreader.end slurry, done

    it 'should remove the claim in redis', (done) ->
      @redisClient.exists 'claim:user-device-uuid', (error, exists) =>
        return done error if error?
        expect(exists).to.equal 0
        done()
      return # stupid promises

  describe 'emit: create', ->
    beforeEach (done) ->
      doneTwice = _.after 2, done
      @spreader.once 'create', (@slurry) =>
        doneTwice()

      @UUID.v4.returns 'this-is-a-nonce'
      slurry =
        uuid: 'user-device-uuid'
        auth:
          uuid: 'cred-uuid'
          token: 'cred-token'
      @spreader.add slurry, doneTwice

    it 'should be a slurry', ->
      expectedData =
        uuid: 'user-device-uuid'
        nonce: 'this-is-a-nonce'
        auth:
          uuid: 'cred-uuid'
          token: 'cred-token'

      expect(@slurry).to.deep.equal expectedData

  describe 'emit: destroy', ->
    beforeEach (done) ->
      doneThrice = _.after 3, done
      @spreader.once 'create', (slurry) =>
        @UUID.v4.returns 'another-nonce'
        @spreader.add slurry, doneThrice

      @spreader.once 'destroy', (@slurry) =>
        doneThrice()

      @UUID.v4.returns 'this-is-a-nonce'
      slurry =
        uuid: 'user-device-uuid'
        auth:
          uuid: 'cred-uuid'
          token: 'cred-token'
      @spreader.add slurry, doneThrice

    it 'should be a slurry', ->
      expectedData =
        uuid: 'user-device-uuid'
        nonce: 'another-nonce'
        auth:
          uuid: 'cred-uuid'
          token: 'cred-token'

      expect(@slurry).to.deep.equal expectedData
