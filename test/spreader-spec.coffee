{afterEach, beforeEach, describe, it} = global
{expect} = require 'chai'
sinon    = require 'sinon'

_       = require 'lodash'
async   = require 'async'
RedisNS = require '@octoblu/redis-ns'
redis   = require 'ioredis'
Redlock = require 'redlock'

SlurrySpreader = require '../src/spreader'

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
    rawClient = redis.createClient(dropBufferSupport: true)
    redisClient = new RedisNS 'test:slurry:spreader', rawClient
    @redlock = new Redlock [redisClient], retryCount: 0
    redisClient.on 'ready', => done()

  beforeEach (done) ->
    @UUID = v4: sinon.stub()
    @sut = new SlurrySpreader {
      redisUri: 'redis://localhost:6379'
      namespace: 'test:slurry:spreader'
      lockTimeout: 1000
    }, {@UUID}

    @sut.connect done

  afterEach (done) ->
    @sut.stop done

  describe '-> add', ->
    beforeEach (done) ->
      @UUID.v4.returns 'this-is-a-nonce'
      slurry =
        uuid: 'user-device-uuid'
        auth:
          uuid: 'cred-uuid'
          token: 'cred-token'
      @sut.add slurry, done

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
      @sut.remove slurry, done

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

  describe '-> close', ->
    describe 'when a slurry has been added', ->
      beforeEach (done) ->
        slurry =
          uuid: 'user-device-uuid'
          auth:
            uuid: 'cred-uuid'
            token: 'cred-token'
        @sut.add slurry, done

      describe 'and close is called', ->
        beforeEach (done) ->
          slurry =
            uuid: 'user-device-uuid'
            auth:
              uuid: 'cred-uuid'
              token: 'cred-token'
          @sut.close slurry, done
          return # stupid promises

        it 'should unlock the slurry', (done) ->
          @redlock.lock "locks:user-device-uuid", 1000, (error, lock) =>
            return done error if error?
            expect(lock).to.exist
            done()
          return # stupid promises

  describe 'processQueue', ->
    describe 'when a slurry is in the queue', ->
      beforeEach (done) ->
        data =
          uuid: 'user-device-uuid'
          nonce: 'this-is-a-nonce'
          auth:
            uuid: 'cred-uuid'
            token: 'cred-token'

        async.series [
          async.apply @redisClient.set, 'data:user-device-uuid', JSON.stringify(data)
          async.apply @redisClient.lpush, 'slurries', 'user-device-uuid'
          async.apply @sut.processQueue
        ], done

      it 'should lock the record', (done) ->
        @redlock.lock "locks:user-device-uuid", 1000, (error, lock) =>
          expect(lock).not.to.exist
          expect(error).to.exist
          done()
        return # stupid promises

      describe 'when the slurry comes back through the queue with the same nonce (20ms later)', ->
        beforeEach (done) ->
          async.series [
            (callback) => _.delay callback, 20
            async.apply @redisClient.lpush, 'slurries', 'user-device-uuid'
            async.apply @sut.processQueue
          ], done

        it 'should extend the lock', (done) ->
          wait75ms = (fn) => _.delay fn, 40
          wait75ms =>
            @redlock.lock "locks:user-device-uuid", 1000, (error, lock) =>
              expect(lock).not.to.exist
              expect(error).to.exist
              done()
            return # stupid promises

      describe 'when the slurry comes back through the queue with a different nonce', ->
        beforeEach (done) ->
          doneTwice = _.after 2, done

          data =
            uuid: 'user-device-uuid'
            nonce: 'different-nonce'
            auth:
              uuid: 'cred-uuid'
              token: 'cred-token'

          @onDestroy = sinon.spy => doneTwice()
          @sut.once 'destroy', @onDestroy
          async.series [
            async.apply @redisClient.set, 'data:user-device-uuid', JSON.stringify(data)
            async.apply @redisClient.lpush, 'slurries', 'user-device-uuid'
            async.apply @sut.processQueue
          ], doneTwice

        it 'should immediately cancel the lock', (done) ->
          @redlock.lock "locks:user-device-uuid", 1000, (error, lock) =>
            expect(error).not.to.exist
            expect(lock).to.exist
            done()
          return # stupid promises

        it 'should emit destroy with the uuid', ->
          expect(@onDestroy).to.have.been.calledWith {uuid: 'user-device-uuid'}

  describe 'emit: create', ->
    describe 'when a slurry is added', ->
      beforeEach (done) ->
        @UUID.v4.returns 'this-is-a-nonce'
        slurry =
          uuid: 'user-device-uuid'
          auth:
            uuid: 'cred-uuid'
            token: 'cred-token'
        @sut.add slurry, done

      describe 'and the queue is processed', ->
        beforeEach (done) ->
          doneTwice = _.after 2, done
          @sut.once 'create', (@slurry) =>
            doneTwice()
          @sut.processQueue doneTwice

        it 'should emit a slurry', ->
          expect(@slurry).to.containSubset {
            uuid: 'user-device-uuid'
            nonce: 'this-is-a-nonce'
            auth:
              uuid: 'cred-uuid'
              token: 'cred-token'
          }
