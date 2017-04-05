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

describe 'connect slurry stream', ->
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

  beforeEach (done) ->
    @UUID = v4: sinon.stub()
    @sut = new SlurrySpreader {
      redisUri: 'redis://localhost:6379'
      namespace: 'test:slurry:spreader'
      lockTimeout: 1000
      privateKey: PRIVATE_KEY
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

    it 'should encrypt the metadata and save in redis', (done) ->
      @redisClient.get 'data:user-device-uuid', (error, data) =>
        return done error if error?
        decrypted = @encryption.decrypt data
        expectedData =
          uuid: 'user-device-uuid'
          nonce: 'this-is-a-nonce'
          auth:
            uuid: 'cred-uuid'
            token: 'cred-token'

        expect(JSON.parse decrypted).to.deep.equal expectedData
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

  describe '-> delay', ->
    describe 'when a slurry is in the queue', ->
      beforeEach (done) ->
        data =
          uuid: 'user-device-uuid'
          nonce: 'this-is-a-nonce'
          auth:
            uuid: 'cred-uuid'
            token: 'cred-token'

        encrypted = @encryption.encrypt JSON.stringify data

        async.series [
          async.apply @redisClient.set, 'data:user-device-uuid', encrypted
          async.apply @redisClient.lpush, 'slurries', 'user-device-uuid'
          async.apply @sut.processQueue
        ], done

      describe 'when delay is not called', ->
        beforeEach (done) ->
          @sut._isDelayed 'user-device-uuid', (error, @delayed) =>
            done()
          return

        it 'sut._isDelayed should be false', ->
          expect(@delayed).to.equal false

      describe 'when delay is called', ->
        beforeEach (done) ->
          @sut.delay uuid: 'user-device-uuid', timeout: 1500, done

        it 'should be delayed in sut._isDelayed', (done) ->
          @sut._isDelayed 'user-device-uuid', (error, delayed) =>
            expect(delayed).to.equal true
            done()
          return

        it 'should set the delay redis property', (done) ->
          wait110ms = (fn) => _.delay fn, 1100
          wait110ms =>
            @redisClient.exists "delay:user-device-uuid", (error, delay) =>
              expect(delay).to.equal 1
              expect(error).to.not.exist
              done()
            return # stupid promises

        describe 'when the queue is process', ->
          beforeEach (done) ->
            @sut.processQueue done

          it 'should still be delayed', (done) ->
            wait110ms = (fn) => _.delay fn, 1100
            wait110ms =>
              @redisClient.exists "delay:user-device-uuid", (error, delay) =>
                expect(delay).to.equal 1
                expect(error).to.not.exist
                done()
              return # stupid promises

  describe '-> start', ->
    describe 'when a slurry is in the queue', ->
      beforeEach (done) ->
        data =
          uuid: 'user-device-uuid'
          nonce: 'this-is-a-nonce'
          auth:
            uuid: 'cred-uuid'
            token: 'cred-token'

        encrypted = @encryption.encrypt JSON.stringify data

        @sut.once 'processedQueue', done

        async.series [
          async.apply @redisClient.set, 'data:user-device-uuid', encrypted
          async.apply @redisClient.lpush, 'slurries', 'user-device-uuid'
          async.apply @sut.start
        ], (error) => done(error) if error?

      it 'should lock the record', (done) ->
        @redlock.lock "locks:user-device-uuid", 1000, (error, lock) =>
          expect(lock).not.to.exist
          expect(error).to.exist
          done()
        return # stupid promises

      describe 'when the slurry comes back through the queue with the same nonce (20ms later)', ->
        beforeEach (done) ->

          @sut.once 'processedQueue', done

          async.series [
            (callback) => _.delay callback, 20
            async.apply @redisClient.lpush, 'slurries', 'user-device-uuid'
            async.apply @sut.start
          ], (error) => done(error) if error?

        it 'should extend the lock', (done) ->
          wait40ms = (fn) => _.delay fn, 40
          wait40ms =>
            @redlock.lock "locks:user-device-uuid", 1000, (error, lock) =>
              expect(lock).not.to.exist
              expect(error).to.exist
              done()
            return # stupid promises

      describe 'when the slurry comes back through the queue with a different nonce', ->
        beforeEach (done) ->

          @onDestroy = =>
            @sut.stop done

          data =
            uuid: 'user-device-uuid'
            nonce: 'same-nonce'
            auth:
              uuid: 'cred-uuid'
              token: 'cred-token'

          encrypted = @encryption.encrypt JSON.stringify data

          @sut.on 'processedQueue', =>
            data.nonce = Date.now()
            encrypted = @encryption.encrypt JSON.stringify data
            @redisClient.set 'data:user-device-uuid', encrypted, =>

          @sut.once 'destroy', @onDestroy

          async.series [
            async.apply @sut.start
            async.apply @redisClient.set, 'data:user-device-uuid', encrypted
            async.apply @redisClient.lpush, 'slurries', 'user-device-uuid'
          ], (error) => done(error) if error?

        it 'should cancel the lock at the unlock interval', (done) ->
          wait1000ms = (fn) => _.delay fn, 1000
          wait1000ms =>
            @redlock.lock "locks:user-device-uuid", 1000, (error, lock) =>
              console.log error.message if error?
              expect(error).not.to.exist
              expect(lock).to.exist
              done()
            return # stupid promises

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

    describe 'when an old (unencrypted) slurry is present', ->
      beforeEach (done) ->
        slurry =
          uuid: 'user-device-uuid'
          nonce: 'this-is-a-nonce'
          auth:
            uuid: 'cred-uuid'
            token: 'cred-token'

        async.series [
          async.apply @redisClient.set, 'data:user-device-uuid', JSON.stringify(slurry)
          async.apply @redisClient.lpush, 'slurries', 'user-device-uuid'
        ], done

      describe 'and the queue is processed', ->
        beforeEach (done) ->
          @sut.processQueue done

        it 'should encrypt the data', (done) ->
          @redisClient.get 'data:user-device-uuid', (error, encrypted) =>
            return done error if error?

            expect(JSON.parse @encryption.decrypt encrypted).to.containSubset {
              uuid: 'user-device-uuid'
              nonce: 'this-is-a-nonce'
              auth:
                uuid: 'cred-uuid'
                token: 'cred-token'
            }
            done()
          return # stupid mocha
