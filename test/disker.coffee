{Disker} = require "../src/index"
{randomKey} = require "key-forge"
assert = require "assert"

options = redis: {host: "127.0.0.1", port: 6379}, maxConnectionPoolSize: 2000, receiverBlockTimeout: 10, timeoutMonitorFrequency: 10

describe "Disker Tests", ->

  dispatcher = Disker.getSingleton(options)
  worker = Disker.getSingleton(options)

  describe "can send and receive messages", ->

    it "can send message", ->
      dispatcher.send {sender: "dispatcher-1", receiver: "worker-1", content: "hello worker-1"}

    it "can receive message", ->
      new Promise (resolve, reject) ->
        worker.registerMessageHandler receiver: "worker-1", oneTime: true, handler: (message) ->
          assert.equal message.content, "hello worker-1"
          worker.reply {message, response: "reply"}
          .then -> resolve()
          .catch (err) -> reject(err)

    it "can receive reply", ->
      new Promise (resolve, reject) ->
        dispatcher.registerMessageHandler receiver: "dispatcher-1", oneTime: true, handler: (reply) ->
          assert.equal reply.content, "reply"
          resolve()


  describe "can set timeout on message", ->

    it "can send message", ->
      dispatcher.send {sender: "dispatcher-2", receiver: "worker-2", content: "hello worker-2", fireAndForget: true}

    it "can receive message", ->
      new Promise (resolve, reject) ->
        worker.registerMessageHandler receiver: "worker-2", oneTime: true, handler: (message) ->
          assert.equal message.content, "hello worker-2"
          resolve()

  describe "can receive a timed out message", ->

    it "can send message", ->
      dispatcher.send {sender: "dispatcher-3", receiver: "worker-3", content: "hello worker-3", timeout: 100}

    it "can receive timeout", ->
      new Promise (resolve, reject) ->
        dispatcher.registerTimeoutHandler sender: "dispatcher-3", oneTime: true, handler: (message) ->
          assert.equal(message.content, "hello worker-3")
          worker.registerMessageHandler receiver: "worker-3", oneTime: true, handler: (message) ->
            reject("worker received a timed out message")
          setTimeout resolve, 1000

  describe "can reply to a timed out message", ->

    it "can send message", ->
      dispatcher.send {sender: "dispatcher-4", receiver: "worker-4", content: "hello worker-4", timeout: 3000}

    it "can reply to a timed out message", ->
      new Promise (resolve, reject) ->
        worker.registerMessageHandler receiver: "worker-4", oneTime: true, handler: (receivedMessage) ->
          assert.equal receivedMessage.content, "hello worker-4"
          dispatcher.registerTimeoutHandler sender: "dispatcher-4", oneTime: true, handler: (timedoutMessage) ->
            assert.equal timedoutMessage.content, "hello worker-4"
            worker.reply {message: receivedMessage, response: "reply"}
            .then -> resolve()


  describe "can send and receive many messages in quick succession", ->
    messageCount = 1000
    it "can send and receive #{messageCount} messages", ->

      worker.registerMessageHandler receiver: "worker-5", handler: (message) ->
        worker.reply {message, response: "reply-#{message.content}"}

      promises = [1..messageCount].map (i) ->
        promise = new Promise (resolve, reject) ->
          dispatcher.registerTimeoutHandler({sender: "dispatcher-5-#{i}", oneTime: true, handler: (message) ->
            resolve()
          }).then ->
            dispatcher.registerMessageHandler receiver: "dispatcher-5-#{i}", oneTime: true, handler: (message) ->
              resolve()
          .then ->
            dispatcher.send {sender: "dispatcher-5-#{i}", receiver: "worker-5", content: i, timeout: 10000}
      Promise.all(promises)

  describe "can end dispatcher and worker", ->
    it "can end dispatcher and worker", ->
      new Promise (resolve, reject) ->
        dispatcher.end()
        .then -> worker.end()
        .then -> resolve()
        .catch (err) -> reject(err)