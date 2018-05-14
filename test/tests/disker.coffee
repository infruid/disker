Disker = require "../../src/disker"
{randomKey} = require "key-forge"
assert = require "assert"

options = {redis: {host: "127.0.0.1", port: 6379}, maxConnectionPoolSize: 2000}

describe "Disker Tests", ->

  dispatcher = Disker.getSingleton(options)
  worker = Disker.getSingleton(options)

  describe "send and receive messages", ->

    it "send message", ->
      dispatcher.send {sender: "dispatcher-1", receiver: "worker-1", content: "message"}

    it "receive message", ->
      new Promise (resolve, reject) ->
        worker.registerMessageHandler receiver: "worker-1", oneTime: true, handler: (message) ->
          assert.equal message.content, "message"
          worker.reply {sender: "worker-1", receiver: "dispatcher-1", message, response: "reply"}
          .then -> resolve()
          .catch (err) -> reject(err)

    it "receive reply", ->
      new Promise (resolve, reject) ->
        dispatcher.registerMessageHandler receiver: "dispatcher-1", oneTime: true, handler: (reply) ->
          assert.equal reply.content, "reply"
          resolve()


  describe "set timeout on message", ->

    it "send message", ->
      dispatcher.send {sender: "dispatcher-2", receiver: "worker-2", content: "message", timeout: 1000}

    it "receive message", ->
      new Promise (resolve, reject) ->
        worker.registerMessageHandler receiver: "worker-2", oneTime: true, handler: (message) ->
          assert.equal message.content, "message"
          resolve()

    it "wait for timeout", ->
      new Promise (resolve, reject) ->
        dispatcher.registerTimeoutHandler sender: "dispatcher-2", oneTime: true, handler: (message) ->
          assert.equal message.content, "message"
          resolve()


  describe "reply to a timed out message", ->

    it "send message", ->
      dispatcher.send {sender: "dispatcher-3", receiver: "worker-3", content: "message", timeout: 3000}

    it "receive message", ->
      new Promise (resolve, reject) ->
        worker.registerMessageHandler receiver: "worker-3", oneTime: true, handler: (receivedMessage) ->
          assert.equal receivedMessage.content, "message"
          dispatcher.registerTimeoutHandler sender: "dispatcher-3", oneTime: true, handler: (timedoutMessage) ->
            assert.equal timedoutMessage.content, "message"
            worker.reply {sender: "worker-3", receiver: "dispatcher-3", message: receivedMessage, response: "reply"}
            .then -> resolve()


  describe "receive a timed out message", ->

    it "send message", ->
      dispatcher.send {sender: "dispatcher-4", receiver: "worker-4", content: "message", timeout: 100}

    it "receive message", ->
      new Promise (resolve, reject) ->
        dispatcher.registerTimeoutHandler sender: "dispatcher-4", oneTime: true, handler: (message) ->
          assert.equal(message.content, "message")
          worker.registerMessageHandler receiver: "worker-4", oneTime: true, handler: (message) ->
            reject("worker received a timed out message")
          setTimeout resolve, 1000


  describe "send and receive many messages in quick succession", ->
    messageCount = 1000
    it "send and receive #{messageCount} messages", ->

      worker.registerMessageHandler receiver: "worker-5", handler: (message) ->
        worker.reply {sender: "worker-5", receiver: "dispatcher-5-#{message.content}", message, response: "reply-#{message.content}"}

      promises = []
      [1..messageCount].map (i) ->
        promise = new Promise (resolve, reject) ->
          dispatcher.registerTimeoutHandler sender: "dispatcher-5-#{i}", oneTime: true, handler: (message) ->
            resolve()
          dispatcher.registerMessageHandler receiver: "dispatcher-5-#{i}", oneTime: true, handler: (message) ->
            resolve()
          dispatcher.send {sender: "dispatcher-5-#{i}", receiver: "worker-5", content: i, timeout: 10000}
        promises.push(promise)
      Promise.all(promises)

  describe "end dispatcher and worker", ->
    it "end dispatcher and worker", ->
      new Promise (resolve, reject) ->
        dispatcher.end()
        .then -> worker.end()
        .then -> resolve()
        .catch (err) -> reject(err)