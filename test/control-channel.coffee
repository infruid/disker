{ControlChannel} = require "../src/index"
assert = require "assert"

options = {redis: {host: "127.0.0.1", port: 6379}}

describe "Control Channel Tests", ->

  it "sender can publish and receiver can subscribe", ->
    
    sender = null
    ControlChannel.create({name: "control-channel-sender", options})
    .then (_sender) ->
      sender = _sender
      ControlChannel.create({name: "control-channel-receiver", options})
    .then (receiver) ->
      new Promise (resolve, reject) ->
        sender.registerMessageHandler (message) ->
          sender.unregisterMessageHandler(this)
          .then ->
            assert.equal message.content, "hello sender"
            sender.end()
            .then ->
              receiver.end()
            .then ->
              resolve()
        receiver.registerMessageHandler (message) ->
          receiver.unregisterMessageHandler(this)
          .then ->
            assert.equal message.content, "hello receiver"
            receiver.publish {receiver: "control-channel-sender", content: "hello sender"}
        .then ->
          sender.publish {receiver: "control-channel-receiver", content: "hello receiver"}
        .catch (err) ->
          reject err
