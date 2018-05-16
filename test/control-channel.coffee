{ControlChannel} = require "../src/index"
assert = require "assert"

options = {redis: {host: "127.0.0.1", port: 6379}}

describe "Control Channel Tests", ->

  it "sender can publish and receiver can subscribe", ->

    sender = new ControlChannel {name: "control-channel-sender", options}
    receiver = new ControlChannel {name: "control-channel-receiver", options}

    new Promise (resolve, reject) ->
      sender.subscribe (message) ->
        assert.equal message.content, "hello sender"
        sender.end()
        .then ->
          receiver.end()
        .then ->
          resolve()
      receiver.subscribe (message) ->
        assert.equal message.content, "hello receiver"
        receiver.publish {receiver: "control-channel-sender", content: "hello sender"}
      .then ->
        sender.publish {receiver: "control-channel-receiver", content: "hello receiver"}
      .catch (err) ->
        reject err
