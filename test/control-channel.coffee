{ControlChannel} = require "../src/index"
assert = require "assert"

options = {redis: {host: "127.0.0.1", port: 6379}}

describe "Control Channel Tests", ->

  it "sender can send and receiver can receive message", ->

    sender = new ControlChannel {name: "control-channel", options}
    receiver = new ControlChannel {name: "control-channel", options}

    new Promise (resolve, reject) ->
      receiver.subscribe (message) ->
        assert.equal message, "hello receiver"
        sender.end()
        .then ->
          receiver.end()
        .then ->
          resolve()
      .then ->
        sender.publish "hello receiver"
      .catch (err) ->
        reject err


  it "sender should not receive its own message", ->

    sender = new ControlChannel {name: "control-channel", options}

    new Promise (resolve, reject) ->
      sender.subscribe (message) ->
        reject "sender received its own message"
      .then ->
        sender.publish "hello receiver"
      .catch (err) ->
        if err?.code is 405
          sender.end()
          .then ->
            resolve()
        else
          reject err
