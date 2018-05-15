{randomKey} = require "key-forge"
redis = require "redis"

defaultOptions = 
  redis:
    host: "127.0.0.1"
    port: 6379

module.exports = class ControlChannel

  constructor: ({@name, options}) ->
    @_id = randomKey(5) # this is useful for debugging
    
    @subscribed = false
    options = Object.assign(defaultOptions, options)
    @client = redis.createClient(options.redis)

  publish: (message) ->
    new Promise (resolve, reject) =>
      if @subscribed
        err = new Error()
        err.code = 405
        err.message = "You can't publish on the same channel you have subscribed to."
        return reject(err)

      @client.publish @name, JSON.stringify(message), (err, data) =>
        return reject(err) if err?
        resolve()

  subscribe: (handler) ->
    new Promise (resolve, reject) =>
      return reject("You have already subscribed to this channel") if @subscribed
      return reject("Missing required argument 'handler'") unless handler?

      @client.on "message", (channel, data) ->
        handler JSON.parse(data)
      @client.on "subscribe", (channel, count) =>
        @subscribed = true
        resolve()
      @client.subscribe @name, (err, data) ->
        reject(err) if err?

  unsubscribe: ->
    new Promise (resolve, reject) =>
      return resolve() unless @subscribed
      @client.on "unsubscribe", (channel, count) =>
        @subscribed = false
        resolve()
      @client.unsubscribe @name, (err) =>
        reject(err) if err?
  
  end: ->
    new Promise (resolve, reject) =>
      @unsubscribe()
      .then =>
        @client.quit()
        @client = null
        resolve()
      .catch (err) ->
        reject(err)