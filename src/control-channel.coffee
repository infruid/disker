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
    @pubClient = redis.createClient(options.redis)
    @subClient = redis.createClient(options.redis)

  publish: ({receiver, content}) ->
    return Promise.reject(new Error("Missing required argument 'receiver'")) unless receiver?
    new Promise (resolve, reject) =>
      @pubClient.publish receiver, JSON.stringify({@name, receiver, content}), (err, data) =>
        return reject(err) if err?
        resolve()

  subscribe: (handler) ->
    new Promise (resolve, reject) =>
      return reject(new Error("You have already subscribed to this channel")) if @subscribed
      return reject(new Error("Missing required argument 'handler'")) unless handler?

      @subClient.on "message", (@name, data) ->
        handler JSON.parse(data)
      @subClient.on "subscribe", (@name, count) =>
        @subscribed = true
        resolve()
      @subClient.subscribe @name, (err, data) ->
        reject(err) if err?

  unsubscribe: ->
    new Promise (resolve, reject) =>
      return resolve() unless @subscribed
      @subClient.on "unsubscribe", (channel, count) =>
        @subscribed = false
        resolve()
      @subClient.unsubscribe @name, (err) =>
        reject(err) if err?
  
  end: ->
    new Promise (resolve, reject) =>
      @unsubscribe()
      .then =>
        @subClient.quit()
        @pubClient.quit()
        @pubClient = @subClient = null
        resolve()
      .catch (err) ->
        reject(err)