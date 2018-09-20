{randomKey} = require "key-forge"
redis = require "redis"

defaultOptions = 
  redis:
    host: "127.0.0.1"
    port: 6379

module.exports = class ControlChannel

  @create: ({name, options}) ->
    new Promise (resolve, reject) ->
      controlChannel = new ControlChannel({name, options, callback: (err) ->
        return reject(err) if err?
        resolve(controlChannel)
      })

  constructor: ({@name, options, callback}) ->
    @_id = randomKey(5) # this is useful for debugging
    
    @subscribed = false
    @messageHandlers = {}
    options = Object.assign(defaultOptions, options)

    @pubClient = redis.createClient(options.redis)
    @pubClient.on "error", (err) -> 
      console.log "Disker: Error connecting to Redis #{err}"
      callback(err) if callback?
    @pubClient.on "ready", =>
      @subClient = redis.createClient(options.redis)
      @subClient.on "error", (err) -> 
        console.log "Disker: Error connecting to Redis #{err}"
        callback(err) if callback?
      @subClient.on "ready", =>
        @subClient.on "subscribe", (@name, count) =>
          @subscribed = true
          callback() if callback?
        @subClient.subscribe @name, (err, data) ->
          callback(err) if err? and callback?

  publish: ({receiver, content}) ->
    return Promise.reject(new Error("Missing required argument 'receiver'")) unless receiver?
    new Promise (resolve, reject) =>
      @pubClient.publish receiver, JSON.stringify({sender: @name, receiver, content}), (err, data) =>
        return reject(err) if err?
        resolve()

  registerMessageHandler: (handler) ->
    return reject(new Error("Missing required argument 'handler'")) unless handler?

    @messageHandlers[handler] = 1
    @subClient.on "message", (name, data) =>
      if @messageHandlers[handler]?
        handler JSON.parse(data)
    Promise.resolve()

  unregisterMessageHandler: (handler) ->
    delete @messageHandlers[handler]
    Promise.resolve()
  
  end: ->
    new Promise (resolve, reject) =>
      @subClient.on "unsubscribe", (channel, count) =>
        @subscribed = false
        @subClient.quit()
        @pubClient.quit()
        @pubClient = @subClient = null
        resolve()
      @subClient.unsubscribe @name, (err) =>
        reject(err) if err?