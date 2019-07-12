{randomKey} = require "key-forge"
redis = require "redis"
GenericPool = require "generic-pool"

defaultOptions = 
  redis:
    host: "127.0.0.1"
    port: 6379
  maxConnectionPoolSize: 10
  receiverBlockTimeout: 1
  timeoutMonitorFrequency: 1
  isSingleton: false

module.exports = class Disker

  constructor: (options) ->
    @_id = randomKey(5) # this is useful for debugging

    options = Object.assign(defaultOptions, options)

    @_ending = false
    @_isSingleton = options.isSingleton
    @_receiverBlockTimeout = options.receiverBlockTimeout
    @_timeoutMonitorFrequency = 1000 * options.timeoutMonitorFrequency
    @_timeoutMonitor = null
    @_messageHandlers = {}
    @_timeoutHandlers = {}

    poolFactory = 
      create: =>
        new Promise (resolve, reject) ->
          client = redis.createClient(options.redis)
          client.on "error", (err) -> 
            console.log "Disker: Error connecting to Redis #{err}"
            reject(err)
          client.on "ready", -> resolve(client)
      destroy: (client) => client.quit()
    @_clientPool = GenericPool.createPool(poolFactory, {min: 0, max: options.maxConnectionPoolSize})

    @_timeoutMonitor = setTimeout(@_monitorTimeouts, @_timeoutMonitorFrequency)

  @getSingleton: (options) ->
    options.isSingleton = true
    Disker._singleton ?= new Disker(options)

  _package: (message) ->
    message.fireAndForget ?= false
    Object.assign({id: randomKey(16)}, message)

  _getMessage: ({client, receiver, id}) ->
    new Promise (resolve, reject) =>
      client.hget(
        "#{receiver}.messages", id,
        (err, data) =>
          if err?
            reject(err)
          else
            resolve(if data? then JSON.parse(data) else null)
      )

  _putMessage: ({client, receiver, id, message}) ->
    new Promise (resolve, reject) =>
      client.hset(
        "#{receiver}.messages", id, JSON.stringify(message), 
        (err, data) =>
          if err?
            reject(err)
          else
            resolve(data)
      )

  _deleteMessage: ({client, receiver, id}) ->
    new Promise (resolve, reject) =>
      client.hdel(
        "#{receiver}.messages", id, 
        (err, data) =>
          if err?
            reject(err)
          else
            resolve(data)
      )

  _queueMessage: ({client, receiver, id}) ->
    new Promise (resolve, reject) ->
      client.lpush(
        "#{receiver}.queue", JSON.stringify(id), 
        (err, data) ->
          if err?
            reject(err)
          else
            resolve()
      )

  _setMessageTimeout: ({client, sender, receiver, id, timeout}) ->
    new Promise (resolve, reject) =>
      unless receiver? and id? and timeout?
        return resolve()

      @_getServerTime({client})
      .then (serverTime) =>
        client.zadd(
          ["#{sender}.pending", (serverTime + timeout), "#{receiver}::#{id}"], 
          (err, data) =>
            if err?
              reject(err)
            else
              resolve(data)
        )

  _clearMessageTimeout: ({client, sender, receiver, id}) ->
    new Promise (resolve, reject) =>
      unless id?
        return Promise.resolve()

      client.zrem(
        ["#{sender}.pending", "#{receiver}::#{id}"]
        (err, data) =>
          # remove the handler
          delete @_timeoutHandlers[sender]
          if err?
            reject(err)
          else
            resolve(data)
      )

  _getMessageTimeout: ({client, sender, receiver, id}) ->
    new Promise (resolve, reject) =>
      unless id?
        return Promise.resolve()

      client.zscore(
        ["#{sender}.pending", "#{receiver}::#{id}"]
        (err, data) =>
          if err?
            reject(err)
          else
            resolve(data)
      )

  _getServerTime: ({client}) ->
    new Promise (resolve, reject) =>
      client.time(
        (err, data) =>
          if err?
            reject(err)
          else
            resolve(data[0] * 1000)
      )

  _monitorTimeouts: =>
    return if @_ending
    client = null
    randomId = randomKey(5)
    @_clientPool.acquire()
    .then (_client) =>
      client = _client
      @_getServerTime({client})
    .then (serverTime) =>
      new Promise (resolve, reject) =>
        keys = Object.keys(@_timeoutHandlers)
        return resolve() if keys.length == 0

        batch = client.batch()
        for key in keys
          batch.zrangebyscore ["#{key}.pending", 0, serverTime]

        batch.exec (err, replies) =>
          return reject(err) if err?
          return resolve() if replies?.length == 0
          promises = []
          for reply, index in replies
            sender = keys[index]
            for expiredMessage in reply
              expiredMessageTokens = expiredMessage.split("::")
              promises.push(
                @_expireMessage {client, sender: keys[index], receiver: expiredMessageTokens[0], id: expiredMessageTokens[1]}
              )
          resolve(Promise.all(promises))
    .finally =>
      @_clientPool.release(client) if client?
      @_timeoutMonitor = setTimeout(@_monitorTimeouts, @_timeoutMonitorFrequency)

  _expireMessage: ({client, sender, receiver, id}) ->
    fireTimeout = false
    handler = @_timeoutHandlers[sender]
    @_getMessage({client, receiver, id})
    .then (message) =>
      @_clearMessageTimeout {client, sender, receiver, id}
      .then (timeoutsCleared) =>
        # if reply was sent in the meantime, timeout would have been cleared, we shouldn't fire timeout if it was replied
        fireTimeout = timeoutsCleared > 0
        if fireTimeout
          @_deleteMessage {client, receiver, id}
      .then =>
        if fireTimeout and message? and handler?
          setImmediate ->
            handler message
        return

  send: ({sender, receiver, content, fireAndForget, timeout}) ->
    return Promise.reject(new Error("Missing required argument 'sender'")) unless sender?
    return Promise.reject(new Error("Missing required argument 'receiver'")) unless receiver?
    return Promise.reject(new Error("You can use either of 'fireAndForget' and 'timeout', but not both")) if fireAndForget? and fireAndForget and timeout?

    message = @_package({sender, receiver, content, fireAndForget, timeout})

    client = null
    @_clientPool.acquire()
    .then (_client) =>
      client = _client
      @_putMessage {client, receiver, id: message.id, message}
      .then =>
        @_setMessageTimeout {client, sender, receiver, id: message.id, timeout: message.timeout}
      .then =>
        @_queueMessage {client, receiver, id: message.id}
    .finally =>
      @_clientPool.release(client) if client?

  reply: ({message, response, timeout}) ->
    return Promise.reject(new Error("Missing required argument 'message'")) unless message?
    return Promise.reject(new Error("Missing required field 'message.sender'")) unless message.sender?
    return Promise.reject(new Error("Missing required field 'message.receiver'")) unless message.receiver?
    return Promise.reject(new Error("Missing required field 'message.requestId'")) unless message.requestId?

    client = null
    @_clientPool.acquire()
    .then (_client) =>
      client = _client
      response = @_package {content: response, sender: message.receiver, receiver: message.sender, requestId: message.requestId, timeout}
      @_clearMessageTimeout {client, sender: message.sender, receiver: message.receiver, id: response.requestId}
    .then (timeoutsCleared) =>
      # its possible that this is a reply to a message that already timed out
      if !message.timeout? or timeoutsCleared > 0
        @_putMessage {client, receiver: response.receiver, id: response.id, message: response}
        .then =>
          if timeout?
            @_setMessageTimeout {client, sender: response.sender, receiver: response.receiver, id: response.id, timeout: response.timeout}
        .then =>
          @_queueMessage({client, receiver: response.receiver, id: response.id})
    .finally =>
      @_clientPool.release(client) if client?

  registerMessageHandler: ({receiver, oneTime, handler}) ->
    return Promise.reject(new Error("Missing required argument 'receiver'")) unless receiver?
    return Promise.reject(new Error("Missing required argument 'handler'")) unless handler?
    return Promise.reject(new Error("Handler was already registered for '#{receiver}'")) if @_messageHandlers[receiver]?

    @_messageHandlers[receiver] = {receiver, oneTime, handler}
    
    _listenForMessages = =>
      return if @_ending
      client = null
      @_clientPool.acquire()
      .then (_client) =>
        client = _client
        client.brpop "#{receiver}.queue", @_receiverBlockTimeout, (err, data) =>
          _finally = (continueToListen) =>
            @_clientPool.release(client) if client?
            setImmediate(_listenForMessages) if continueToListen
            return

          # do not continue to listen if end was called
          return _finally(false) if @_ending
          # do not notify if the handler was unregistered already
          return _finally(false) unless @_messageHandlers[receiver]?
          # we still do not have a message, continue listening
          return _finally(true) unless data?

          [key, json] = data
          messageId = JSON.parse(json)
          @_getMessage({client, receiver, id: messageId})
          .then (message) =>
            # its possible that this message has already timed out and no longer available in the store
            return null unless message?
            
            # this is the original message
            unless message.requestId?
              # if sender is not interested in a reply, we can delete the original message now that its received for processing
              if message.fireAndForget
                return @_deleteMessage(
                  {client, receiver: message.receiver, id: message.id}
                ).then ->
                  return message
              else
                return message

            # this is a reply message
            @_getMessage {client, receiver: message.sender, id: message.requestId}
            .then (request) =>
              if request?
                # now that we got the reply, we can delete the original request and corresponding timeout
                return @_deleteMessage {client, receiver: message.sender, id: message.requestId}
                .then =>
                  return @_clearMessageTimeout {client, sender: message.receiver, receiver: message.sender, id: message.requestId}
                .then =>
                  # now that we got the reply, we are done with the lifecycle of the message, delete the reply too
                  return @_deleteMessage {client, receiver, id: message.id}
                .then ->
                  return message
              else
                # request has timed out, we should delete the message as the sender won't receive this message
                @_deleteMessage {client, receiver, id: message.id}
                .then ->
                  return null
          .then (message) =>
            if message?
              _message = content: message.content
              _message.sender = if message.requestId? then message.receiver else message.sender
              _message.receiver = if message.requestId? then message.sender else message.receiver
              _message.requestId = if message.requestId? then message.requestId else message.id
              _message.responseId = message.id if message.requestId?
              _message.timeout = message.timeout if message.timeout?
              setImmediate ->
                handler(_message)
              # if this handler is registered for only one time, remove handler if we have a message
              delete @_messageHandlers[receiver] if oneTime? and oneTime
            return
          .finally =>
            _finally(@_messageHandlers[receiver]?)
      .catch (err) =>
        console.log "Disker: Unable to open connection to pop messages from Redis queue. #{err}"
    
    setImmediate _listenForMessages
    Promise.resolve()

  unregisterMessageHandler: ({receiver}) ->
    return Promise.reject(new Error("Missing required argument 'receiver'")) unless receiver?
    delete @_messageHandlers[receiver]
    Promise.resolve()

  registerTimeoutHandler: ({sender, handler}) ->
    return Promise.reject(new Error("Missing required argument 'sender'")) unless sender?
    return Promise.reject(new Error("Missing required argument 'handler'")) unless handler?
    @_timeoutHandlers[sender] = handler
    Promise.resolve()

  end: -> 
    @_ending = true
    clearTimeout @_timeoutMonitor
    @_clientPool.drain()
    .then => @_clientPool.clear()
    .then => Disker._singleton = null
