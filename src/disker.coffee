{randomKey} = require "key-forge"
redis = require "redis"
GenericPool = require "generic-pool"

module.exports = class Disker

  constructor: (options) ->
    {port, host, poolSize} = options

    poolSize ?= 10
    @_id = randomKey(5)
    @_ending = false
    @_isSingleton = if options.isSingleton? then options.isSingleton? else false
    @_blockTimeout = if options.blockTimeout? then options.blockTimeout else 1
    @_timeoutMonitorFrequency = if options.timeoutMonitorFrequency? then options.timeoutMonitorFrequency else 1000
    @_timeoutMonitor = null
    @_messageHandlers = {}
    @_timeoutHandlers = {}

    poolFactory = 
      create: =>
        new Promise (resolve, reject) ->
          client = redis.createClient port, host, options.redis
          client.on "error", (error) -> reject(error)
          client.on "connect", -> resolve(client)
      destroy: (client) => client.quit()
    @_clientPool = GenericPool.createPool(poolFactory, {min: 0, max: poolSize})

    @_monitorTimeouts()

  @getSingleton: (options) ->
    options.isSingleton = true
    Disker._singleton ?= new Disker(options)

  _package: ({sender, receiver, content, requestId, timeout}) ->
    message = 
      id: randomKey(16)
      requestId: requestId
      sender: sender
      receiver: receiver
      timeout: timeout
      content: content

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
        resolve()

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

  _monitorTimeouts: ->
    loopToMonitor = =>
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
              for expiredMessage in reply
                expiredMessageTokens = expiredMessage.split("::")
                promises.push(
                  @_expireMessage {client, sender: keys[index], receiver: expiredMessageTokens[0], id: expiredMessageTokens[1]}
                )
            resolve(Promise.all(promises))
      .finally =>
        if !@_ending
          @_timeoutMonitor = setTimeout(loopToMonitor, @_timeoutMonitorFrequency)
        @_clientPool.release(client) if client?

    @_timeoutMonitor = setTimeout(loopToMonitor, @_timeoutMonitorFrequency)

  _expireMessage: ({client, sender, receiver, id}) ->
    @_getMessage({client, receiver, id})
    .then (message) =>
      @_getMessageTimeout({client, sender, receiver, id})
      .then (timeout) =>
        # if reply was sent in the meantime, timeout would have been cleared, we shouldn't fire timeout if it was replied
        return unless timeout?
        if message?
          @_deleteMessage {client, receiver, id}
          .then =>
            @_clearMessageTimeout {client, sender, receiver, id}
          .then =>
            if message?
              handler = @_timeoutHandlers[sender]
              if handler?
                handler {content: message.content, id: message.id, requestId: message.requestId}
            return

  send: ({sender, receiver, content, timeout}) ->
    return Promise.reject("Missing required argument 'sender'") unless sender?
    return Promise.reject("Missing required argument 'receiver'") unless receiver?

    message = @_package({sender, receiver, content, timeout})

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

  reply: ({sender, receiver, message, response, timeout}) ->
    return Promise.reject("Missing required argument 'sender'") unless sender?
    return Promise.reject("Missing required argument 'receiver'") unless receiver?
    return Promise.reject("Missing required argument 'message'") unless message?
    return Promise.reject("Missing required field 'message.requestId'") unless message.requestId?

    client = null
    @_clientPool.acquire()
    .then (_client) =>
      client = _client
      @_getMessage {client, receiver: sender, id: message.requestId}
      .then (request) =>
        # its possible that this is a reply to a message that already timed out
        return null unless request?
        
        response = @_package {content: response, sender, receiver, requestId: message.requestId, timeout}
        @_clearMessageTimeout {client, sender: receiver, receiver: sender, id: response.requestId}
        .then =>
          @_putMessage {client, receiver, id: response.id, message: response}
        .then =>
          @_queueMessage({client, receiver, id: response.id})
    .finally =>
      @_clientPool.release(client) if client?

  registerMessageHandler: ({receiver, handler}) ->
    return Promise.reject("Missing required argument 'receiver'") unless receiver?
    return Promise.reject("Missing required argument 'handler'") unless handler?
    return Promise.reject("Handler was already registered for '#{receiver}'") if @_messageHandlers[receiver]?

    @_messageHandlers[receiver] = handler
    
    do listen = =>
      return if @_ending
      client = null
      @_clientPool.acquire()
      .then (_client) =>
        client = _client
        client.brpop "#{receiver}.queue", @_blockTimeout, (err, data) =>
          # do not notify if the handler was unregistered already
          return if @_ending or !@_messageHandlers[receiver]?
          #we still do not have a message, continue listening
          return setImmediate(listen) unless data?

          [key, json] = data
          messageId = JSON.parse(json)
          @_clientPool.acquire()
          .then (_client) =>
            client = _client
            @_getMessage({client, receiver, id: messageId})
          .then (message) =>
            # its possible that this message has already timed out and no longer available in the store
            return null unless message?
            return message unless message.requestId?

            @_getMessage {client, receiver: message.sender, id: message.requestId}
            .then (request) =>
              if request?
                # now that we got the reply, we can delete the original request also
                @_deleteMessage {client, receiver: message.sender, id: message.requestId}
                .then =>
                  @_deleteMessage {client, receiver, id: message.id}
                .then ->
                  return message
              else
                # request has timed out, we should delete the message as the sender won't receive this message
                @_deleteMessage {client, receiver, id: message.id}
                .then ->
                  return null
          .then (message) ->
            if message?
              _message = content: message.content
              _message.sender = if message.requestId? then message.receiver else message.sender
              _message.receiver = if message.requestId? then message.sender else message.receiver
              _message.requestId = if message.requestId? then message.requestId else message.id
              _message.responseId = message.id if message.requestId?
              handler(_message)
            return
          .finally =>
            @_clientPool.release(client) if client?
            setImmediate(listen)
      .finally =>
        @_clientPool.release(client) if client?

  unregisterMessageHandler: ({receiver}) ->
    return Promise.reject("Missing required argument 'receiver'") unless receiver?
    delete @_messageHandlers[receiver]

  registerTimeoutHandler: ({sender, handler}) ->
    return Promise.reject("Missing required argument 'sender'") unless sender?
    return Promise.reject("Missing required argument 'handler'") unless handler?
    @_timeoutHandlers[sender] = handler

  unregisterTimeoutHandler: ({sender}) ->
    return Promise.reject("Missing required argument 'sender'") unless sender?
    delete @_timeoutHandlers[sender]

  end: -> 
    @_ending = true
    clearTimeout @_timeoutMonitor
    @_clientPool.drain()
    .then => @_clientPool.clear()
    .then => Disker._singleton = null
