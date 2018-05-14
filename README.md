# Disker
Disker is an implementation of dispatcher worker pattenr in Node.js. Disker is built for http request response lifecylye.

# Usage

### Create a dispatcher and worker

Either we can use a singleton or create a new instance. Its optimal to use single Disker instance per process. 

Create a dispatcher:
```coffeescript
  options = {host: "127.0.0.1", port: 6379, poolSize: 100}
  dispatcher = Disker.getSingleton(options)
  # or if we need to create a new instance
  # dispatcher = new Disker(options)
```

Similarly, create a worker:
```coffeescript
  options = {host: "127.0.0.1", port: 6379, poolSize: 100}
  worker = Disker.getSingleton(options)
  # or if we need to create a new instance
  # worker = new Disker(options)
```
> Note: You can run worker code on one or more remote machines.

### Send messages

```coffeescript
  dispatcher.send {sender: "my-dispatcher", receiver: "my-worker", content: "hello worker"}
  .then ->
    console.log "Successfully sent message"
  .catch (er) ->
    console.log "Failed to send message", err
```

### Receive messages

```coffeescript
  # Register a message handler to receive messages
  worker.registerMessageHandler receiver: "my-worker", handler: (message) ->
    # We have received a message, lets reply to the message
    worker.reply {sender: "my-worker", receiver: "my-dispatcher", message, response: "hello dispatcher"}
      .then -> console.log "Successfully replied to message"
      .catch (err) -> console.log "Failed to reply to message"
  .then ->
    console.log "Successfully registered message handler"
  .catch (err) ->
    console.log "Failed to register message handler", err
```

> Note: When replying to a message, 'sender' would be worker's name and 'receiver' would be dispatcher's name. We should send the original message and response.

### Receive timeout notifications

```coffeescript
  # Register a timeout handler to receive timeouts
  dispatcher.registerTimeoutHandler sender: "my-dispatcher", handler: (message) ->
    # We have received a timeout
  .then ->
    console.log "Successfully registered timeout handler"
  .catch (err) ->
    console.log "Failed to register timeout handler", err
```

> Note: To receive timeout, 'sender' would be dispatcher's name itself to indicate whose message timeouts we want to receive notifications for.

### Finally, end the dispatcher and worker to close connections

End the dispatcher
```coffeescript
  dispatcher.end()
```

End the worker
```coffeescript
  worker.end()
```

### Supported Options

| Property                  | Default   | Description |
|---------------------------|-----------|-------------|
| host                      | 127.0.0.1 | IP address of the Redis server |
| port                      | 6379      | Port of the Redis server |
| maxPoolSize               | 10        | The maxiimum number of connections to keep in pool |
| messageBlockTimeout       | 1         | Time in seconds to wait for a message. Set this to `0` to wait indefinitely |
| timeoutMonitorFrequency   | 1         | Frequency in seconds for monitoring for timeouts |
