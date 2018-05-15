# Disker
Disker is an implementation of dispatcher-worker pattern for distributed processing of tasks using Redis. For example, Disker can be used for distributed processing of HTTP requests. Disker is inspired by [mutual](https://github.com/pandastrike/mutual).

# Usage

### Create a dispatcher and worker

Either we can use a singleton or create a new instance. Its optimal to use single Disker instance per process. 

Create a dispatcher:
```javascript
  const options = {"redis": {"host": "127.0.0.1", "port": 6379}, "maxConnectionPoolSize": 100};
  const dispatcher = Disker.getSingleton(options);
  // or if we need to create a new instance
  // const dispatcher = new Disker(options);
```

Similarly, create a worker:
```javascript
  const options = { "redis": {"host": "127.0.0.1", "port": 6379}, "maxConnectionPoolSize": 100 };
  const worker = Disker.getSingleton(options);
  // or if we need to create a new instance
  // const worker = new Disker(options);
```
> Note: You can run worker code on one or more remote machines for distributed processing of tasks.

### Send messages

```javascript
  dispatcher.send(
    { "sender": "my-dispatcher", "receiver": "my-worker", "content": "hello worker" }
  ).then(function() {
    console.log("Successfully sent message");
  }).catch(function(err) {
    console.log("Failed to send message: ${err}");
  });
```

### Receive messages

```javascript
  // Register a message handler to receive messages
  worker.registerMessageHandler({ 
    "receiver": "my-worker", 
    "handler": function(message) {
      // We have received a message, lets reply to the message
      worker.reply(
        { "sender": "my-worker", "receiver": "my-dispatcher", "message": message, "response": "hello dispatcher" }
      ).then(function() { 
        console.log("Successfully replied to message"); 
      }).catch(function(err) {
        console.log("Failed to reply to message");
      });
    }
  }).then(function() {
    console.log("Successfully registered message handler");
  }).catch(function(err) {
    console.log("Failed to register message handler: ${err}");
  });
```

> Note: When replying to a message, 'sender' would be worker's name and 'receiver' would be dispatcher's name. We should send the original message and response.

### Receive timeout notifications

```javascript
  // Send a message with a timeout. Timeout is specified in milliseconds
  dispatcher.send(
    { "sender": "my-dispatcher", "receiver": "my-worker", "content": "hello worker", "timeout": 1000 }
  ).then(function() {
    console.log("Successfully sent message");
  }).catch(function(err) {
    console.log("Failed to send message: ${err}");
  });

  // Register a timeout handler to receive timeouts
  dispatcher.registerTimeoutHandler({
    "sender": "my-dispatcher", 
    "handler": function(message) {
      console.log("Successfully received a timeout");
    }
  }).then(function() {
    console.log("Successfully registered timeout handler");
  }).catch(function(err) {
    console.log("Failed to register timeout handler: ${err}");
  });
```

> Note: To receive timeout, 'sender' would be dispatcher's name itself to indicate whose message timeouts we want to receive notifications for.

### Finally, end the dispatcher and worker to close connections

End the dispatcher
```javascript
  dispatcher.end();
```

End the worker
```javascript
  worker.end();
```

### Supported Options

| Property                  | Default   | Description |
|---------------------------|-----------|-------------|
| redis.host                | 127.0.0.1 | IP address of the Redis server. |
| redis.port                | 6379      | Port of the Redis server. |
| maxConnectionPoolSize     | 10        | The maximum number of connections to keep in pool. |
| receiverBlockTimeout      | 1         | Time in seconds to wait for a message. Set this to `0` to wait indefinitely. |
| timeoutMonitorFrequency   | 1         | How frequently should Disker monitor for timeouts. This value is in seconds. |
