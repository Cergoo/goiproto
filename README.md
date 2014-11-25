# goiproto

Asynchronous 'iproto' protocol implementation on Go.       

## Protocol

```
<request> | <response> := <header><body>
<header> = <type:uint32><body_length:uint32><request_id:uint32>
```
