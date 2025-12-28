# Building My Own Redis

## What will I be building currently?

- Binding to a TCP port and listening for connections
- Responding to basic commands like `PING` and `ECHO`
- Parsing the Redis Protocol (RESP) from client requests
- Handling multiple clients concurrently
- Implementing the `SET` and `GET` commands to store and retrieve data.

Note: This is a challenge from [codecrafters.io](https://app.codecrafters.io/courses/redis/overview)

## Things to be done

- [x] Allow handling concurrent clients
- [x] Implement a codec for framing
- [x] Implement the data flow:  
       read bytes -> parse frames -> map to commands -> run command -> response frames -> write bytes
- [x] Implement `ECHO` and `PING` commands
- [ ] Implement `SET` and `GET` commands
- [ ] Implement Expiry
- [ ] Add structured error handling
- [ ] Add extensive unit tests
