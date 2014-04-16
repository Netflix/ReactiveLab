ReactiveLabs
============

The ReactiveLabs provides two sets of Servers. The Basic ones provide a first overview of reactive design using Rx. The "NetFlix" ones provides a deeper implementation which looks more like what may be done a [NetFlix](http://netflix.github.io/ "NefFlix OSS"), integrating Hystrix for example.

### Basic Edge Server (to be run with Basic Middle Tier servers) ###

example of call: 
`curl "http://localhost:8080?id=42342"`


### NetFlix Edge Server (to be run with Basic Middle NetFlix servers) ###


example of call; 
`curl "http://localhost:8080?userId=42342"`

The Netflix Edge Server may be monitored using Hystrix:
```bash
user@host$ git clone git@github.com:Netflix/Hystrix.git
user@host$ cd Hystrix
user@host$ ./gradlew jettyRun
```
You may then launch hystrix in you browser at `http://localhost:7979/hystrix-dashboard` and monitor `http://localhost:8080/hystrix.stream `in the Hystrix welcome page.
