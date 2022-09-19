consul-recipes
----

Set of high level recipes for working with Consul.

## Usage

```groovy
compile group: 'pl.allegro.tech.discovery', name: 'consul-recipes', version: '0.8.6'
```

## Recipes

You can access all available recipes through `ConsulRecipes` object:

```java
URI consulAgentUri = URI.create("http://localhost:8500");

ConsulRecipes recipes = ConsulRecipes.consulRecipes()
    .withAgentUri(consulAgentUri)
    .build();
```

Some recipes require an instance of `JsonSerializer` or `JsonDeserializer`,
which are simple interfaces for JSON deserialization. If you have Jackson on classpath,
you can create an instance of `JacksonJsonSerializer` or `JacksonJsonDeserializer` in the following way:

```java
ObjectMapper objectMapper = new ObjectMapper();
JsonDeserializer serializer = new JacksonJsonSerializer(objectMapper);
JsonDeserializer deserializer = new JacksonJsonDeserializer(objectMapper);

ConsulRecipes recipes = ConsulRecipes.consulRecipes()
    .withJsonSerializer(serializer)
    .withJsonDeserializer(deserializer)
    .build();
```

If a recipe requires serializer or deserializer and no instance is provided,
an exception will be thrown from the respective factory method.

### Watcher

`ConsulWatcher` enables you to listen for changes on specific path/endpoint. Every time a change is detected, provided
callback is run in specified thread pool. As with Consul watches, there are no guarantees that the content has
actually changed when change event is fired.

```java
ExecutorService workerPool = Executors.newFixedThreadPool(10);

ConsulWatcher watcher = consulRecipes.consulWatcher(workerPool).build();

Canceller callbackCanceller = watcher.watchEndpoint("/v1/catalog/services", (content) -> {
    // process String content
});

callbackCanceller.cancel();
// close stops watches on all watched endpoints
watcher.close();
// but you have to take care of cleaning up the worker pool
workerPool.shutdown();
```

#### Typed Watcher

`ConsulWatcher` returns `WatcherResult<String>` with raw JSON body. It's more convenient to have a typed POJO of this JSON.
The `EndpointWatcher<T>` class can be used to build a typed watcher on specific endpoint.
The required arguments are: an endpoint, `ConsulWatcher` instance and `JsonDecoder<T>` which is a JSON function
that converts raw JSON to POJO.

There are a few built-in `EndpointWatcher`s listed below.

##### Catalog Services Watcher

*Requires deserializer*.

`ServicesWatcher` is a watcher that converts a watcher result body to `Services` class

```java
public class Services {
    private final Map<String, List<String>> serviceNamesToTags;
    // getters & setters omitted
}
```

```java
ExecutorService workerPool = Executors.newFixedThreadPool(10);
EndpointWatcher<Services> servicesWatcher = consulRecipes.catalogServicesWatcher(
            consulRecipes
                .consulWatcher(workerPool)
                .build())

...

serviceWatcher.watch((WatchResult<Services> services) -> {
    // process services
})
```


##### Catalog Service Instances Watcher

*Requires deserializer*.

`ServiceInstancesWatcher` is a watcher that converts a watcher result body to `ServiceInstances` class.

```java
public class ServiceInstances {
    private final String serviceName;
    private final List<ServiceInstance> instances;
    // getters & setters omitted
}
public class ServiceInstance {
    private final String serviceId;
    private final List<String> serviceTags;
    private final String serviceAddress;
    private final int servicePort;
    // getters & setters omitted
}
```

```java
ExecutorService workerPool = Executors.newFixedThreadPool(10);
EndpointWatcher<ServiceInstances> serviceInstancesWatcher = consulRecipes.catalogServiceInstancesWatcher("my-services",
            consulRecipes
                .consulWatcher(workerPool)
                .build())

...

serviceInstancesWatcher.watch("my-service", (WatchResult<ServiceInstances> instances) -> {
    // proces instances for my-service
}));
```

##### Health Service Instances Watcher

Same as Catalog Service Instances Watcher, but watches only healthy service instances.

```java
ExecutorService workerPool = Executors.newFixedThreadPool(10);
EndpointWatcher<ServiceInstances> serviceInstancesWatcher = consulRecipes.healthServiceInstancesWatcher("my-services",
            consulRecipes
                .consulWatcher(workerPool)
                .build())

...

serviceInstancesWatcher.watch("my-service", (WatchResult<ServiceInstances> instances) -> {
    // proces instances for my-service
}));
```

### Datacenter reader

*Requires deserializer*.

Returns information about current datacenter and other known datacenters.

```java
ConsulDatacenterReader datacenterReader = consulRecipes.consulDatacenterReader().build();

String currentDc = datacenterReader.localDatacenter();

List<String> knownDcs = datacenterReader.knownDatacenters();
```

### Agent locator

*Requires deserializer*.

Returns addresses of agents that registered given service from all known DCs.
Useful if you want to create watches in different DCs.

```java
ConsulAgentLocator agentLocator = consulRecipes.consulAgentLocator().build();

Map<String, URI> agents = agentLocator.locateDatacenterAgents("my-service");
```

### Session

*Incubating feature*.

*Requires serializer and deserializer*.

Creates and manages Consul Session.

```java
Session session = consulRecipes.session("myservice")
        .build();

session.start();

String id = session.currentId();

// don't forget to close it when you close your application
session.close();
```

### Leader election

*Incubating feature*.

*Requires serializer and deserializer*.

Allows electing a leader using Consul Session.

```java
LeaderElector elector = consulRecipes
        .leaderElector("myservice")
        .build();

elector.start();

elector.registerObserver(new LeadershipObserver() {
    @Override
    public void leadershipAcquired() {
        logger.info("Leadership acquired");
    }

    @Override
    public void leadershipLost() {
        logger.info("Leadership lost");
    }
});

if (elector.isLeader()) {
    // do something on leader instance
}

// don't forget to close it when you close your application
elector.close();
```
