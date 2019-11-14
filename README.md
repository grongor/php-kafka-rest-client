kafka-rest-client [![Build Status](https://travis-ci.com/grongor/php-kafka-rest-client.svg?branch=master)](https://travis-ci.com/grongor/php-kafka-rest-client)
=================

This is a modern PHP client for [Confluent REST Proxy](https://docs.confluent.io/current/kafka-rest/) (version 2).

HTTP client used for communication with API adheres to the modern [HTTP standards](http://php-http.org) like
[PSR-7](https://www.php-fig.org/psr/psr-7/), [PSR-17](https://www.php-fig.org/psr/psr-17/)
and [PSR-18](https://www.php-fig.org/psr/psr-18/).

Aside from implementing the REST Proxy API this library adds some convenient classes useful for the most interactions
with Apache Kafka - see [examples](#Examples) for the whole list and a simple tutorial on how to use them.

Missing features
----------------

Some features were deliberately skipped to simplify the implementation/shorten the development time.
If you need anything and you're willing create a pull request, I'd be happy to check it out
and (if it makes sense) merge it. You can start the discussion by opening an issue.

- version 1 of the REST Proxy API
  - It's easy to upgrade the REST Proxy, so I don't see any point in implementing the first version.
- AVRO embedded format
- JSON embedded format
  - I think that the binary format is sufficient. You can always serialize/deserialize your objects before/after
  they interact with this library, therefore direct integration here is just a complication.
- async operations
  - To simplify the library all the methods are synchronous. That might definitely change if the need arises.
- all not-yet-implemented features mentioned [here](https://docs.confluent.io/current/kafka-rest/#features)
  - When these features are implemented, they will be added here ASAP.

Examples
--------

### Producer

Producer allows you to produce messages, either one by one or in batches. Both use the same underlying API method
so it's more efficient to use the batch one.

Both `produce` and `produceBatch` return nothing on success and throw an exception on failure.
There might be partial success/failure so the thrown exception `FailedToProduceMessages` contains two public properties,
`$retryable` and `$nonRetryable`, where each contains an array of
`['error' => 'error provided by Kafka', message => (Message object given to produce())]`.
Whether the error is [non-]retryable is based on the `error_code` as is
[documented](https://docs.confluent.io/current/kafka-rest/api.html#post--topics-(string-topic_name)).
It's up to you what you do with those.

```php
$producer = new Producer($restClient);
$producer->produce('your-topic', new Message('some-message'));

$messages = [new Message('some-message'), new Message('and-some-other-message')];
$producer->produceBatch('your-topic', $messages);
```

### Consumer

Consumer allows you to consume messages one by one until you return from the loop, application throws exception
or is otherwise forced to exit.
The `consume` method returns a [Generator](https://www.php.net/manual/en/class.generator.php) and loops indefinitely,
yielding messages as they are available. `consume` method accepts two parameters: `timeout` and `maxBytes`.
`timeout` is the maximum time the consumer will wait for the messages. `maxBytes` is the maximum size of the messages
to fetch in a single request. Both of these settings are complementary to the settings in `ConsumerOptions` and to the
server settings (check the Kafka documentation for more information).

If you set the consumer option `autoCommitEnable` to `false` then you may use consumer's `commit` method to commit
messages. Simply pass it a message you wish to commit. For most cases it's recommended to turn off the auto-commit
and manually commit each message, so that you don't ever "lose" a message if your application dies
in the middle of the processing.

You can also (optionally) set an idle callback using `setIdleCallback` method. This callback will be called whenever
there are no messages to yield. The idle interval is then equal to the `timeout` parameter if provided, or to the
`consumerRequestTimeoutMs` option if set, otherwise to the proxy configuration option `consumer.request.timeout.ms`.

```php
$consumerFactory = new ConsumerFactory($restClient);
$consumer = $consumerFactory->create('your-consumer-group', Subscription::topic('your-topic'));
foreach ($consumer->consume() as $message) {
    // Do your magic
    $logger->info('Got new message', $message->content);

    // ... and when you are done, commit the message (if you turned off auto-committing).
    $consumer->commit($message);
}
```

### BatchConsumer

BatchConsumer works the same way as Consumer does; the difference is that BatchConsumer doesn't yield each message
separately but first puts them in batches (`MessagesBatch`). These batches can be configured to be "limited" by either
count of messages in the batch (`maxCount`), by time (`maxDuration`) or by both (setting both `maxCount` and
`maxDuration`). If you set `maxDuration` then the batch won't ever take longer than that (+ few ms for processing) as
it changes the `timeout` parameter of the consumer (consumer won't get stuck on network waiting for more messages).

BatchConsumer can be quite useful for processing of "large" data sets, where you would have to otherwise batch
the messages yourself (eg. for inserting into database, where batch operations are always better).

As mentioned in the Consumer example, you may need to commit the messages. For that there is a `commit` method,
which accepts the yielded `MessagesBatch` and commits all the messages inside it in one request.

```php
$batchConsumerFactory = new BatchConsumerFactory($restClient);
$batchConsumer = $batchConsumerFactory->create(
    'your-consumer-group',
    Subscription::topic('your-topic'),
    $maxCount = 10000, // Yield the batch when there is 10 000 messages in it
    $maxDuration = 60  // or when 60 seconds passes, whichever comes first.
);
foreach ($batchConsumer->consume() as $messagesBatch) {
    // The batch might be empty if you specified the maxDuration.
    if ($messagesBatch->isEmpty()) {
        continue;
    }

    // Do your magic
    $database->insert($messagesBatch->getMessages());

    // ... and when you are done, commit the batch.
    $batchConsumer->commit($messagesBatch);
}
```
