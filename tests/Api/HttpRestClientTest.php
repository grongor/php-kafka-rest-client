<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Tests\Api;

use Grongor\KafkaRest\Api\Exception\UnexpectedApiResponse;
use Grongor\KafkaRest\Api\HttpRestClient;
use Grongor\KafkaRest\Api\Value\Request\ConsumerOptions;
use Grongor\KafkaRest\Api\Value\Request\Message;
use Grongor\KafkaRest\Api\Value\Request\Offset;
use Grongor\KafkaRest\Api\Value\Request\Partition;
use Grongor\KafkaRest\Api\Value\Request\Subscription;
use Grongor\KafkaRest\Api\Value\Response\Consumer;
use Http\Factory\Guzzle\RequestFactory;
use Http\Factory\Guzzle\ResponseFactory;
use Http\Factory\Guzzle\StreamFactory;
use Http\Factory\Guzzle\UriFactory;
use JMS\Serializer\SerializerBuilder;
use Mockery;
use Mockery\MockInterface;
use PHPUnit\Framework\Constraint\IsTrue;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\NullLogger;
use Teapot\StatusCode\Http;
use function Safe\file_get_contents;
use function Safe\sprintf;

final class HttpRestClientTest extends TestCase
{
    /** @var HttpRestClient */
    private $client;

    /** @var MockInterface|ClientInterface */
    private $httpClient;

    /** @var ResponseInterface */
    private $response;

    public function setUp() : void
    {
        $this->httpClient = Mockery::mock(ClientInterface::class);
        $this->client = new HttpRestClient(
            $this->httpClient,
            new NullLogger(),
            new RequestFactory(),
            SerializerBuilder::create()->build(),
            new StreamFactory(),
            new UriFactory(),
            'http://api'
        );
        $this->response = (new ResponseFactory())->createResponse();
    }

    public function testListTopics() : void
    {
        $this->expectRequest('GET', '/topics', null, $this->responseWithBody('["lorem-ipsum", "dolor-sit-amet"]'));

        $topics = $this->client->listTopics();

        self::assertSame(['lorem-ipsum', 'dolor-sit-amet'], $topics);
    }

    public function testGetTopic() : void
    {
        $this->expectRequest('GET', '/topics/lorem-ipsum', null, $this->fileResponse('getTopic'));

        $topic = $this->client->getTopic('lorem-ipsum');

        self::assertSame('lorem-ipsum', $topic->name);
        self::assertSame(['retention.bytes' => '-1', 'flush.ms' => '50000'], $topic->configs);
        self::assertCount(2, $topic->partitions);

        // partition 0
        self::assertSame(0, $topic->partitions[0]->partition);
        self::assertSame(0, $topic->partitions[0]->leader);
        self::assertCount(2, $topic->partitions[0]->replicas);

        // partition 0, replica 0
        self::assertSame(0, $topic->partitions[0]->replicas[0]->broker);
        self::assertTrue($topic->partitions[0]->replicas[0]->leader);
        self::assertTrue($topic->partitions[0]->replicas[0]->inSync);

        // partition 0, replica 1
        self::assertSame(1, $topic->partitions[0]->replicas[1]->broker);
        self::assertFalse($topic->partitions[0]->replicas[1]->leader);
        self::assertTrue($topic->partitions[0]->replicas[1]->inSync);

        // partition 1
        self::assertSame(1, $topic->partitions[1]->partition);
        self::assertSame(1, $topic->partitions[1]->leader);
        self::assertCount(2, $topic->partitions[1]->replicas);

        // partition 1, replica 0
        self::assertSame(1, $topic->partitions[1]->replicas[0]->broker);
        self::assertTrue($topic->partitions[1]->replicas[0]->leader);
        self::assertTrue($topic->partitions[1]->replicas[0]->inSync);

        // partition 1, replica 1
        self::assertSame(0, $topic->partitions[1]->replicas[1]->broker);
        self::assertFalse($topic->partitions[1]->replicas[1]->leader);
        self::assertTrue($topic->partitions[1]->replicas[1]->inSync);
    }

    public function testProduce() : void
    {
        $this->expectRequest(
            'POST',
            '/topics/lorem-ipsum',
            '{"records":[{"value":"bXNnMQ=="},{"key":"c29tZSBrZXk=","value":"bXNnMg=="},{"value":"bXNnMw=="}]}',
            $this->fileResponse('produce')
        );

        $results = $this->client->produce(
            'lorem-ipsum',
            [new Message('msg1'), new Message('msg2', 'some key'), new Message('msg3')]
        );

        self::assertCount(3, $results);

        self::assertFalse($results[0]->isError());
        $offset = $results[0]->getOffset();
        self::assertSame(0, $offset->partition);
        self::assertSame(8, $offset->offset);

        self::assertFalse($results[1]->isError());
        $offset = $results[1]->getOffset();
        self::assertSame(1, $offset->partition);
        self::assertSame(15, $offset->offset);

        self::assertTrue($results[2]->isError());
        $error = $results[2]->getError();
        self::assertSame(1, $error->errorCode);
        self::assertSame('some unexpected error', $error->error);
    }

    public function testListPartitions() : void
    {
        $this->expectRequest('GET', '/topics/lorem-ipsum/partitions', null, $this->fileResponse('listPartitions'));

        $partitions = $this->client->listPartitions('lorem-ipsum');

        self::assertCount(2, $partitions);

        // partition 0
        self::assertSame(0, $partitions[0]->partition);
        self::assertSame(0, $partitions[0]->leader);
        self::assertCount(2, $partitions[0]->replicas);

        // partition 0, replica 0
        self::assertSame(0, $partitions[0]->replicas[0]->broker);
        self::assertTrue($partitions[0]->replicas[0]->leader);
        self::assertTrue($partitions[0]->replicas[0]->inSync);

        // partition 0, replica 1
        self::assertSame(1, $partitions[0]->replicas[1]->broker);
        self::assertFalse($partitions[0]->replicas[1]->leader);
        self::assertTrue($partitions[0]->replicas[1]->inSync);

        // partition 1
        self::assertSame(1, $partitions[1]->partition);
        self::assertSame(1, $partitions[1]->leader);
        self::assertCount(2, $partitions[1]->replicas);

        // partition 1, replica 0
        self::assertSame(1, $partitions[1]->replicas[0]->broker);
        self::assertTrue($partitions[1]->replicas[0]->leader);
        self::assertTrue($partitions[1]->replicas[0]->inSync);

        // partition 1, replica 1
        self::assertSame(0, $partitions[1]->replicas[1]->broker);
        self::assertFalse($partitions[1]->replicas[1]->leader);
        self::assertTrue($partitions[1]->replicas[1]->inSync);
    }

    public function testGetPartition() : void
    {
        $this->expectRequest('GET', '/topics/lorem-ipsum/partitions/0', null, $this->fileResponse('getPartition'));

        $partition = $this->client->getPartition('lorem-ipsum', 0);

        // partition 0
        self::assertSame(0, $partition->partition);
        self::assertSame(0, $partition->leader);
        self::assertCount(2, $partition->replicas);

        // partition 0, replica 0
        self::assertSame(0, $partition->replicas[0]->broker);
        self::assertTrue($partition->replicas[0]->leader);
        self::assertTrue($partition->replicas[0]->inSync);

        // partition 0, replica 1
        self::assertSame(1, $partition->replicas[1]->broker);
        self::assertFalse($partition->replicas[1]->leader);
        self::assertTrue($partition->replicas[1]->inSync);
    }

    public function testProduceToPartition() : void
    {
        $this->expectRequest(
            'POST',
            '/topics/lorem-ipsum/partitions/0',
            '{"records":[{"value":"bXNnMQ=="},{"key":"c29tZSBrZXk=","value":"bXNnMg=="}]}',
            $this->fileResponse('produceToPartition')
        );

        $offsets = $this->client->produceToPartition(
            'lorem-ipsum',
            0,
            [new Message('msg1'), new Message('msg2', 'some key')]
        );

        self::assertCount(2, $offsets);

        self::assertFalse($offsets[0]->isError());
        $offset = $offsets[0]->getOffset();
        self::assertSame(0, $offset->partition);
        self::assertSame(8, $offset->offset);

        self::assertTrue($offsets[1]->isError());
        $error = $offsets[1]->getError();
        self::assertSame(1, $error->errorCode);
        self::assertSame('some unexpected error', $error->error);
    }

    public function testCreateConsumer() : void
    {
        $consumerOptions = new ConsumerOptions();
        $consumerOptions->name = 'custom-consumer-name';

        $this->expectRequest('POST', '/consumers/dolor-sit-amet', '{}', $this->fileResponse('createConsumer'));

        $consumer = $this->client->createConsumer('dolor-sit-amet', new ConsumerOptions());

        self::assertSame('custom-consumer-name', $consumer->instanceId);
        self::assertSame('http://api/consumers/dolor-sit-amet/instances/custom-consumer-name', $consumer->baseUri);
    }

    public function testDeleteConsumer() : void
    {
        $consumer = new Consumer();
        $consumer->baseUri = 'http://api/consumers/dolor-sit-amet/instances/custom-consumer-name';

        $this->expectRequest(
            'DELETE',
            '/consumers/dolor-sit-amet/instances/custom-consumer-name',
            null,
            $this->response->withStatus(Http::NO_CONTENT)
        );

        $this->client->deleteConsumer($consumer);
        self::assertThat(true, new IsTrue());
    }

    public function testConsumerCommitOffsets() : void
    {
        $consumer = new Consumer();
        $consumer->baseUri = 'http://api/consumers/dolor-sit-amet/instances/custom-consumer-name';

        $this->expectRequest(
            'POST',
            '/consumers/dolor-sit-amet/instances/custom-consumer-name/offsets',
            null,
            $this->response->withStatus(Http::NO_CONTENT)
        );

        $this->client->consumerCommitOffsets($consumer);

        $this->expectRequest(
            'POST',
            '/consumers/dolor-sit-amet/instances/custom-consumer-name/offsets',
            '{"offsets":[{"topic":"lorem-ipsum","partition":0,"offset":25}]}',
            $this->response->withStatus(Http::NO_CONTENT)
        );

        $this->client->consumerCommitOffsets($consumer, [new Offset('lorem-ipsum', 0, 25)]);
        self::assertThat(true, new IsTrue());
    }

    public function testGetConsumerCommittedOffsets() : void
    {
        $consumer = new Consumer();
        $consumer->baseUri = 'http://api/consumers/dolor-sit-amet/instances/custom-consumer-name';

        $this->expectRequest(
            'GET',
            '/consumers/dolor-sit-amet/instances/custom-consumer-name/offsets',
            '{"partitions":[{"topic":"lorem-ipsum","partition":0}]}',
            $this->responseWithBody('{"offsets":[{"topic":"lorem-ipsum","partition":0,"offset":25,"metadata":""}]}')
        );

        $offsets = $this->client->getConsumerCommittedOffsets($consumer, [new Partition('lorem-ipsum', 0)]);

        self::assertCount(1, $offsets);
        self::assertSame(0, $offsets[0]->partition);
        self::assertSame(25, $offsets[0]->offset);
        self::assertSame('', $offsets[0]->metadata);
    }

    public function testConsumerSubscribe() : void
    {
        $consumer = new Consumer();
        $consumer->baseUri = 'http://api/consumers/dolor-sit-amet/instances/custom-consumer-name';

        $this->expectRequest(
            'POST',
            '/consumers/dolor-sit-amet/instances/custom-consumer-name/subscription',
            '{"topics":["lorem-ipsum"]}',
            $this->response->withStatus(Http::NO_CONTENT)
        );

        $this->client->consumerSubscribe($consumer, Subscription::topic('lorem-ipsum'));

        $this->expectRequest(
            'POST',
            '/consumers/dolor-sit-amet/instances/custom-consumer-name/subscription',
            '{"topic_pattern":"lorem-*"}',
            $this->response->withStatus(Http::NO_CONTENT)
        );

        $this->client->consumerSubscribe($consumer, Subscription::pattern('lorem-*'));
        self::assertThat(true, new IsTrue());
    }

    public function testGetConsumerSubscribedTopics() : void
    {
        $consumer = new Consumer();
        $consumer->baseUri = 'http://api/consumers/dolor-sit-amet/instances/custom-consumer-name';

        $this->expectRequest(
            'GET',
            '/consumers/dolor-sit-amet/instances/custom-consumer-name/subscription',
            null,
            $this->responseWithBody('{"topics":["lorem-ipsum"]}')
        );

        $topics = $this->client->getConsumerSubscribedTopics($consumer);

        self::assertCount(1, $topics);
        self::assertSame('lorem-ipsum', $topics[0]);
    }

    public function testConsumerUnsubscribe() : void
    {
        $consumer = new Consumer();
        $consumer->baseUri = 'http://api/consumers/dolor-sit-amet/instances/custom-consumer-name';

        $this->expectRequest(
            'DELETE',
            '/consumers/dolor-sit-amet/instances/custom-consumer-name/subscription',
            null,
            $this->response->withStatus(Http::NO_CONTENT)
        );

        $this->client->consumerUnsubscribe($consumer);
        self::assertThat(true, new IsTrue());
    }

    public function testConsumerAssignPartitions() : void
    {
        $consumer = new Consumer();
        $consumer->baseUri = 'http://api/consumers/dolor-sit-amet/instances/custom-consumer-name';

        $this->expectRequest(
            'POST',
            '/consumers/dolor-sit-amet/instances/custom-consumer-name/assignments',
            '{"partitions":[{"topic":"lorem-ipsum","partition":0}]}',
            $this->response->withStatus(Http::NO_CONTENT)
        );

        $this->client->consumerAssignPartitions($consumer, [new Partition('lorem-ipsum', 0)]);
        self::assertThat(true, new IsTrue());
    }

    public function testGetConsumerAssignedPartitions() : void
    {
        $consumer = new Consumer();
        $consumer->baseUri = 'http://api/consumers/dolor-sit-amet/instances/custom-consumer-name';

        $this->expectRequest(
            'GET',
            '/consumers/dolor-sit-amet/instances/custom-consumer-name/assignments',
            null,
            $this->responseWithBody('{"partitions":[{"topic":"lorem-ipsum","partition":0}]}')
        );

        $partitions = $this->client->getConsumerAssignedPartitions($consumer);

        self::assertCount(1, $partitions);
        self::assertSame('lorem-ipsum', $partitions[0]->topic);
        self::assertSame(0, $partitions[0]->partition);
    }

    public function testConsumerSeek() : void
    {
        $consumer = new Consumer();
        $consumer->baseUri = 'http://api/consumers/dolor-sit-amet/instances/custom-consumer-name';

        $this->expectRequest(
            'POST',
            '/consumers/dolor-sit-amet/instances/custom-consumer-name/positions',
            '{"offsets":[{"topic":"lorem-ipsum","partition":0,"offset":12}]}',
            $this->response->withStatus(Http::NO_CONTENT)
        );

        $this->client->consumerSeek($consumer, [new Offset('lorem-ipsum', 0, 12)]);
        self::assertThat(true, new IsTrue());
    }

    public function testConsumerSeekStart() : void
    {
        $consumer = new Consumer();
        $consumer->baseUri = 'http://api/consumers/dolor-sit-amet/instances/custom-consumer-name';

        $this->expectRequest(
            'POST',
            '/consumers/dolor-sit-amet/instances/custom-consumer-name/positions/beginning',
            '{"partitions":[{"topic":"lorem-ipsum","partition":0}]}',
            $this->response->withStatus(Http::NO_CONTENT)
        );

        $this->client->consumerSeekStart($consumer, [new Partition('lorem-ipsum', 0)]);
        self::assertThat(true, new IsTrue());
    }

    public function testConsumerSeekEnd() : void
    {
        $consumer = new Consumer();
        $consumer->baseUri = 'http://api/consumers/dolor-sit-amet/instances/custom-consumer-name';

        $this->expectRequest(
            'POST',
            '/consumers/dolor-sit-amet/instances/custom-consumer-name/positions/end',
            '{"partitions":[{"topic":"lorem-ipsum","partition":0}]}',
            $this->response->withStatus(Http::NO_CONTENT)
        );

        $this->client->consumerSeekEnd($consumer, [new Partition('lorem-ipsum', 0)]);
        self::assertThat(true, new IsTrue());
    }

    public function testGetConsumerMessages() : void
    {
        $consumer = new Consumer();
        $consumer->baseUri = 'http://api/consumers/dolor-sit-amet/instances/custom-consumer-name';

        $this->expectRequest(
            'GET',
            '/consumers/dolor-sit-amet/instances/custom-consumer-name/records',
            null,
            $this->fileResponse('getConsumerMessages')
        );

        $messages = $this->client->getConsumerMessages($consumer);

        self::assertCount(2, $messages);

        self::assertSame('lorem-ipsum', $messages[0]->topic);
        self::assertNull($messages[0]->key);
        self::assertSame('msg1', $messages[0]->content);
        self::assertSame(0, $messages[0]->partition);
        self::assertSame(52, $messages[0]->offset);

        self::assertSame('lorem-ipsum', $messages[1]->topic);
        self::assertSame('some key', $messages[1]->key);
        self::assertSame('msg2', $messages[1]->content);
        self::assertSame(1, $messages[1]->partition);
        self::assertSame(53, $messages[1]->offset);
    }

    public function testGetBrokers() : void
    {
        $this->expectRequest('GET', '/brokers', null, $this->responseWithBody('{"brokers":[1, 0]}'));

        $brokers = $this->client->getBrokers();

        self::assertCount(2, $brokers);
        self::assertSame(1, $brokers[0]);
        self::assertSame(0, $brokers[1]);
    }

    /**
     * @dataProvider providerErrors
     */
    public function testErrors(?string $body, int $status, string $message) : void
    {
        if ($body === null) {
            $response = $this->response->withStatus($status);
        } else {
            $response = $this->responseWithBody($body)->withStatus($status);
        }

        $this->expectRequest('GET', '/topics', null, $response);

        $this->expectException(UnexpectedApiResponse::class);
        $this->expectExceptionMessage($message);

        $this->client->listTopics();
    }

    /**
     * @return iterable<array<mixed>>
     */
    public function providerErrors() : iterable
    {
        yield [
            'body' => '{"error_code":40401,"message":"Topic not found"}',
            'status' => Http::NOT_FOUND,
            'message' => 'Unexpected response HTTP 404: [40401] Topic not found',
        ];

        yield [
            'body' => null,
            'status' => Http::INTERNAL_SERVER_ERROR,
            'message' => 'Unexpected HTTP status 500: Internal Server Error',
        ];
    }

    private function expectRequest(string $method, string $path, ?string $body, ResponseInterface $response) : void
    {
        $this->httpClient->shouldReceive('sendRequest')
            ->once()
            ->with(Mockery::on(static function (RequestInterface $request) use ($method, $path, $body) {
                return $request->getMethod() === $method &&
                    (string) $request->getUri() === 'http://api' . $path &&
                    $request->getHeader('Accept') === ['application/vnd.kafka.v2+json'] &&
                    (string) $request->getBody() === ($body ?? '');
            }))
            ->andReturn($response);
    }

    private function fileResponse(string $name) : ResponseInterface
    {
        $path = sprintf('%s/data/%s.json', __DIR__, $name);

        return $this->responseWithBody(file_get_contents($path));
    }

    private function responseWithBody(string $body) : ResponseInterface
    {
        return $this->response->withBody((new StreamFactory())->createStream($body));
    }
}
