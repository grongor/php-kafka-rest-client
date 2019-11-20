<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Api;

use Assert\Assertion;
use Grongor\KafkaRest\Api\Exception\UnexpectedApiResponse;
use Grongor\KafkaRest\Api\Value\Request\ConsumerOptions;
use Grongor\KafkaRest\Api\Value\Request\Subscription;
use Grongor\KafkaRest\Api\Value\Response\AssignedPartition;
use Grongor\KafkaRest\Api\Value\Response\Consumer;
use Grongor\KafkaRest\Api\Value\Response\Message;
use Grongor\KafkaRest\Api\Value\Response\Offset;
use Grongor\KafkaRest\Api\Value\Response\Partition;
use Grongor\KafkaRest\Api\Value\Response\ProduceResults;
use Grongor\KafkaRest\Api\Value\Response\Topic;
use JMS\Serializer\SerializerInterface;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestFactoryInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\StreamFactoryInterface;
use Psr\Http\Message\UriFactoryInterface;
use Psr\Http\Message\UriInterface;
use Psr\Log\LoggerInterface;
use Teapot\StatusCode\Http;
use function explode;
use function http_build_query;
use function Safe\json_decode;
use function Safe\sprintf;

final class HttpRestClient implements RestClient
{
    /** @var ClientInterface */
    private $client;

    /** @var LoggerInterface */
    private $logger;

    /** @var RequestFactoryInterface */
    private $requestFactory;

    /** @var SerializerInterface */
    private $serializer;

    /** @var StreamFactoryInterface */
    private $streamFactory;

    /** @var UriFactoryInterface */
    private $uriFactory;

    /** @var UriInterface */
    private $apiUri;

    /** {@inheritDoc} */
    public function __construct(
        ClientInterface $client,
        LoggerInterface $logger,
        RequestFactoryInterface $requestFactory,
        SerializerInterface $serializer,
        StreamFactoryInterface $streamFactory,
        UriFactoryInterface $uriFactory,
        string $apiUri
    ) {
        $this->client = $client;
        $this->logger = $logger;
        $this->requestFactory = $requestFactory;
        $this->serializer = $serializer;
        $this->streamFactory = $streamFactory;
        $this->uriFactory = $uriFactory;

        $this->apiUri = $this->uriFactory->createUri($apiUri);
    }

    /** {@inheritDoc} */
    public function listTopics() : array
    {
        return $this->execute($this->get('/topics'), 'array<string>');
    }

    /** {@inheritDoc} */
    public function getTopic(string $topic) : Topic
    {
        return $this->execute($this->get(sprintf('/topics/%s', $topic)), Topic::class);
    }

    /** {@inheritDoc} */
    public function produce(string $topic, iterable $messages) : array
    {
        return $this->execute(
            $this->post(sprintf('/topics/%s', $topic), $this->serialize(['records' => $messages])),
            ProduceResults::class
        )->results;
    }

    /** {@inheritDoc} */
    public function listPartitions(string $topic) : array
    {
        return $this->execute(
            $this->get(sprintf('/topics/%s/partitions', $topic)),
            sprintf('array<%s>', Partition::class)
        );
    }

    /** {@inheritDoc} */
    public function getPartition(string $topic, int $partition) : Partition
    {
        return $this->execute(
            $this->get(sprintf('/topics/%s/partitions/%d', $topic, $partition)),
            Partition::class
        );
    }

    /** {@inheritDoc} */
    public function produceToPartition(string $topic, int $partition, iterable $messages) : array
    {
        return $this->execute(
            $this->post(
                sprintf('/topics/%s/partitions/%d', $topic, $partition),
                $this->serialize(['records' => $messages])
            ),
            ProduceResults::class
        )->results;
    }

    /** {@inheritDoc} */
    public function createConsumer(string $group, ?ConsumerOptions $consumerOptions = null) : Consumer
    {
        $consumerOptions = $consumerOptions ?? new ConsumerOptions();

        $consumer = $this->execute(
            $this->post(sprintf('/consumers/%s', $group), $this->serialize($consumerOptions)),
            Consumer::class
        );

        if ($this->apiUri->getUserInfo() !== '') {
            $userAndPassword = explode(':', $this->apiUri->getUserInfo(), 2);
            $consumerUri = $this->uriFactory->createUri($consumer->baseUri);
            $consumer->baseUri = (string) $consumerUri->withUserInfo($userAndPassword[0], $userAndPassword[1] ?? null);
        }

        return $consumer;
    }

    /** {@inheritDoc} */
    public function deleteConsumer(Consumer $consumer) : void
    {
        $this->execute($this->consumerDelete($consumer, ''));
    }

    /** {@inheritDoc} */
    public function consumerCommitOffsets(Consumer $consumer, ?iterable $offsets = null) : void
    {
        $body = $offsets === null ? '' : $this->serialize(['offsets' => $offsets]);

        // For some reason the API always returns empty array
        // @see https://github.com/confluentinc/kafka-rest/blob/3473f3f/kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerState.java#L136
        $this->execute($this->consumerPost($consumer, '/offsets', $body), 'array');
    }

    /** {@inheritDoc} */
    public function getConsumerCommittedOffsets(Consumer $consumer, iterable $partitions) : array
    {
        return $this->execute(
            $this->consumerGet($consumer, '/offsets', $this->serialize(['partitions' => $partitions])),
            sprintf('array<string, array<%s>>', Offset::class)
        )['offsets'];
    }

    /** {@inheritDoc} */
    public function consumerSubscribe(Consumer $consumer, Subscription $subscription) : void
    {
        $this->execute($this->consumerPost($consumer, '/subscription', $this->serialize($subscription)));
    }

    /** {@inheritDoc} */
    public function getConsumerSubscribedTopics(Consumer $consumer) : array
    {
        return $this->execute($this->consumerGet($consumer, '/subscription'), 'array<string, array<string>>')['topics'];
    }

    /** {@inheritDoc} */
    public function consumerUnsubscribe(Consumer $consumer) : void
    {
        $this->execute($this->consumerDelete($consumer, '/subscription'));
    }

    /** {@inheritDoc} */
    public function consumerAssignPartitions(Consumer $consumer, iterable $partitions) : void
    {
        $this->execute($this->consumerPost($consumer, '/assignments', $this->serialize(['partitions' => $partitions])));
    }

    /** {@inheritDoc} */
    public function getConsumerAssignedPartitions(Consumer $consumer) : array
    {
        return $this->execute(
            $this->consumerGet($consumer, '/assignments'),
            sprintf('array<string, array<%s>>', AssignedPartition::class)
        )['partitions'];
    }

    /** {@inheritDoc} */
    public function consumerSeek(Consumer $consumer, iterable $offsets) : void
    {
        $this->execute($this->consumerPost($consumer, '/positions', $this->serialize(['offsets' => $offsets])));
    }

    /** {@inheritDoc} */
    public function consumerSeekStart(Consumer $consumer, iterable $partitions) : void
    {
        $this->execute(
            $this->consumerPost($consumer, '/positions/beginning', $this->serialize(['partitions' => $partitions]))
        );
    }

    /** {@inheritDoc} */
    public function consumerSeekEnd(Consumer $consumer, iterable $partitions) : void
    {
        $this->execute(
            $this->consumerPost($consumer, '/positions/end', $this->serialize(['partitions' => $partitions]))
        );
    }

    /** {@inheritDoc} */
    public function getConsumerMessages(Consumer $consumer, ?int $timeout = null, ?int $maxBytes = null) : array
    {
        $parameters = [];
        if ($timeout !== null) {
            $parameters['timeout'] = $timeout;
        }

        if ($maxBytes !== null) {
            $parameters['max_bytes'] = $maxBytes;
        }

        $request = $this->consumerGet($consumer, '/records');
        $request = $request->withUri($request->getUri()->withQuery(http_build_query($parameters)), true);

        return $this->execute($request, sprintf('array<%s>', Message::class));
    }

    /** {@inheritDoc} */
    public function getBrokers() : array
    {
        return $this->execute($this->get('/brokers'), 'array<string, array<int>>')['brokers'];
    }

    private function request(string $method, string $path, ?UriInterface $baseUri = null) : RequestInterface
    {
        $request = $this->requestFactory->createRequest($method, ($baseUri ?? $this->apiUri)->withPath($path));

        return $request->withHeader('Accept', 'application/vnd.kafka.v2+json');
    }

    private function get(string $path, ?UriInterface $baseUri = null) : RequestInterface
    {
        return $this->request('GET', $path, $baseUri);
    }

    private function consumerGet(Consumer $consumer, string $path, ?string $body = null) : RequestInterface
    {
        $consumerUri = $this->uriFactory->createUri($consumer->baseUri);
        $request = $this->get($consumerUri->getPath() . $path, $consumerUri);

        return $body === null ? $request : $this->requestWithBody($request, $body);
    }

    private function post(string $path, string $body, ?UriInterface $baseUri = null) : RequestInterface
    {
        return $this->requestWithBody($this->request('POST', $path, $baseUri), $body);
    }

    private function consumerPost(Consumer $consumer, string $path, string $body) : RequestInterface
    {
        $consumerUri = $this->uriFactory->createUri($consumer->baseUri);

        return $this->post($consumerUri->getPath() . $path, $body, $consumerUri);
    }

    private function consumerDelete(Consumer $consumer, string $path) : RequestInterface
    {
        $consumerUri = $this->uriFactory->createUri($consumer->baseUri);

        return $this->request('DELETE', $consumerUri->getPath() . $path, $consumerUri);
    }

    private function requestWithBody(RequestInterface $request, string $body) : RequestInterface
    {
        $request = $request->withHeader('Content-Type', 'application/vnd.kafka.binary.v2+json');
        $request = $request->withBody($this->streamFactory->createStream($body));

        return $request;
    }

    /**
     * @return mixed
     */
    private function execute(RequestInterface $request, ?string $resultType = null)
    {
        $this->logger->debug(
            'Sending request',
            [
                'method' => $request->getMethod(),
                'uri' => (string) $request->getUri(),
                'body' => (string) $request->getBody(),
            ]
        );

        $response = $this->client->sendRequest($request);

        $this->logger->debug(
            'Received response',
            [
                'statusCode' => $response->getStatusCode(),
                'statusReason' => $response->getReasonPhrase(),
                'body' => (string) $response->getBody(),
            ]
        );

        if ($response->getStatusCode() === Http::OK) {
            Assertion::notNull($resultType);

            return $this->serializer->deserialize((string) $response->getBody(), $resultType, 'json');
        }

        if ($response->getStatusCode() === Http::NO_CONTENT) {
            return null;
        }

        $body = (string) $response->getBody();
        if ($body === '') {
            throw UnexpectedApiResponse::fromStatus($response->getStatusCode(), $response->getReasonPhrase());
        }

        throw UnexpectedApiResponse::fromResponse($response->getStatusCode(), json_decode($body, true));
    }

    /**
     * @param mixed $data
     */
    private function serialize($data) : string
    {
        return $this->serializer->serialize($data, 'json');
    }
}
