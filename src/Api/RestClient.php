<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Api;

use Grongor\KafkaRest\Api\Value\Request\ConsumerOptions;
use Grongor\KafkaRest\Api\Value\Request\Subscription;
use Grongor\KafkaRest\Api\Value\Response\AssignedPartition;
use Grongor\KafkaRest\Api\Value\Response\Consumer;
use Grongor\KafkaRest\Api\Value\Response\Message;
use Grongor\KafkaRest\Api\Value\Response\Offset;
use Grongor\KafkaRest\Api\Value\Response\Partition;
use Grongor\KafkaRest\Api\Value\Response\ProduceResult;
use Grongor\KafkaRest\Api\Value\Response\Topic;

interface RestClient
{
    /**
     * GET /topics
     *
     * @return array<string>
     */
    public function listTopics() : array;

    /**
     * GET /topics/(string:topic_name)
     */
    public function getTopic(string $topic) : Topic;

    /**
     * POST /topics/(string:topic_name)
     *
     * @param Value\Request\Message[] $messages
     *
     * @return ProduceResult[]
     */
    public function produce(string $topic, iterable $messages) : array;

    /**
     * GET /topics/(string:topic_name)/partitions
     *
     * @return Partition[]
     */
    public function listPartitions(string $topic) : array;

    /**
     * GET /topics/(string:topic_name)/partitions/(int:partition_id)
     */
    public function getPartition(string $topic, int $partition) : Partition;

    /**
     * POST /topics/(string:topic_name)/partitions/(int:partition_id)
     *
     * @param Value\Request\Message[] $messages
     *
     * @return ProduceResult[]
     */
    public function produceToPartition(string $topic, int $partition, iterable $messages) : array;

    /**
     * POST /consumers/(string:group_name)
     */
    public function createConsumer(string $group, ?ConsumerOptions $consumerOptions = null) : Consumer;

    /**
     * DELETE /consumers/(string:group_name)/instances/(string:instance)
     */
    public function deleteConsumer(Consumer $consumer) : void;

    /**
     * POST /consumers/(string:group_name)/instances/(string:instance)/offsets
     *
     * Be aware that REST Proxy increments all the offsets for some reason
     *
     * @see https://github.com/confluentinc/kafka-rest/blob/3473f3f/kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerState.java#L123
     * @see https://kafka.apache.org/22/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html#commitSync
     *
     * @param Value\Request\Offset[]|null $offsets
     */
    public function consumerCommitOffsets(Consumer $consumer, ?iterable $offsets = null) : void;

    /**
     * GET /consumers/(string:group_name)/instances/(string:instance)/offsets
     *
     * @param Value\Request\Partition[] $partitions
     *
     * @return Offset[]
     */
    public function getConsumerCommittedOffsets(Consumer $consumer, iterable $partitions) : array;

    /**
     * POST /consumers/(string:group_name)/instances/(string:instance)/subscription
     */
    public function consumerSubscribe(Consumer $consumer, Subscription $subscription) : void;

    /**
     * GET /consumers/(string:group_name)/instances/(string:instance)/subscription
     *
     * @return array<string>
     */
    public function getConsumerSubscribedTopics(Consumer $consumer) : array;

    /**
     * DELETE /consumers/(string:group_name)/instances/(string:instance)/subscription
     */
    public function consumerUnsubscribe(Consumer $consumer) : void;

    /**
     * POST /consumers/(string:group_name)/instances/(string:instance)/assignments
     *
     * @param Value\Request\Partition[] $partitions
     */
    public function consumerAssignPartitions(Consumer $consumer, iterable $partitions) : void;

    /**
     * GET /consumers/(string:group_name)/instances/(string:instance)/assignments
     *
     * @return AssignedPartition[]
     */
    public function getConsumerAssignedPartitions(Consumer $consumer) : array;

    /**
     * POST /consumers/(string:group_name)/instances/(string:instance)/positions
     *
     * @param Value\Request\Offset[] $offsets
     */
    public function consumerSeek(Consumer $consumer, iterable $offsets) : void;

    /**
     * POST /consumers/(string:group_name)/instances/(string:instance)/positions/beginning
     *
     * @param Value\Request\Partition[] $partitions
     */
    public function consumerSeekStart(Consumer $consumer, iterable $partitions) : void;

    /**
     * POST /consumers/(string:group_name)/instances/(string:instance)/positions/end
     *
     * @param Value\Request\Partition[] $partitions
     */
    public function consumerSeekEnd(Consumer $consumer, iterable $partitions) : void;

    /**
     * GET /consumers/(string:group_name)/instances/(string:instance)/records
     *
     * @return Message[]
     */
    public function getConsumerMessages(Consumer $consumer, ?int $timeout = null, ?int $maxBytes = null) : array;

    /**
     * GET /brokers
     *
     * @return array<int>
     */
    public function getBrokers() : array;
}
