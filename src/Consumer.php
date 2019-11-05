<?php

declare(strict_types=1);

namespace Grongor\KafkaRest;

use Grongor\KafkaRest\Api\RestClient;
use Grongor\KafkaRest\Api\Value\Request\Offset;
use Grongor\KafkaRest\Api\Value\Response\Message;
use Grongor\KafkaRest\Exception\ConsumerClosed;
use Throwable;

final class Consumer
{
    /** @var RestClient */
    private $client;

    /** @var Api\Value\Response\Consumer */
    private $consumer;

    public function __construct(RestClient $client, Api\Value\Response\Consumer $consumer)
    {
        $this->client = $client;
        $this->consumer = $consumer;
    }

    public function __destruct()
    {
        if (! isset($this->consumer)) {
            return;
        }

        $this->close();
    }

    /**
     * @return Message[]
     */
    public function consume(?int $timeout = null, ?int $maxBytes = null) : iterable
    {
        if (! isset($this->consumer)) {
            throw ConsumerClosed::new();
        }

        try {
            while (true) {
                yield from $this->client->getConsumerMessages($this->consumer, $timeout, $maxBytes);
            }
        } catch (Throwable $throwable) {
            $this->close();

            throw ConsumerClosed::error($throwable);
        }
    }

    public function commit(Message $message) : void
    {
        $this->client->consumerCommitOffsets(
            $this->consumer,
            [new Offset($message->topic, $message->partition, $message->offset)]
        );
    }

    private function close() : void
    {
        $this->client->deleteConsumer($this->consumer);
        unset($this->consumer);
    }
}
