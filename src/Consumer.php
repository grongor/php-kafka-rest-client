<?php

declare(strict_types=1);

namespace Grongor\KafkaRest;

use Grongor\KafkaRest\Api\RestClient;
use Grongor\KafkaRest\Api\Value\Request\Offset;
use Grongor\KafkaRest\Api\Value\Response\Message;
use Grongor\KafkaRest\Exception\ConsumerClosed;
use Throwable;
use function count;

final class Consumer
{
    /** @var RestClient */
    private $client;

    /** @var Api\Value\Response\Consumer */
    private $consumer;

    /** @var callable|null */
    private $idleCallback;

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

    public function setIdleCallback(callable $idleCallback) : void
    {
        $this->idleCallback = $idleCallback;
    }

    /**
     * @return Message[]
     */
    public function consume(?int $timeout = null, ?int $maxBytes = null) : iterable
    {
        if (! isset($this->consumer)) {
            throw ConsumerClosed::consume();
        }

        try {
            while (true) {
                $messages = $this->client->getConsumerMessages($this->consumer, $timeout, $maxBytes);

                if (! isset($this->consumer)) {
                    break;
                }

                if ($this->idleCallback !== null && count($messages) === 0) {
                    ($this->idleCallback)();

                    continue;
                }

                yield from $messages;
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

    public function close() : void
    {
        if (! isset($this->consumer)) {
            throw ConsumerClosed::close();
        }

        $this->client->deleteConsumer($this->consumer);
        unset($this->consumer);
    }
}
