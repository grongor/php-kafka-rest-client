<?php

declare(strict_types=1);

namespace Grongor\KafkaRest;

use Assert\Assertion;
use DateInterval;
use DateTimeImmutable;
use Grongor\KafkaRest\Api\RestClient;
use Grongor\KafkaRest\Api\Value\Request\Message;
use Grongor\KafkaRest\Api\Value\Request\Offset;
use Grongor\KafkaRest\Exception\ConsumerClosed;
use Grongor\KafkaRest\Value\MessagesBatch;
use Lcobucci\Clock\Clock;
use Lcobucci\Clock\SystemClock;
use Throwable;
use function count;
use function Safe\sprintf;

final class BatchConsumer
{
    /** @var RestClient */
    private $client;

    /** @var Api\Value\Response\Consumer */
    private $consumer;

    /** @var int|null */
    private $maxCount;

    /** @var DateInterval|null */
    private $maxDuration;

    /** @var Clock */
    private $clock;

    public function __construct(
        RestClient $client,
        Api\Value\Response\Consumer $consumer,
        ?int $maxCount,
        ?int $maxDuration = null,
        ?Clock $clock = null
    ) {
        Assertion::true(
            $maxCount !== null || $maxDuration !== null,
            'You must specify at least one of maxCount/maxDuration'
        );

        $this->client = $client;
        $this->consumer = $consumer;
        $this->maxCount = $maxCount;

        if ($maxDuration !== null) {
            $this->maxDuration = new DateInterval(sprintf('PT%dS', $maxDuration));
        }

        $this->clock = $clock ?? new SystemClock();
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
    public function consume(?int $maxBytes = null) : iterable
    {
        if (! isset($this->consumer)) {
            throw ConsumerClosed::new();
        }

        $batch = new MessagesBatch();
        $batchTimeout = $this->getBatchTimeout();

        try {
            while (true) {
                $messages = $this->client->getConsumerMessages(
                    $this->consumer,
                    $this->getTimeout($batchTimeout),
                    $maxBytes
                );
                foreach ($messages as $message) {
                    $batch->add($message);

                    if (($this->maxCount === null || $batch->count() !== $this->maxCount) &&
                        ($this->maxDuration === null || $this->clock->now() < $batchTimeout)
                    ) {
                        continue;
                    }

                    yield $batch;

                    $batch = new MessagesBatch();
                    $batchTimeout = $this->getBatchTimeout();
                }
            }
        } catch (Throwable $throwable) {
            $this->close();

            throw ConsumerClosed::error($throwable);
        }
    }

    public function commit(MessagesBatch $messagesBatch) : void
    {
        if ($messagesBatch->isEmpty()) {
            return;
        }

        $offsets = $offsetsMap = [];
        $messages = $messagesBatch->getMessages();
        for ($i = count($messages) - 1; $i >= 0; $i--) {
            $message = $messages[$i];
            if (isset($offsetsMap[$message->topic][$message->partition])) {
                continue;
            }

            $offsetsMap[$message->topic][$message->partition] = $message->offset;
            $offsets[] = new Offset($message->topic, $message->partition, $message->offset);
        }

        $this->client->consumerCommitOffsets($this->consumer, $offsets);
    }

    private function getBatchTimeout() : ?DateTimeImmutable
    {
        if ($this->maxDuration !== null) {
            return $this->clock->now()->add($this->maxDuration);
        }

        return null;
    }

    private function getTimeout(?DateTimeImmutable $batchTimeout) : int
    {
        if ($batchTimeout === null) {
            return 10;
        }

        return $batchTimeout->getTimestamp() - $this->clock->now()->getTimestamp();
    }

    private function close() : void
    {
        $this->client->deleteConsumer($this->consumer);
        unset($this->consumer);
    }
}
