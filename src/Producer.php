<?php

declare(strict_types=1);

namespace Grongor\KafkaRest;

use Grongor\KafkaRest\Api\RestClient;
use Grongor\KafkaRest\Api\Value\Request\Message;
use Grongor\KafkaRest\Exception\FailedToProduceMessages;
use function array_key_exists;
use function count;
use function iterable_to_array;

final class Producer
{
    /** @var RestClient */
    private $client;

    public function __construct(RestClient $client)
    {
        $this->client = $client;
    }

    public function produce(string $topic, Message $message) : void
    {
        $this->produceBatch($topic, [$message]);
    }

    /**
     * @param Message[] $messages
     */
    public function produceBatch(string $topic, iterable $messages) : void
    {
        $messages = iterable_to_array($messages, false);
        if (count($messages) === 0) {
            return;
        }

        $errors = [];
        foreach ($this->client->produce($topic, $messages) as $i => $result) {
            if (! $result->isError()) {
                continue;
            }

            $errors[$i] = $result->getError();
        }

        if (count($errors) === 0) {
            return;
        }

        $retryable = $nonRetryable = [];
        foreach ($messages as $i => $message) {
            if (! array_key_exists($i, $errors)) {
                continue;
            }

            if ($errors[$i]->errorCode === 2) {
                $retryable[] = ['error' => $errors[$i]->error, 'message' => $message];
            } else {
                $nonRetryable[] = ['error' => $errors[$i]->error, 'message' => $message];
            }
        }

        throw FailedToProduceMessages::new($retryable, $nonRetryable);
    }
}
