<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Exception;

use RuntimeException;

final class FailedToProduceMessages extends RuntimeException
{
    /** @var mixed[] */
    public $retryable;

    /** @var mixed[] */
    public $nonRetryable;

    /**
     * @param mixed[] $retryable
     * @param mixed[] $nonRetryable
     */
    public static function new(array $retryable, array $nonRetryable) : self
    {
        $exception = new self('Failed to produce some messages');
        $exception->retryable = $retryable;
        $exception->nonRetryable = $nonRetryable;

        return $exception;
    }
}
