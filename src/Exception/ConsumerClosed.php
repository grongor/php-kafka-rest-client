<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Exception;

use RuntimeException;
use Throwable;

final class ConsumerClosed extends RuntimeException
{
    public static function error(Throwable $previous) : self
    {
        return new self('Failed to consume messages; consumer is now closed', 0, $previous);
    }

    public static function new() : self
    {
        return new self('Consumer is closed');
    }
}
