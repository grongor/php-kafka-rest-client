<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Api\Exception;

use RuntimeException;
use function Safe\sprintf;

final class UnexpectedApiResponse extends RuntimeException
{
    public static function fromStatus(int $httpCode, string $message) : self
    {
        return new self(sprintf('Unexpected HTTP status %d: %s', $httpCode, $message));
    }

    /**
     * @param array<string, int|string> $response
     */
    public static function fromResponse(int $httpCode, array $response) : self
    {
        return new self(
            sprintf('Unexpected response HTTP %d: [%d] %s', $httpCode, $response['error_code'], $response['message'])
        );
    }
}
