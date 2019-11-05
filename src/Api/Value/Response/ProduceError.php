<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Api\Value\Response;

final class ProduceError
{
    /** @var int */
    public $errorCode;

    /** @var string */
    public $error;

    /**
     * @internal
     */
    public function __construct(int $errorCode, string $error)
    {
        $this->errorCode = $errorCode;
        $this->error = $error;
    }
}
