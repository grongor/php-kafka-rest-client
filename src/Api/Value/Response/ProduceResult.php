<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Api\Value\Response;

use Assert\Assertion;
use JMS\Serializer\Annotation as Serializer;

final class ProduceResult
{
    /**
     * @Serializer\Type("int")
     * @var int|null
     */
    public $partition;

    /**
     * @Serializer\Type("int")
     * @var int|null
     */
    public $offset;

    /**
     * @Serializer\Type("int")
     * @var int|null
     */
    public $errorCode;

    /**
     * @Serializer\Type("string")
     * @var string|null
     */
    public $error;

    private function __construct()
    {
    }

    /**
     * @internal
     */
    public static function offset(int $partition, int $offset) : self
    {
        $result = new self();
        $result->partition = $partition;
        $result->offset = $offset;

        return $result;
    }

    /**
     * @internal
     */
    public static function error(int $errorCode, string $error) : self
    {
        $result = new self();
        $result->errorCode = $errorCode;
        $result->error = $error;

        return $result;
    }

    public function isError() : bool
    {
        return $this->errorCode !== null;
    }

    public function getOffset() : Offset
    {
        Assertion::false($this->isError());

        return new Offset((int) $this->partition, (int) $this->offset);
    }

    public function getError() : ProduceError
    {
        Assertion::true($this->isError());

        return new ProduceError((int) $this->errorCode, (string) $this->error);
    }
}
