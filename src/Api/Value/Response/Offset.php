<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Api\Value\Response;

use JMS\Serializer\Annotation as Serializer;

final class Offset
{
    /**
     * @Serializer\Type("int")
     * @var int
     */
    public $partition;

    /**
     * @Serializer\Type("int")
     * @var int
     */
    public $offset;

    /**
     * @Serializer\Type("string")
     * @var string|null
     */
    public $metadata;

    /**
     * @internal
     */
    public function __construct(int $partition, int $offset)
    {
        $this->partition = $partition;
        $this->offset = $offset;
    }
}
