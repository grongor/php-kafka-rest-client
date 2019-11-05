<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Api\Value\Request;

use JMS\Serializer\Annotation as Serializer;

final class Offset
{
    /**
     * @Serializer\Type("string")
     * @var string
     */
    public $topic;

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

    public function __construct(string $topic, int $partition, int $offset)
    {
        $this->topic = $topic;
        $this->partition = $partition;
        $this->offset = $offset;
    }
}
