<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Api\Value\Request;

use JMS\Serializer\Annotation as Serializer;

final class Partition
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

    public function __construct(string $topic, int $partition)
    {
        $this->topic = $topic;
        $this->partition = $partition;
    }
}
