<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Api\Value\Response;

use JMS\Serializer\Annotation as Serializer;

final class Topic
{
    /**
     * @Serializer\Type("string")
     * @var string
     */
    public $name;

    /**
     * @Serializer\Type("array<string, string>")
     * @var array<string, string>
     */
    public $configs;

    /**
     * @Serializer\Type("array<Grongor\KafkaRest\Api\Value\Response\Partition>")
     * @var Partition[]
     */
    public $partitions;
}
