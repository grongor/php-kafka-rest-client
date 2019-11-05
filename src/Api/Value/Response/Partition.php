<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Api\Value\Response;

use JMS\Serializer\Annotation as Serializer;

final class Partition
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
    public $leader;

    /**
     * @Serializer\Type("array<Grongor\KafkaRest\Api\Value\Response\Replica>")
     * @var Replica[]
     */
    public $replicas;
}
