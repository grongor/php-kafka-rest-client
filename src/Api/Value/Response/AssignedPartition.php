<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Api\Value\Response;

use JMS\Serializer\Annotation as Serializer;

final class AssignedPartition
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
}
