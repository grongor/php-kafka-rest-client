<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Api\Value\Response;

use JMS\Serializer\Annotation as Serializer;

final class Replica
{
    /**
     * @Serializer\Type("int")
     * @var int
     */
    public $broker;

    /**
     * @Serializer\Type("bool")
     * @var bool
     */
    public $leader;

    /**
     * @Serializer\Type("bool")
     * @var bool
     */
    public $inSync;
}
