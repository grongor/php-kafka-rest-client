<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Api\Value\Response;

use JMS\Serializer\Annotation as Serializer;

final class Consumer
{
    /**
     * @Serializer\Type("string")
     * @var string
     */
    public $instanceId;

    /**
     * @Serializer\Type("string")
     * @var string
     */
    public $baseUri;
}
