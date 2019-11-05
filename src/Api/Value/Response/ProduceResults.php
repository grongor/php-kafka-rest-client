<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Api\Value\Response;

use JMS\Serializer\Annotation as Serializer;

/**
 * @internal
 */
final class ProduceResults
{
    /**
     * @Serializer\Type("array<Grongor\KafkaRest\Api\Value\Response\ProduceResult>")
     * @Serializer\SerializedName("offsets")
     * @var ProduceResult[]
     */
    public $results;
}
