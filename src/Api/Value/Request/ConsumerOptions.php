<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Api\Value\Request;

use JMS\Serializer\Annotation as Serializer;

final class ConsumerOptions
{
    public const AUTO_OFFSET_RESET_EARLIEST = 'earliest';
    public const AUTO_OFFSET_RESET_LATEST = 'latest';
    public const AUTO_OFFSET_RESET_NONE = 'none';

    /**
     * @Serializer\Type("string")
     * @Serializer\SkipWhenEmpty()
     * @var string|null
     */
    public $name;

    /**
     * @Serializer\Type("string")
     * @Serializer\SerializedName("auto.offset.reset")
     * @Serializer\SkipWhenEmpty()
     * @var string|null
     */
    public $autoOffsetReset;

    /**
     * @Serializer\Type("string")
     * @Serializer\SerializedName("auto.commit.enable")
     * @Serializer\SkipWhenEmpty()
     * @var string|null
     */
    public $autoCommitEnable;

    /**
     * @Serializer\Type("string")
     * @Serializer\SerializedName("fetch.min.bytes")
     * @Serializer\SkipWhenEmpty()
     * @var string|null
     */
    public $fetchMinBytes;

    /**
     * @Serializer\Type("string")
     * @Serializer\SerializedName("consumer.request.timeout.ms")
     * @Serializer\SkipWhenEmpty()
     * @var string|null
     */
    public $consumerRequestTimeoutMs;
}
